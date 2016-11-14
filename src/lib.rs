/*
 * TODO:
 *
 * - Clean up code
 * - Document API
 * - Document concurrent algorithm
 * - Add `Sender::poll_cancel`
 *
 */

extern crate futures;

mod mpsc_queue;

use mpsc_queue::{Queue, PopResult};

use futures::{Async, AsyncSink, Poll, StartSend};
use futures::task::{self, Task};
use futures::sink::{Sink};
use futures::stream::Stream;

use std::{thread, usize};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

pub use std::sync::mpsc::SendError;

pub struct Sender<T> {
    // Channel state shared between the sender and receiver.
    inner: Arc<Inner<T>>,

    // Handle to the task that is blocked on this sender. This handle is sent
    // to the receiver half in order to be notified when the sender becomes
    // unblocked.
    sender_task: SenderTask,

    // True if the sender might be blocked. This is an optimization to avoid
    // having to lock the mutex most of the time.
    //
    // This Cell also prevents the type from being `Sync`
    maybe_parked: Cell<bool>,
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,

    received_last_msg: bool,

    // Prevent `Receiver` from being `Sync`
    no_sync: PhantomData<Cell<()>>,
}

struct Inner<T> {
    // Max buffer size of the channel
    buffer: Option<usize>,

    // Internal channel state
    state: AtomicUsize,

    // Message queue
    message_queue: Queue<Option<T>>,

    // Wait queue
    wait_queue: Queue<SenderTask>,

    // Number of senders
    num_senders: AtomicUsize,

    // Handle to the receiver's task.
    recv_task: Mutex<Option<Task>>,
}

#[derive(Debug, Clone, Copy)]
struct State {
    // The `Receive` half of the channel is parked
    recv_parked: bool,

    // `true` when the channel is open
    is_open: bool,

    // Number of messages in the channel
    num_messages: usize,
}

const RECV_PARKED_MASK: usize = 1 << 31;

const OPEN_MASK: usize = 1 << 30;

const INIT_STATE: usize = OPEN_MASK;

// The absolute maximum buffer size of the channel. This is due to the fact
// that the `messages` usize value must also track the wait flag.
const MAX_BUFFER: usize = !(RECV_PARKED_MASK | OPEN_MASK);

// Sent to the consumer to wake up blocked producers
type SenderTask = Arc<Mutex<Option<Task>>>;

/// Create a new channel pair
pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    // Check that the requested buffer size does not exceed the maximum buffer
    // size permitted by the system.
    assert!(buffer < MAX_BUFFER, "requested buffer size too large");
    channel2(Some(buffer))
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    // usize::MAX is a special case where producers will never be blocked
    channel2(None)
}

fn channel2<T>(buffer: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        buffer: buffer,
        state: AtomicUsize::new(INIT_STATE),
        message_queue: Queue::new(),
        wait_queue: Queue::new(),
        num_senders: AtomicUsize::new(1),
        recv_task: Mutex::new(None),
    });

    let tx = Sender {
        inner: inner.clone(),
        sender_task: Arc::new(Mutex::new(None)),
        maybe_parked: Cell::new(false),
    };

    let rx = Receiver {
        inner: inner,
        received_last_msg: false,
        no_sync: PhantomData,
    };

    (tx, rx)
}

/*
 *
 * ===== impl Sender =====
 *
 */

impl<T> Sender<T> {
    /// Try to return a new `Sender` handle.
    ///
    /// This function will succeed if doing so will not cause the total number
    /// of outstanding senders to exceed the maximum that can be handled by the
    /// system.
    pub fn try_clone(&self) -> Option<Sender<T>> {
        // Since this atomic op isn't actually guarding any memory and we don't
        // care about any orderings besides the ordering on the single atomic
        // variable, a relaxed ordering is acceptable.
        let mut curr = self.inner.num_senders.load(Ordering::Relaxed);

        loop {
            if curr == self.inner.max_senders() {
                return None;
            }

            debug_assert!(curr < self.inner.max_senders());

            let next = curr + 1;
            let actual = self.inner.num_senders.compare_and_swap(curr, next, Ordering::Relaxed);

            // The ABA problem doesn't matter here. We only care that the
            // number of senders never exceeds the maximum.
            if actual == curr {
                return Some(Sender {
                    inner: self.inner.clone(),
                    sender_task: Arc::new(Mutex::new(None)),
                    maybe_parked: Cell::new(false),
                });
            }

            curr = actual;
        }
    }

    pub fn poll_ready(&self) -> Async<()> {
        // First check the `maybe_parked` variable. This avoids acquiring the
        // lock in most cases
        if self.maybe_parked.get() {
            // Get a lock on the task handle
            let task = self.sender_task.lock().unwrap();

            if task.is_some() {
                Async::NotReady
            } else {
                self.maybe_parked.set(false);
                Async::Ready(())
            }
        } else {
            Async::Ready(())
        }
    }

    // Does the actual sending work
    fn start_send2(&self, msg: T) -> StartSend<T, SendError<T>> {
        // If the sender is currently blocked, reject the message
        if !self.poll_ready().is_ready() {
            return Ok(AsyncSink::NotReady(msg));
        }

        try!(self.do_send(Some(msg), true));

        Ok(AsyncSink::Ready)
    }

    // Do the send without failing
    fn do_send(&self, msg: Option<T>, can_park: bool) -> Result<(), SendError<T>> {
        let (park_self, unpark_recv) = match self.inc_num_messages() {
            Some((park_self, unpark_recv)) => (park_self, unpark_recv),
            None => {
                // The receiver has closed the channel
                if let Some(msg) = msg {
                    return Err(SendError(msg));
                } else {
                    return Ok(());
                }
            }
        };

        if park_self {
            self.park(can_park);
        }

        // Push the message
        self.inner.message_queue.push(msg);

        if unpark_recv {
            // Do this step first so that the lock is dropped when
            // `unpark` is called
            let task = self.inner.recv_task.lock().unwrap().take();

            if let Some(task) = task {
                task.unpark();
            }
        }

        Ok(())
    }

    // Increment the number of queued messages. Returns if the sender should
    // block.
    fn inc_num_messages(&self) -> Option<(bool, bool)> {
        let mut curr = self.inner.state.load(Ordering::SeqCst);

        loop {
            let mut state = decode_state(curr);

            // The receiver end closed the channel.
            if !state.is_open {
                return None;
            }

            assert!(state.num_messages < MAX_BUFFER, "buffer space exhausted; sending this messages would overflow the state");

            state.num_messages += 1;

            let next = encode_state(&state);
            let actual = self.inner.state.compare_and_swap(curr, next, Ordering::SeqCst);

            if curr == actual {
                // Block if the current length is greater than the buffer
                let park_self = match self.inner.buffer {
                    Some(buffer) => state.num_messages > buffer,
                    None => false,
                };

                // Only unpark the receive half if transitioning from 0 -> 1.
                let unpark_recv = state.num_messages == 1;

                return Some((park_self, unpark_recv));
            }

            curr = actual;
        }
    }

    fn park(&self, can_park: bool) {
        // TODO: clean up internal state if the task::park will fail

        let task = if can_park {
            Some(task::park())
        } else {
            None
        };

        self.maybe_parked.set(true);
        *self.sender_task.lock().unwrap() = task;

        // Send handle over queue
        let t = self.sender_task.clone();
        self.inner.wait_queue.push(t);
    }
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.start_send2(msg)
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<'a, T> Sink for &'a Sender<T> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.start_send2(msg)
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        self.try_clone().expect("failed to clone sender; maximum number of senders already reached")
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Ordering between variables don't matter here
        let prev = self.inner.num_senders.fetch_sub(1, Ordering::Relaxed);

        if prev == 1 {
            let _ = self.do_send(None, false);
        }
    }
}

/*
 *
 * ===== impl Receiver =====
 *
 */

impl<T> Receiver<T> {
    /// Closes the receiving half.
    ///
    /// This prevents any further messages from being sent on the channel while
    /// still enabling the receiver to drain messages that are buffered.
    pub fn close(&self) {
        // A relaxed memory ordering is acceptable given that toggling the
        // flag is an isolated operation. If no further functions are
        // called on `Receiver` then the outcome of this function doesn't
        // really matter. If `poll` is called after this, then the the same
        // cell will be operated on again with stronger ordering.
        let mut curr = self.inner.state.load(Ordering::Relaxed);

        loop {
            let mut state = decode_state(curr);

            if !state.is_open {
                return;
            }

            state.is_open = false;

            let next = encode_state(&state);
            let actual = self.inner.state.compare_and_swap(curr, next, Ordering::Relaxed);

            if actual == curr {
                return;
            }

            curr = actual;
        }
    }

    fn next_message(&self) -> Async<Option<T>> {
        // Pop off a message
        loop {
            match unsafe { self.inner.message_queue.pop() } {
                PopResult::Data(msg) => {
                    return Async::Ready(msg);
                }
                PopResult::Empty => {
                    return Async::NotReady;
                }
                PopResult::Inconsistent => {
                    // Inconsistent means that there will be a message to pop
                    // in a short time. This branch can only be reached if
                    // values are being produced from another thread, so there
                    // are a few ways that we can deal with this:
                    //
                    // 1) Spin
                    // 2) thread::yield_now()
                    // 3) task::park().unwrap() & return NotReady
                    thread::yield_now();
                }
            }
        }
    }

    fn unpark_one(&self) {
        loop {
            match unsafe { self.inner.wait_queue.pop() } {
                PopResult::Data(task) => {
                    // Do this step first so that the lock is dropped when
                    // `unpark` is called
                    let task = task.lock().unwrap().take();

                    if let Some(task) = task {
                        task.unpark();
                    }

                    return;
                }
                PopResult::Empty => {
                    return;
                }
                PopResult::Inconsistent => {
                    // Same as above
                    thread::yield_now();
                }
            }
        }
    }

    fn try_park(&self) -> bool {
        let curr = self.inner.state.load(Ordering::SeqCst);
        let state = decode_state(curr);

        if state.num_messages > 0 {
            return false;
        }

        // First, track the task
        let mut task = self.inner.recv_task.lock().unwrap();
        *task = Some(task::park());

        // Ensure that there are still no messages
        let curr = self.inner.state.load(Ordering::SeqCst);
        let state = decode_state(curr);

        state.num_messages == 0
    }

    fn dec_num_messages(&self) {
        // No memory is being acquired as part of this step. Release is used to
        // ensure that the queue reads happen before decrementing the counter.
        let mut curr = self.inner.state.load(Ordering::SeqCst);

        loop {
            let mut state = decode_state(curr);

            state.num_messages -= 1;

            let next = encode_state(&state);
            let actual = self.inner.state.compare_and_swap(curr, next, Ordering::SeqCst);

            if actual == curr {
                return;
            }

            curr = actual;
        }
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        // If `None` has previously been seen, then always return `None`
        if self.received_last_msg {
            return Ok(Async::Ready(None));
        }

        loop {
            let msg = match self.next_message() {
                Async::Ready(msg) => msg,
                Async::NotReady => {
                    if self.try_park() {
                        return Ok(Async::NotReady);
                    }

                    // A message should arrive shortly
                    thread::yield_now();
                    continue;
                }
            };

            // Unpark a send waiter
            self.unpark_one();

            // Decrement number of messages
            self.dec_num_messages();

            self.received_last_msg = msg.is_none();

            return Ok(Async::Ready(msg));
        }
    }
}

/*
 *
 * ===== impl Inner =====
 *
 */

impl<T> Inner<T> {
    // The return value is such that the total number of messages that can be
    // enqueued into the channel will never exceed MAX_BUFFER
    //
    // if `buffer == usize::MAX` is a special case
    fn max_senders(&self) -> usize {
        match self.buffer {
            Some(buffer) => MAX_BUFFER - buffer,
            None => usize::MAX,
        }
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/*
 *
 * ===== Helpers =====
 *
 */

fn decode_state(num: usize) -> State {
    State {
        recv_parked: num & RECV_PARKED_MASK == RECV_PARKED_MASK,
        is_open: num & OPEN_MASK == OPEN_MASK,
        num_messages: num & MAX_BUFFER,
    }
}

fn encode_state(state: &State) -> usize {
    let mut num = state.num_messages;

    if state.recv_parked {
        num |= RECV_PARKED_MASK;
    }

    if state.is_open {
        num |= OPEN_MASK;
    }

    num
}
