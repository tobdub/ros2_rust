use rosidl_runtime_rs::Message;

use super::{MessageInfo, SubscriptionHandle};
use crate::{RclrsError, RclrsErrorFilter, ReadOnlyLoanedMessage, WorkerCommands};

use futures::future::BoxFuture;

use std::{sync::Arc, task::Context};

/// An enum capturing the various possible function signatures for subscription callbacks
/// that can be used with the async worker.
///
/// The correct enum variant is deduced by the [`IntoNodeSubscriptionCallback`][1] or
/// [`IntoAsyncSubscriptionCallback`][2] trait.
///
/// [1]: crate::IntoNodeSubscriptionCallback
/// [2]: crate::IntoAsyncSubscriptionCallback
pub enum NodeSubscriptionCallback<T: Message> {
    /// A callback with only the message as an argument.
    Regular(Box<dyn FnMut(T) -> BoxFuture<'static, ()> + Send>),
    /// A callback with the message and the message info as arguments.
    RegularWithMessageInfo(Box<dyn FnMut(T, MessageInfo) -> BoxFuture<'static, ()> + Send>),
    /// A callback with only the boxed message as an argument.
    Boxed(Box<dyn FnMut(Box<T>) -> BoxFuture<'static, ()> + Send>),
    /// A callback with the boxed message and the message info as arguments.
    BoxedWithMessageInfo(Box<dyn FnMut(Box<T>, MessageInfo) -> BoxFuture<'static, ()> + Send>),
    /// A callback with only the loaned message as an argument.
    #[allow(clippy::type_complexity)]
    Loaned(Box<dyn FnMut(ReadOnlyLoanedMessage<T>) -> BoxFuture<'static, ()> + Send>),
    /// A callback with the loaned message and the message info as arguments.
    #[allow(clippy::type_complexity)]
    LoanedWithMessageInfo(
        Box<dyn FnMut(ReadOnlyLoanedMessage<T>, MessageInfo) -> BoxFuture<'static, ()> + Send>,
    ),
}

impl<T: Message> NodeSubscriptionCallback<T> {
    /// Takes the next message and runs the callback.
    ///
    /// The callback's future is polled once right away, on the thread that
    /// took the message. A future that completes on its first poll, which
    /// includes every non-async callback, is done there. Only a future that
    /// is still pending is handed to the executor, which continues polling it.
    pub(super) fn execute(
        &mut self,
        handle: &Arc<SubscriptionHandle>,
        commands: &WorkerCommands,
    ) -> Result<(), RclrsError> {
        let mut evaluate = || {
            match self {
                NodeSubscriptionCallback::Regular(cb) => {
                    let (msg, _) = handle.take::<T>()?;
                    run_inline_or_async(commands, cb(msg));
                }
                NodeSubscriptionCallback::RegularWithMessageInfo(cb) => {
                    let (msg, msg_info) = handle.take::<T>()?;
                    run_inline_or_async(commands, cb(msg, msg_info));
                }
                NodeSubscriptionCallback::Boxed(cb) => {
                    let (msg, _) = handle.take_boxed::<T>()?;
                    run_inline_or_async(commands, cb(msg));
                }
                NodeSubscriptionCallback::BoxedWithMessageInfo(cb) => {
                    let (msg, msg_info) = handle.take_boxed::<T>()?;
                    run_inline_or_async(commands, cb(msg, msg_info));
                }
                NodeSubscriptionCallback::Loaned(cb) => {
                    let (msg, _) = handle.take_loaned::<T>()?;
                    run_inline_or_async(commands, cb(msg));
                }
                NodeSubscriptionCallback::LoanedWithMessageInfo(cb) => {
                    let (msg, msg_info) = handle.take_loaned::<T>()?;
                    run_inline_or_async(commands, cb(msg, msg_info));
                }
            }
            Ok(())
        };

        evaluate().take_failed_ok()
    }
}

/// Polls `future` once on the current thread and hands it to the executor only
/// if it is still pending.
///
/// Handing every callback future to the executor costs an allocation, a
/// channel send and a thread wakeup per message, and the executor would poll
/// the future only once anyway for any callback that does not await.
fn run_inline_or_async(commands: &WorkerCommands, mut future: BoxFuture<'static, ()>) {
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    if future.as_mut().poll(&mut cx).is_pending() {
        // The executor polls it again with its own waker, which replaces the
        // no-op waker given to this first poll.
        commands.run_async(future);
    }
}
