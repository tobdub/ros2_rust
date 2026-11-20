use std::{ops::Deref, sync::Arc};

use rosidl_runtime_rs::Message;

use crate::{rcl_bindings::*, subscription::SubscriptionHandle, ToResult};

/// A message that is owned by the middleware, loaned out for reading.
///
/// It dereferences to a `&T::RmwMsg`. That is, if `T` is already an RMW-native
/// message, it's the same as `&T`, and otherwise it's the corresponding RMW-native
/// message.
///
/// This type may be used in subscription callbacks to receive a message. The
/// loan is returned by dropping the `ReadOnlyLoanedMessage`.
///
/// If the middleware cannot loan messages for the subscription (for example
/// because the message type is unbounded), the message is taken into a heap
/// allocation instead, which is dropped with the `ReadOnlyLoanedMessage`. This
/// matches what rclcpp does for subscriptions that cannot loan.
pub struct ReadOnlyLoanedMessage<T>
where
    T: Message,
{
    pub(super) storage: LoanStorage<T>,
}

/// Where the message of a [`ReadOnlyLoanedMessage`] lives.
pub(super) enum LoanStorage<T>
where
    T: Message,
{
    /// Loaned from the middleware, returned on drop.
    Loaned {
        msg_ptr: *const T::RmwMsg,
        handle: Arc<SubscriptionHandle>,
    },
    /// Taken with a regular take because the middleware cannot loan it.
    Owned(Box<T::RmwMsg>),
}

impl<T> Deref for ReadOnlyLoanedMessage<T>
where
    T: Message,
{
    type Target = T::RmwMsg;
    fn deref(&self) -> &Self::Target {
        match &self.storage {
            LoanStorage::Loaned { msg_ptr, .. } => unsafe { &**msg_ptr },
            LoanStorage::Owned(msg) => msg,
        }
    }
}

impl<T> Drop for ReadOnlyLoanedMessage<T>
where
    T: Message,
{
    fn drop(&mut self) {
        if let LoanStorage::Loaned { msg_ptr, handle } = &self.storage {
            unsafe {
                rcl_return_loaned_message_from_subscription(&*handle.lock(), *msg_ptr as *mut _)
                    .ok()
                    .unwrap();
            }
        }
    }
}

// SAFETY: The functions accessing this type, including drop(), shouldn't care about the thread
// they are running in. Therefore, this type can be safely sent to another thread.
unsafe impl<T> Send for ReadOnlyLoanedMessage<T> where T: Message {}
// SAFETY: This type has no interior mutability, in fact it has no mutability at all.
unsafe impl<T> Sync for ReadOnlyLoanedMessage<T> where T: Message {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traits() {
        use crate::test_helpers::*;
        use ros_env::test_msgs;

        assert_send::<ReadOnlyLoanedMessage<test_msgs::msg::rmw::BoundedSequences>>();
        assert_sync::<ReadOnlyLoanedMessage<test_msgs::msg::rmw::BoundedSequences>>();
    }
}
