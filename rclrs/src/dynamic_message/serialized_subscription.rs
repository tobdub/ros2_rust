use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use super::{create_runtime_typed_subscription, MessageTypeName};
use crate::{
    rcl_bindings::*, MessageInfo, Node, NodeHandle, RclPrimitive, RclPrimitiveHandle,
    RclPrimitiveKind, RclrsError, RclrsErrorFilter, ReadyKind, SubscriptionHandle,
    SubscriptionOptions, ToResult, Waitable, WaitableLifecycle, WorkScope, Worker, WorkerCommands,
};

/// A message in its serialized form, as the middleware sends it over the wire.
///
/// For the DDS middlewares this is CDR, including the 4-byte encapsulation
/// header. The buffer grows as needed when a message is taken into it.
pub struct SerializedMessage {
    msg: rcl_serialized_message_t,
}

// SAFETY: The message exclusively owns its buffer, and the allocator it uses
// is the thread-safe default allocator.
unsafe impl Send for SerializedMessage {}
// SAFETY: A shared reference only allows reading the buffer.
unsafe impl Sync for SerializedMessage {}

impl SerializedMessage {
    /// Creates an empty serialized message. It does not allocate until a
    /// message is taken into it.
    pub fn new() -> Self {
        Self::with_capacity(0).expect("Initializing an empty serialized message does not allocate")
    }

    /// Creates an empty serialized message that can hold `capacity` bytes
    /// before it needs to grow.
    pub fn with_capacity(capacity: usize) -> Result<Self, RclrsError> {
        // SAFETY: Getting a zero-initialized value and the default allocator is always safe.
        let mut msg = unsafe { rcutils_get_zero_initialized_uint8_array() };
        let allocator = unsafe { rcutils_get_default_allocator() };
        // SAFETY: The array is zero-initialized and the allocator is valid.
        unsafe { rcutils_uint8_array_init(&mut msg, capacity, &allocator).ok()? };
        Ok(Self { msg })
    }

    /// The serialized bytes of the message.
    pub fn as_bytes(&self) -> &[u8] {
        if self.msg.buffer.is_null() {
            return &[];
        }
        // SAFETY: The buffer holds at least buffer_length initialized bytes.
        unsafe { std::slice::from_raw_parts(self.msg.buffer, self.msg.buffer_length) }
    }
}

impl Default for SerializedMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8]> for SerializedMessage {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::fmt::Debug for SerializedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedMessage")
            .field("len", &self.msg.buffer_length)
            .field("capacity", &self.msg.buffer_capacity)
            .finish()
    }
}

impl Drop for SerializedMessage {
    fn drop(&mut self) {
        // SAFETY: The array was initialized in with_capacity and is not used after this.
        unsafe {
            rcutils_uint8_array_fini(&mut self.msg);
        }
    }
}

/// Struct for receiving messages in their serialized form, without
/// deserializing them.
///
/// Create a serialized subscription using
/// [`NodeState::create_serialized_subscription()`][1].
///
/// The message type is given at runtime, like for a [`DynamicSubscription`][2],
/// and the same lifetime rules as for a [`Subscription`][3] apply.
///
/// [1]: crate::NodeState::create_serialized_subscription
/// [2]: crate::DynamicSubscription
/// [3]: crate::Subscription
pub type SerializedSubscription = Arc<SerializedSubscriptionState<Node>>;

/// A [`SerializedSubscription`] that runs on a [`Worker`].
///
/// Create a worker serialized subscription using
/// [`WorkerState::create_serialized_subscription`][1].
///
/// [1]: crate::WorkerState::create_serialized_subscription
pub type WorkerSerializedSubscription<Payload> = Arc<SerializedSubscriptionState<Worker<Payload>>>;

type WorkerSerializedCallback<Payload> = Box<dyn FnMut(&mut Payload, &[u8], MessageInfo) + Send>;

pub(crate) enum SerializedSubscriptionCallback<Payload> {
    /// Runs on the executor. Each message is taken into its own buffer.
    Node(Arc<dyn Fn(SerializedMessage, MessageInfo) + Send + Sync>),
    /// Runs on the worker's thread with the bytes borrowed from a buffer that
    /// is reused for every message.
    Worker(WorkerSerializedCallback<Payload>),
}

impl SerializedSubscriptionCallback<()> {
    pub(crate) fn node(f: impl Fn(SerializedMessage, MessageInfo) + Send + Sync + 'static) -> Self {
        Self::Node(Arc::new(f))
    }
}

impl<Payload> SerializedSubscriptionCallback<Payload> {
    pub(crate) fn worker(f: impl FnMut(&mut Payload, &[u8], MessageInfo) + Send + 'static) -> Self {
        Self::Worker(Box::new(f))
    }
}

struct SerializedSubscriptionExecutable<Payload> {
    handle: Arc<SubscriptionHandle>,
    callback: Arc<Mutex<SerializedSubscriptionCallback<Payload>>>,
    commands: Arc<WorkerCommands>,
    /// Reused for every message delivered to a worker callback.
    buffer: SerializedMessage,
}

/// Takes the next message of the subscription into `buffer`, growing it if needed.
fn take_serialized(
    handle: &SubscriptionHandle,
    buffer: &mut SerializedMessage,
) -> Result<MessageInfo, RclrsError> {
    let mut message_info = unsafe { rmw_get_zero_initialized_message_info() };
    let rcl_subscription = &mut *handle.lock();
    unsafe {
        // SAFETY: The subscription and the serialized message are valid and
        // initialized. rmw resizes the serialized message with its own
        // allocator if it is too small. The allocation pointer may be NULL.
        rcl_take_serialized_message(
            rcl_subscription,
            &mut buffer.msg,
            &mut message_info,
            std::ptr::null_mut(),
        )
        .ok()?;
    }
    Ok(MessageInfo::from_rmw_message_info(&message_info))
}

impl<Payload: 'static> SerializedSubscriptionExecutable<Payload> {
    fn execute_callback(&mut self, any_payload: &mut dyn Any) -> Result<(), RclrsError> {
        let Some(payload) = any_payload.downcast_mut::<Payload>() else {
            return Err(RclrsError::InvalidPayload {
                expected: std::any::TypeId::of::<Payload>(),
                received: (*any_payload).type_id(),
            });
        };
        let Self {
            handle,
            callback,
            commands,
            buffer,
        } = self;
        let mut evaluate = || {
            match &mut *callback.lock().unwrap() {
                SerializedSubscriptionCallback::Node(cb) => {
                    let mut msg = SerializedMessage::new();
                    let msg_info = take_serialized(handle, &mut msg)?;
                    let cb = Arc::clone(cb);
                    commands.run_async(Box::pin(async move {
                        cb(msg, msg_info);
                    }));
                }
                SerializedSubscriptionCallback::Worker(cb) => {
                    let msg_info = take_serialized(handle, buffer)?;
                    cb(payload, buffer.as_bytes(), msg_info);
                }
            }
            Ok(())
        };
        evaluate().take_failed_ok()
    }
}

impl<Payload: 'static> RclPrimitive for SerializedSubscriptionExecutable<Payload> {
    unsafe fn execute(
        &mut self,
        ready: ReadyKind,
        payload: &mut dyn Any,
    ) -> Result<(), RclrsError> {
        ready.for_basic()?;
        self.execute_callback(payload)
    }

    fn kind(&self) -> RclPrimitiveKind {
        RclPrimitiveKind::Subscription
    }

    fn handle(&self) -> RclPrimitiveHandle<'_> {
        RclPrimitiveHandle::Subscription(self.handle.lock())
    }
}

/// Struct for receiving serialized messages whose type is given at runtime.
pub struct SerializedSubscriptionState<Scope>
where
    Scope: WorkScope,
{
    /// This handle is used to access the data that rcl holds for this subscription.
    handle: Arc<SubscriptionHandle>,
    /// Holding onto this keeps the waiter for this subscription alive in the
    /// wait set of the executor.
    #[allow(unused)]
    lifecycle: WaitableLifecycle,
    // The rosidl_typesupport_c library of the message type. The subscription
    // refers to the type support inside it, so it must stay loaded.
    #[allow(dead_code)]
    type_support_library: Arc<libloading::Library>,
    #[allow(dead_code)]
    scope: std::marker::PhantomData<Scope>,
}

impl<Scope> SerializedSubscriptionState<Scope>
where
    Scope: WorkScope,
{
    /// Creates a new serialized subscription.
    ///
    /// This is not a public function, by the same rationale as `Subscription::new()`.
    pub(crate) fn create<'a>(
        topic_type: MessageTypeName,
        options: impl Into<SubscriptionOptions<'a>>,
        callback: SerializedSubscriptionCallback<Scope::Payload>,
        node_handle: &Arc<NodeHandle>,
        commands: &Arc<WorkerCommands>,
    ) -> Result<Arc<Self>, RclrsError> {
        let (handle, type_support_library) =
            create_runtime_typed_subscription(&topic_type, options, node_handle)?;

        let (waitable, lifecycle) = Waitable::new(
            Box::new(SerializedSubscriptionExecutable {
                handle: Arc::clone(&handle),
                callback: Arc::new(Mutex::new(callback)),
                commands: Arc::clone(commands),
                buffer: SerializedMessage::new(),
            }),
            Some(Arc::clone(commands.get_guard_condition())),
        );
        commands.add_to_wait_set(waitable);

        Ok(Arc::new(Self {
            handle,
            lifecycle,
            type_support_library,
            scope: std::marker::PhantomData,
        }))
    }

    /// Returns the topic name of the subscription.
    ///
    /// This returns the topic name after remapping, so it is not necessarily the
    /// topic name which was used when creating the subscription.
    pub fn topic_name(&self) -> String {
        self.handle.topic_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use futures::{channel::mpsc, StreamExt};
    use ros_env::example_interfaces::msg::String as StringMsg;
    use std::time::Duration;

    #[test]
    fn serialized_subscription_is_send_and_sync() {
        use crate::test_helpers::*;
        assert_send::<SerializedSubscription>();
        assert_sync::<SerializedSubscription>();
        assert_send::<WorkerSerializedSubscription<()>>();
        assert_sync::<WorkerSerializedSubscription<()>>();
        assert_send::<SerializedMessage>();
        assert_sync::<SerializedMessage>();
    }

    #[test]
    fn empty_serialized_message() {
        let msg = SerializedMessage::new();
        assert!(msg.as_bytes().is_empty());
        let msg = SerializedMessage::with_capacity(64).unwrap();
        assert!(msg.as_bytes().is_empty());
    }

    /// The CDR encoding of a string field: a u32 length that counts the
    /// terminating NUL, then the bytes and the NUL.
    fn contains_cdr_string(bytes: &[u8], s: &str) -> bool {
        let mut encoded = ((s.len() + 1) as u32).to_le_bytes().to_vec();
        encoded.extend_from_slice(s.as_bytes());
        encoded.push(0);
        let mut big_endian = ((s.len() + 1) as u32).to_be_bytes().to_vec();
        big_endian.extend_from_slice(&encoded[4..]);
        bytes.windows(encoded.len()).any(|w| w == encoded)
            || bytes.windows(big_endian.len()).any(|w| w == big_endian)
    }

    #[test]
    fn test_serialized_subscriptions() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_basic_executor();
        let node =
            executor.create_node(format!("test_serialized_subscriptions_{}", line!()).as_str())?;
        let topic = "test_serialized_subscriptions";
        let topic_type: MessageTypeName = "example_interfaces/msg/String".try_into()?;

        let (sender, mut receiver) = mpsc::unbounded();
        let node_sender = sender.clone();
        let node_subscription = node.create_serialized_subscription(
            topic_type.clone(),
            topic,
            move |msg: SerializedMessage, _: MessageInfo| {
                let _ = node_sender.unbounded_send(("node", msg.as_bytes().to_vec()));
            },
        )?;
        assert_eq!(node_subscription.topic_name(), format!("/{topic}"));

        let worker = node.create_worker(());
        let _worker_subscription = worker.create_serialized_subscription(
            topic_type,
            topic,
            move |_: &mut (), bytes: &[u8], _: MessageInfo| {
                let _ = sender.unbounded_send(("worker", bytes.to_vec()));
            },
        )?;

        let publisher = node.create_publisher::<StringMsg>(topic)?;
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        let promise = executor.commands().run(async move {
            publisher.notify_on_subscriber_ready().await.unwrap();
            // The worker reuses its buffer: grow it, then take a shorter message.
            for data in ["first", "second message", "3rd"] {
                publisher
                    .publish(StringMsg {
                        data: data.to_string(),
                    })
                    .unwrap();
            }
            for _ in 0..6 {
                if let Some(r) = receiver.next().await {
                    received_clone.lock().unwrap().push(r);
                }
            }
        });

        let r = executor.spin(
            SpinOptions::default()
                .until_promise_resolved(promise)
                .timeout(Duration::from_secs(5)),
        );
        assert!(r.is_empty(), "{r:?}");

        let received = received.lock().unwrap();
        let messages = |scope: &str| -> Vec<Vec<u8>> {
            received
                .iter()
                .filter(|(s, _)| *s == scope)
                .map(|(_, bytes)| bytes.clone())
                .collect()
        };
        let node_messages = messages("node");
        assert_eq!(node_messages.len(), 3, "{received:?}");
        for (bytes, expected) in node_messages.iter().zip(["first", "second message", "3rd"]) {
            assert!(contains_cdr_string(bytes, expected), "{bytes:?}");
        }
        // The worker's reused buffer yields exactly the same bytes, also after
        // a longer message.
        assert_eq!(messages("worker"), node_messages);
        Ok(())
    }

    #[test]
    fn test_serialized_subscription_unknown_type() {
        let executor = Context::default().create_basic_executor();
        let node = executor
            .create_node(format!("test_serialized_unknown_type_{}", line!()).as_str())
            .unwrap();
        let result = node.create_serialized_subscription(
            MessageTypeName {
                package_name: "no_such_package".to_owned(),
                type_name: "NoSuchType".to_owned(),
            },
            "test_serialized_unknown_type",
            |_: SerializedMessage, _: MessageInfo| {},
        );
        assert!(result.is_err());
    }
}
