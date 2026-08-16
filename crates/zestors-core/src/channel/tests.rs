use super::*;
use crate::{Interface, Message, Payload, signals::Shutdown};
use std::time::Duration;
use std::{assert_matches, sync::Arc};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use type_sets::Set;

// =========================================================================
// Mock Message & Interface Setup
// =========================================================================

#[derive(Debug, Clone, PartialEq, Eq, Message)]
#[msg(path = "crate")]
struct PingMessage(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Message)]
#[msg(path = "crate")]
struct PongMessage(pub u64);

#[derive(Debug, Interface)]
#[interface(crate = "crate")]
enum TestInterface {
    Ping(Payload<PingMessage>),
    Pong(Payload<PongMessage>),
}

#[derive(Debug, Interface)]
#[interface(crate = "crate")]
enum UnrelatedInterface {}

fn create_running_channel<S: ChannelKind + Interface>() -> Channel<S> {
    let channel = Channel::<S>::new(Pid::default());
    channel.set_status(ActorStatus::Running);
    channel
}

// =========================================================================
// Basic Properties & Status
// =========================================================================

#[tokio::test]
async fn test_actor_lifecycle() {
    let channel = Channel::<TestInterface>::new(Pid::default());

    assert!(channel.status().is_dead());

    let (tx, mut rx) = mpsc::unbounded_channel::<()>();

    let child = channel
        .spawn(async move |mut stream| {
            rx.recv().await.unwrap();

            while let Some(ev) = stream.next().await {
                match ev {
                    Event::Message(msg) => {
                        tracing::info!("Received message: {:?}", msg);
                    }
                    Event::Signal(signal) => {
                        if matches!(
                            signal,
                            SignalEvent::StatusUpdate(StatusUpdateEvent::Shutdown)
                        ) {
                            rx.recv().await.unwrap();
                            return Ok(());
                        }
                    }
                }
            }

            Ok(())
        })
        .unwrap();

    assert!(child.status().is_initializing());

    tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(child.status().is_running());

    child.signal_suspend();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(child.status().is_suspended());

    child.signal_resume();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(child.status().is_running());

    child.signal_shutdown();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(child.status().is_shutting_down());

    tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(child.status().is_dead());
}

#[test]
fn test_pid_is_preserved() {
    let pid = Pid::default();
    let channel = Channel::<TestInterface>::new(pid.clone());

    assert_eq!(channel.pid(), &pid);
}

// =========================================================================
// Signal Handling
// =========================================================================

#[tokio::test]
async fn test_signal_push_pop_and_recv() {
    let channel = create_running_channel::<TestInterface>();

    // Empty initially.
    assert_matches!(channel.pop_signal(), None);

    // Synchronous push/pop.
    channel.signal_shutdown();

    assert_matches!(channel.pop_signal(), Some(_));
    assert_matches!(channel.pop_signal(), None);

    // Async receive.
    let channel_clone = channel.clone();

    let handle = tokio::spawn(async move { channel_clone.recv_signal().await });

    sleep(Duration::from_millis(10)).await;

    channel.signal_shutdown();

    let received = timeout(Duration::from_millis(100), handle)
        .await
        .expect("Receive timed out")
        .expect("Task failed");

    assert_matches!(received, Some(_));
}

// =========================================================================
// Message Sending & Receiving
// =========================================================================

#[tokio::test]
async fn test_send_and_recv_msg() {
    let channel = create_running_channel::<TestInterface>();
    let msg = PingMessage("hello".into());

    let send_res = channel.send(msg.clone()).await;

    assert!(send_res.is_ok());

    let received = channel.recv_msg().await;

    assert!(received.is_some());
}

#[test]
fn test_pop_msg_empty() {
    let channel = create_running_channel::<TestInterface>();

    assert!(channel.pop_msg().is_none());
}

#[test]
fn test_send_now_and_pop_msg() {
    let channel = create_running_channel::<TestInterface>();
    let msg = PingMessage("hello".into());

    let output = channel.send_now(msg);

    assert!(output.is_ok());
    assert!(channel.pop_msg().is_some());
    assert!(channel.pop_msg().is_none());
}

#[test]
fn test_send_when_channel_closed() {
    let channel = create_running_channel::<TestInterface>();

    channel.set_status(ActorStatus::Exiting);

    let msg = PingMessage("closed_test".into());
    let res = channel.try_send(msg.clone());

    match res {
        Err(TrySendError::Closed(returned_msg)) => {
            assert_eq!(returned_msg, msg);
        }
        _ => panic!("Expected TrySendError::Closed"),
    }
}

// =========================================================================
// Clone / Shared State
// =========================================================================

#[test]
fn test_clone_shares_same_arc() {
    let channel = create_running_channel::<TestInterface>();
    let clone = channel.clone();

    assert_eq!(Arc::as_ptr(&channel.inner), Arc::as_ptr(&clone.inner),);
}

#[test]
fn test_clone_shares_queue() {
    let channel = create_running_channel::<TestInterface>();
    let clone = channel.clone();

    channel.send_now(PingMessage("hello".into())).unwrap();

    assert!(clone.pop_msg().is_some());
    assert!(channel.pop_msg().is_none());
}

// =========================================================================
// Type Identification
// =========================================================================

#[test]
fn test_interface_checks() {
    let channel = create_running_channel::<TestInterface>();

    assert!(channel.is_interface::<TestInterface>());
    assert!(!channel.is_interface::<UnrelatedInterface>());
}

#[test]
fn test_interface_check_survives_type_erasure() {
    let channel = create_running_channel::<TestInterface>();

    let dyn_channel = channel.into_dyn_unchecked::<Set!()>();

    assert!(dyn_channel.is_interface::<TestInterface>());
    assert!(!dyn_channel.is_interface::<UnrelatedInterface>());
}

// =========================================================================
// Type-Erased Channel / Allocation Identity
// =========================================================================

#[test]
fn test_into_dyn_preserves_arc_allocation() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel = channel.into_dyn_unchecked::<Set!()>();

    let erased_ptr = Arc::as_ptr(&dyn_channel.inner);

    assert_eq!(original_ptr, erased_ptr);
}

#[test]
fn test_as_dyn_preserves_arc_allocation() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

    let erased_ptr = Arc::as_ptr(&dyn_channel.inner);

    assert_eq!(original_ptr, erased_ptr);
}

#[test]
fn test_multiple_typed_views_share_same_allocation() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

    let typed_channel: &Channel<TestInterface> =
        dyn_channel.downcast_ref::<TestInterface>().unwrap();

    assert_eq!(original_ptr, Arc::as_ptr(&dyn_channel.inner));
    assert_eq!(original_ptr, Arc::as_ptr(&typed_channel.inner));
}

// =========================================================================
// Downcasting
// =========================================================================

#[test]
fn test_interface_checks_and_downcasting() {
    let channel = create_running_channel::<TestInterface>();

    assert!(channel.is_interface::<TestInterface>());

    let channel_ref: Option<&Channel<TestInterface>> = channel.downcast_ref::<TestInterface>();

    assert!(channel_ref.is_some());

    let dyn_channel = channel.into_dyn_unchecked::<Set!()>();

    let downcast_res = dyn_channel.downcast::<TestInterface>();

    assert!(downcast_res.is_ok());
}

#[test]
fn test_downcast_ref_succeeds_for_correct_interface() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

    let restored = dyn_channel
        .downcast_ref::<TestInterface>()
        .expect("expected TestInterface downcast to succeed");

    assert_eq!(original_ptr, Arc::as_ptr(&restored.inner));
}

#[test]
fn test_downcast_ref_fails_for_wrong_interface() {
    let channel = create_running_channel::<TestInterface>();

    let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

    assert!(dyn_channel.downcast_ref::<UnrelatedInterface>().is_none());
}

#[test]
fn test_downcast_failure_returns_original_channel() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel = channel.into_dyn_unchecked::<UnrelatedInterface>();

    assert!(!dyn_channel.is_interface::<UnrelatedInterface>());

    let Err(original) = dyn_channel.downcast::<UnrelatedInterface>() else {
        panic!("expected downcast to fail");
    };

    assert_eq!(original_ptr, Arc::as_ptr(&original.inner));
    assert!(original.is_interface::<TestInterface>());
}

#[test]
fn test_downcast_round_trip_preserves_allocation() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel = channel.into_dyn_unchecked::<Set!()>();

    assert_eq!(original_ptr, Arc::as_ptr(&dyn_channel.inner));

    let Ok(channel) = dyn_channel.downcast::<TestInterface>() else {
        panic!("expected downcast to succeed");
    };

    assert_eq!(original_ptr, Arc::as_ptr(&channel.inner));
}

#[test]
fn test_reference_downcast_round_trip_preserves_allocation() {
    let channel = create_running_channel::<TestInterface>();

    let original_ptr = Arc::as_ptr(&channel.inner);

    let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

    let restored = dyn_channel
        .downcast_ref::<TestInterface>()
        .expect("expected downcast to succeed");

    assert_eq!(original_ptr, Arc::as_ptr(&dyn_channel.inner));
    assert_eq!(original_ptr, Arc::as_ptr(&restored.inner));
}

// =========================================================================
// Raw Queue
// =========================================================================

#[test]
fn test_raw_queue_returns_concrete_queue() {
    let channel = create_running_channel::<TestInterface>();

    let queue = channel
        .raw_queue()
        .expect("expected raw queue downcast to succeed");

    assert_eq!(queue.len(), 0);
}

#[test]
fn test_raw_queue_contains_sent_message() {
    let channel = create_running_channel::<TestInterface>();

    channel.send_now(PingMessage("hello".into())).unwrap();

    let queue = channel
        .raw_queue()
        .expect("expected raw queue downcast to succeed");

    assert_eq!(queue.len(), 1);
}

// =========================================================================
// Backpressure
// =========================================================================

#[tokio::test]
async fn test_try_send_backpressure_limit() {
    let channel = create_running_channel::<TestInterface>();

    for i in 0..BACKPRESSURE_LIMIT {
        let msg = PongMessage(i as u64);
        let _ = channel.send_now(msg);
    }

    let msg = PongMessage(999);

    let res = channel.try_send(msg.clone());

    match res {
        Err(TrySendError::Full(returned_msg)) => {
            assert_eq!(returned_msg, msg);
        }
        _ => panic!("Expected TrySendError::Full due to backpressure"),
    }
}

#[test]
fn test_backpressure_is_shared_between_clones() {
    let channel = create_running_channel::<TestInterface>();
    let clone = channel.clone();

    for i in 0..BACKPRESSURE_LIMIT {
        let _ = channel.send_now(PongMessage(i as u64));
    }

    let msg = PongMessage(999);

    match clone.try_send(msg.clone()) {
        Err(TrySendError::Full(returned_msg)) => {
            assert_eq!(returned_msg, msg);
        }
        _ => panic!("Expected TrySendError::Full"),
    }
}

// =========================================================================
// Type Erasure + Messaging
// =========================================================================

// #[tokio::test]
// async fn test_erased_channel_can_receive_messages() {
//     let channel = create_test_channel::<TestInterface>();

//     let dyn_channel = channel.into_dyn_unchecked::<Set!()>();

//     dyn_channel
//         .send_now_checked(PingMessage("hello".into()))
//         .unwrap();

//     assert!(dyn_channel.recv_msg().await.is_some());

//     let Err(TrySendCheckedError::NotAccepted(_)) = dyn_channel.try_send_checked(8u32) else {
//         panic!("Expected NotAccepted error for PongMessage");
//     };
// }

// #[tokio::test]
// async fn test_typed_and_erased_views_share_message_queue() {
//     let channel = create_test_channel::<TestInterface>();

//     let dyn_channel: &Channel<Set!()> = channel.as_dyn_unchecked::<Set!()>();

//     channel.send_now(PingMessage("hello".into())).unwrap();

//     assert!(dyn_channel.pop_msg().is_some());
//     assert!(channel.pop_msg().is_none());
// }

#[tokio::test]
async fn test_recv_msg_waits_for_message() {
    let channel = create_running_channel::<TestInterface>();
    let receiver = channel.clone();

    sleep(Duration::from_millis(10)).await;

    channel.send_now(PingMessage("hello".into())).unwrap();

    let received = timeout(Duration::from_millis(100), receiver.recv_msg())
        // let handle = tokio::spawn(async move { .await });
        .await
        .expect("Receive timed out");

    assert!(received.is_some());
}
