use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::{Receiver, Sender};

use crate::net::message::Message;

pub(crate) struct ChanneledMessage {
    pub(crate) message: Message,
    response_sender: Sender<MessageStatus>,
}

impl ChanneledMessage {
    pub(crate) fn new(message: Message, response_sender: Sender<MessageStatus>) -> Self {
        ChanneledMessage {
            message,
            response_sender,
        }
    }

    pub(crate) fn send_response(self, status: MessageStatus) -> Result<(), MessageStatus> {
        self.response_sender.send(status)
    }
}

pub(crate) enum MessageStatus {
    StoreDone,
    PingDone,
    PingReplyDone,
    AddNodeDone,
    ShutdownDone,
}

impl MessageStatus {
    pub(crate) fn is_store_done(&self) -> bool {
        if let MessageStatus::StoreDone = self {
            return true;
        }
        return false;
    }
}

pub(crate) struct MessageResponse {
    receiver: Receiver<MessageStatus>,
}

impl MessageResponse {
    pub(crate) fn new(response_receiver: Receiver<MessageStatus>) -> Self {
        MessageResponse {
            receiver: response_receiver,
        }
    }

    pub(crate) async fn wait_until_response_is_received(self) -> Result<MessageStatus, RecvError> {
        self.receiver.await
    }
}

#[cfg(test)]
mod channeled_message_tests {
    use tokio::sync::oneshot;

    use crate::executor::response::{ChanneledMessage, MessageStatus};
    use crate::net::message::Message;

    #[tokio::test]
    async fn send_response() {
        let (sender, receiver) = oneshot::channel();
        let channeled_message = ChanneledMessage::new(Message::shutdown_type(), sender);

        let send_result = channeled_message.send_response(MessageStatus::StoreDone);
        assert!(send_result.is_ok());

        let message_status = receiver.await.unwrap();
        assert!(message_status.is_store_done());
    }

    #[test]
    fn send_response_with_failure() {
        let (sender, receiver) = oneshot::channel();
        let channeled_message = ChanneledMessage::new(Message::shutdown_type(), sender);

        drop(receiver);

        let send_result = channeled_message.send_response(MessageStatus::StoreDone);
        assert!(send_result.is_err());
    }
}

#[cfg(test)]
mod message_response_tests {
    use tokio::sync::oneshot;

    use crate::executor::response::{MessageResponse, MessageStatus};

    #[tokio::test]
    async fn wait_until_the_message_response_is_received() {
        let (sender, receiver) = oneshot::channel();
        let message_response = MessageResponse::new(receiver);

        let handle = tokio::spawn(async move {
            let message_response_result = message_response.wait_until_response_is_received().await;
            assert!(message_response_result.is_ok());

            let message_status = message_response_result.unwrap();
            assert!(message_status.is_store_done());
        });

        let other_handle = tokio::spawn(async move {
            let message_status_send_result = sender.send(MessageStatus::StoreDone);
            assert!(message_status_send_result.is_ok());
        });

        handle.await.unwrap();
        other_handle.await.unwrap();
    }
}
