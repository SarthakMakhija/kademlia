use crate::net::message::Message;
use std::sync::mpsc::{Receiver, RecvError, SendError, Sender};

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

    pub(crate) fn send_response(
        &self,
        status: MessageStatus,
    ) -> Result<(), SendError<MessageStatus>> {
        self.response_sender.send(status)
    }
}

pub(crate) enum MessageStatus {
    StoreDone,
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

    pub(crate) fn wait_until_response_is_received(&self) -> Result<MessageStatus, RecvError> {
        self.receiver.recv()
    }
}

#[cfg(test)]
mod channeled_message_tests {
    use std::sync::mpsc;

    use crate::executor::response::{ChanneledMessage, MessageStatus};
    use crate::net::message::Message;

    #[test]
    fn send_response() {
        let (sender, receiver) = mpsc::channel();
        let channeled_message = ChanneledMessage::new(Message::shutdown_type(), sender);

        let send_result = channeled_message.send_response(MessageStatus::StoreDone);
        assert!(send_result.is_ok());

        let message_status = receiver.recv().unwrap();
        assert!(message_status.is_store_done());
    }

    #[test]
    fn send_response_with_failure() {
        let (sender, receiver) = mpsc::channel();
        let channeled_message = ChanneledMessage::new(Message::shutdown_type(), sender);

        drop(receiver);

        let send_result = channeled_message.send_response(MessageStatus::StoreDone);
        assert!(send_result.is_err());
    }
}

#[cfg(test)]
mod message_response_tests {
    use std::sync::mpsc;
    use std::thread;

    use crate::executor::response::{MessageResponse, MessageStatus};

    #[test]
    fn wait_until_the_message_response_is_received() {
        let (sender, receiver) = mpsc::channel();
        let message_response = MessageResponse::new(receiver);

        thread::scope(|scope| {
            scope.spawn(move || {
                let message_response_result = message_response.wait_until_response_is_received();
                assert!(message_response_result.is_ok());

                let message_status = message_response_result.unwrap();
                assert!(message_status.is_store_done());
            });

            scope.spawn(move || {
                let message_status_send_result = sender.send(MessageStatus::StoreDone);
                assert!(message_status_send_result.is_ok());
            });
        });
    }
}
