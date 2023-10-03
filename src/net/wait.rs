use std::any::Any;
use std::error::Error;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;

use crate::net::message::Message;

pub(crate) type ResponseError = Box<dyn Error>;

pub(crate) trait Callback {
    fn on_response(&self, response: Result<Message, ResponseError>);
    fn as_any(&self) -> &dyn Any;
}

pub(crate) struct WaitingList {
    pending_responses: DashMap<i64, Box<dyn Callback>>,
}

impl WaitingList {
    pub(crate) fn new() -> Self {
        WaitingList {
            pending_responses: DashMap::new(),
        }
    }

    pub(crate) fn add(&self, message_id: i64, callback: Box<dyn Callback>) {
        self.pending_responses.insert(message_id, callback);
    }

    pub(crate) fn handle_response(
        &self,
        message_id: i64,
        response: Result<Message, ResponseError>,
    ) {
        self.handle_response_with_clear_entry(message_id, response, true);
    }

    pub(crate) fn handle_response_with_clear_entry(
        &self,
        message_id: i64,
        response: Result<Message, ResponseError>,
        clear_entry: bool,
    ) {
        if clear_entry {
            let key_value_existence = self.pending_responses.remove(&message_id);
            if let Some(callback_by_key) = key_value_existence {
                let callback = callback_by_key.1;
                callback.on_response(response);
            }
            return;
        }
        let key_value_existence = self.pending_responses.get(&message_id);
        if let Some(callback_by_key) = key_value_existence {
            let callback = callback_by_key.value();
            callback.on_response(response);
        }
    }

    #[cfg(test)]
    pub(crate) fn get_callback<'guard>(
        &'guard self,
        message_id: &i64,
    ) -> Option<Ref<'guard, i64, Box<dyn Callback>>> {
        self.pending_responses.get(message_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::net::message::Message;
    use crate::net::wait::tests::setup::{TestCallback, TestError};
    use crate::net::wait::WaitingList;

    mod setup {
        use std::any::Any;
        use std::error::Error;
        use std::fmt::{Display, Formatter};
        use std::sync::Mutex;

        use crate::net::message::Message;
        use crate::net::wait::{Callback, ResponseError};

        pub(crate) struct TestCallback {
            responses: Mutex<Vec<Message>>,
            error_responses: Mutex<Vec<ResponseError>>,
        }

        #[derive(Debug)]
        pub(crate) struct TestError {
            pub(crate) msg: String,
        }

        impl Display for TestError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.msg)
            }
        }

        impl Error for TestError {}

        impl TestCallback {
            pub(crate) fn new() -> Box<TestCallback> {
                Box::new(TestCallback {
                    responses: Mutex::new(Vec::new()),
                    error_responses: Mutex::new(Vec::new()),
                })
            }

            pub(crate) fn get_message_at(&self, index: usize) -> Option<Message> {
                self.responses
                    .lock()
                    .unwrap()
                    .get(index)
                    .map(|message| message.clone())
            }

            pub(crate) fn get_error_at(&self, index: usize) -> Option<TestError> {
                self.error_responses
                    .lock()
                    .unwrap()
                    .get(index)
                    .map(|err| TestError {
                        msg: err.to_string(),
                    })
            }
        }

        impl Callback for TestCallback {
            fn on_response(&self, response: Result<Message, ResponseError>) {
                if response.is_ok() {
                    self.responses.lock().unwrap().push(response.unwrap());
                } else {
                    self.error_responses
                        .lock()
                        .unwrap()
                        .push(response.unwrap_err());
                }
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    #[test]
    fn add_callback_to_waiting_list() {
        let waiting_list = WaitingList::new();
        let callback = TestCallback::new();

        let message_id: i64 = 10;
        waiting_list.add(message_id, callback);
        waiting_list.handle_response_with_clear_entry(
            message_id,
            Ok(Message::shutdown_type()),
            false,
        );

        let callback_by_key_ref = waiting_list.get_callback(&message_id).unwrap();
        let callback = callback_by_key_ref.value();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let message = callback.get_message_at(0).unwrap();
        assert!(message.is_shutdown_type())
    }

    #[test]
    fn add_failure_callback_to_waiting_list() {
        let waiting_list = WaitingList::new();
        let callback = TestCallback::new();

        let message_id: i64 = 10;
        waiting_list.add(message_id, callback);
        waiting_list.handle_response_with_clear_entry(
            message_id,
            Err(Box::new(TestError {
                msg: "test error".to_string(),
            })),
            false,
        );

        let callback_by_key_ref = waiting_list.get_callback(&message_id).unwrap();
        let callback = callback_by_key_ref.value();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let error = callback.get_error_at(0).unwrap();
        assert_eq!("test error", error.msg);
    }

    #[test]
    fn handle_response_for_unknown_message_id() {
        let waiting_list = WaitingList::new();
        let callback = TestCallback::new();

        let message_id: i64 = 10;
        let unknown_message_id: i64 = 20;

        waiting_list.add(message_id, callback);
        waiting_list.handle_response_with_clear_entry(
            unknown_message_id,
            Ok(Message::shutdown_type()),
            false,
        );

        let callback_by_key_ref = waiting_list.get_callback(&message_id).unwrap();
        let callback = callback_by_key_ref.value();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let message = callback.get_message_at(0);
        assert!(message.is_none());
    }
}
