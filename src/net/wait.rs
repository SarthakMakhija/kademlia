use std::any::Any;
use std::error::Error;
use std::time::{Duration, SystemTime};

use dashmap::mapref::one::Ref;
use dashmap::DashMap;

use crate::net::message::Message;
use crate::time::Clock;

pub(crate) type ResponseError = Box<dyn Error>;

pub(crate) trait Callback {
    fn on_response(&self, response: Result<Message, ResponseError>);
    fn as_any(&self) -> &dyn Any;
}

pub(crate) struct TimedCallback {
    callback: Box<dyn Callback>,
    creation_time: SystemTime,
}

impl TimedCallback {
    fn new(callback: Box<dyn Callback>, creation_time: SystemTime) -> Self {
        TimedCallback {
            callback,
            creation_time,
        }
    }

    fn on_response(&self, response: Result<Message, ResponseError>) {
        self.callback.on_response(response);
    }

    fn has_expired(&self, clock: &Box<dyn Clock>, expiry_after: &Duration) -> bool {
        clock.duration_since(self.creation_time).gt(expiry_after)
    }

    #[cfg(test)]
    fn get_callback(&self) -> &Box<dyn Callback> {
        &self.callback
    }
}

pub(crate) struct WaitingList {
    pending_responses: DashMap<i64, TimedCallback>,
    clock: Box<dyn Clock>,
}

impl WaitingList {
    pub(crate) fn new(clock: Box<dyn Clock>) -> Self {
        WaitingList {
            pending_responses: DashMap::new(),
            clock,
        }
    }

    pub(crate) fn add(&self, message_id: i64, callback: Box<dyn Callback>) {
        self.pending_responses
            .insert(message_id, TimedCallback::new(callback, self.clock.now()));
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
    pub(crate) fn get_callback(&self, message_id: &i64) -> Option<Ref<i64, TimedCallback>> {
        self.pending_responses.get(message_id)
    }
}

#[cfg(test)]
mod waiting_list_tests {
    use crate::net::message::Message;
    use crate::net::wait::waiting_list_tests::setup::{TestCallback, TestError};
    use crate::net::wait::WaitingList;
    use crate::time::SystemClock;

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
        let waiting_list = WaitingList::new(SystemClock::new());
        let callback = TestCallback::new();

        let message_id: i64 = 10;
        waiting_list.add(message_id, callback);
        waiting_list.handle_response_with_clear_entry(
            message_id,
            Ok(Message::shutdown_type()),
            false,
        );

        let callback_by_key_ref = waiting_list.get_callback(&message_id).unwrap();
        let callback = callback_by_key_ref.get_callback();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let message = callback.get_message_at(0).unwrap();
        assert!(message.is_shutdown_type())
    }

    #[test]
    fn add_failure_callback_to_waiting_list() {
        let waiting_list = WaitingList::new(SystemClock::new());
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
        let callback = callback_by_key_ref.get_callback();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let error = callback.get_error_at(0).unwrap();
        assert_eq!("test error", error.msg);
    }

    #[test]
    fn handle_response_for_unknown_message_id() {
        let waiting_list = WaitingList::new(SystemClock::new());
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
        let callback = callback_by_key_ref.get_callback();
        let callback = callback.as_any().downcast_ref::<TestCallback>().unwrap();

        let message = callback.get_message_at(0);
        assert!(message.is_none());
    }
}

#[cfg(test)]
mod timed_callback_tests {
    use crate::net::wait::timed_callback_tests::setup::{FutureClock, NothingCallback};
    use crate::net::wait::TimedCallback;
    use crate::time::{Clock, SystemClock};
    use std::time::Duration;

    mod setup {
        use std::any::Any;
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::net::message::Message;
        use crate::net::wait::{Callback, ResponseError};
        use crate::time::Clock;

        #[derive(Clone)]
        pub struct FutureClock {
            pub(crate) duration_to_add: Duration,
        }

        pub(crate) struct NothingCallback;

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                SystemTime::now().add(self.duration_to_add)
            }
        }

        impl Callback for NothingCallback {
            fn on_response(&self, _response: Result<Message, ResponseError>) {}

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    #[test]
    fn has_expired() {
        let timed_callback =
            TimedCallback::new(Box::new(NothingCallback), SystemClock::new().now());

        let clock: Box<dyn Clock> = Box::new(FutureClock {
            duration_to_add: Duration::from_secs(5),
        });

        assert!(timed_callback.has_expired(&clock, &Duration::from_secs(2)));
    }

    #[test]
    fn has_not_expired() {
        let timed_callback =
            TimedCallback::new(Box::new(NothingCallback), SystemClock::new().now());

        let clock: Box<dyn Clock> = SystemClock::new();

        assert_eq!(
            false,
            timed_callback.has_expired(&clock, &Duration::from_secs(2))
        );
    }
}
