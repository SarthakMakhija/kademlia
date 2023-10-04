use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use dashmap::mapref::one::Ref;
use dashmap::DashMap;

use crate::net::message::Message;
use crate::time::Clock;

pub(crate) type ResponseError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
pub struct ResponseTimeoutError {
    pub message_id: i64,
}

impl Display for ResponseTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "response timeout for {}", self.message_id)
    }
}

impl Error for ResponseTimeoutError {}

pub(crate) trait Callback: Send + Sync + 'static {
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

    fn on_timeout_response(&self, message_id: &i64) {
        self.callback
            .on_response(Err(Box::new(ResponseTimeoutError {
                message_id: *message_id,
            })));
    }

    fn has_expired(&self, clock: &Box<dyn Clock>, expiry_after: &Duration) -> bool {
        clock.duration_since(self.creation_time).gt(expiry_after)
    }

    #[cfg(test)]
    fn get_callback(&self) -> &Box<dyn Callback> {
        &self.callback
    }
}

#[derive(Copy, Clone)]
pub(crate) struct WaitingListOptions {
    pub(crate) expire_pending_responses_after: Duration,
    pub(crate) run_expired_pending_responses_checker_every: Duration,
}

impl WaitingListOptions {
    pub(crate) fn new(
        expire_pending_responses_after: Duration,
        run_expired_pending_responses_checker_every: Duration,
    ) -> Self {
        WaitingListOptions {
            expire_pending_responses_after,
            run_expired_pending_responses_checker_every,
        }
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

struct ExpiredPendingResponsesCleaner {
    pending_responses: Arc<DashMap<i64, TimedCallback>>,
    clock: Box<dyn Clock>,
    expiry_after: Duration,
    should_stop: AtomicBool,
}

impl ExpiredPendingResponsesCleaner {
    fn new(
        waiting_list_options: WaitingListOptions,
        pending_responses: Arc<DashMap<i64, TimedCallback>>,
        clock: Box<dyn Clock>,
    ) -> Arc<ExpiredPendingResponsesCleaner> {
        let cleaner = Arc::new(ExpiredPendingResponsesCleaner {
            pending_responses,
            clock,
            expiry_after: waiting_list_options.expire_pending_responses_after,
            should_stop: AtomicBool::new(false),
        });
        cleaner.clone().start(waiting_list_options);
        cleaner
    }

    fn start(self: Arc<ExpiredPendingResponsesCleaner>, waiting_list_options: WaitingListOptions) {
        thread::spawn(move || loop {
            if self.should_stop.load(Ordering::Acquire) {
                return;
            }
            (&self).clean();
            thread::sleep(waiting_list_options.run_expired_pending_responses_checker_every);
        });
    }

    fn stop(self: &Arc<ExpiredPendingResponsesCleaner>) {
        self.should_stop.store(true, Ordering::Release);
    }

    fn clean(self: &Arc<ExpiredPendingResponsesCleaner>) {
        self.pending_responses.retain(|message_id, timed_callback| {
            if timed_callback.has_expired(&self.clock, &self.expiry_after) {
                timed_callback.on_timeout_response(message_id);
                return false;
            }
            return true;
        });
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
    use std::time::Duration;

    use crate::net::wait::timed_callback_tests::setup::{FutureClock, NothingCallback};
    use crate::net::wait::TimedCallback;
    use crate::time::{Clock, SystemClock};

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

#[cfg(test)]
mod expired_pending_responses_cleaner_tests {
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, SystemTime};

    use dashmap::DashMap;

    use crate::net::wait::expired_pending_responses_cleaner_tests::setup::{
        FutureClock, TimeoutErrorResponseCallback,
    };
    use crate::net::wait::{ExpiredPendingResponsesCleaner, TimedCallback, WaitingListOptions};
    use crate::time::Clock;

    mod setup {
        use std::any::Any;
        use std::ops::Add;
        use std::sync::Mutex;
        use std::time::{Duration, SystemTime};

        use crate::net::message::Message;
        use crate::net::wait::{Callback, ResponseError, ResponseTimeoutError};
        use crate::time::Clock;

        #[derive(Clone)]
        pub struct FutureClock {
            pub(crate) duration_to_add: Duration,
        }

        pub struct TimeoutErrorResponseCallback {
            pub(crate) failed_message_id: Mutex<i64>,
        }

        impl Clock for FutureClock {
            fn now(&self) -> SystemTime {
                SystemTime::now().add(self.duration_to_add)
            }
        }

        impl Callback for TimeoutErrorResponseCallback {
            fn on_response(&self, response: Result<Message, ResponseError>) {
                let response_error_type = response.unwrap_err();
                let timeout_error = response_error_type
                    .downcast_ref::<ResponseTimeoutError>()
                    .unwrap();
                let mut guard = self.failed_message_id.lock().unwrap();
                *guard = timeout_error.message_id;
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    }

    #[test]
    fn error_response_on_expired_key() {
        let message_id: i64 = 1;
        let clock: Box<dyn Clock> = Box::new(FutureClock {
            duration_to_add: Duration::from_secs(5),
        });

        let pending_responses = Arc::new(DashMap::new());
        let error_response_callback = Box::new(TimeoutErrorResponseCallback {
            failed_message_id: Mutex::new(0),
        });

        pending_responses.insert(
            message_id,
            TimedCallback::new(error_response_callback, SystemTime::now()),
        );

        let cleaner = ExpiredPendingResponsesCleaner::new(
            WaitingListOptions::new(Duration::from_secs(2), Duration::from_millis(0)),
            pending_responses.clone(),
            clock,
        );
        thread::sleep(Duration::from_millis(5));
        assert!(pending_responses.is_empty());

        cleaner.stop();
    }
}
