use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use dashmap::DashMap;

use crate::net::callback::{Callback, ResponseError};
use crate::net::message::{Message, MessageId};
use crate::time::Clock;

#[derive(Debug)]
pub struct ResponseTimeoutError {
    pub message_id: MessageId,
}

impl Display for ResponseTimeoutError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "response timeout for {}", self.message_id)
    }
}

impl Error for ResponseTimeoutError {}

pub(crate) struct TimedCallback {
    callback: Arc<dyn Callback>,
    creation_time: SystemTime,
}

impl TimedCallback {
    fn new(callback: Arc<dyn Callback>, creation_time: SystemTime) -> Self {
        TimedCallback {
            callback,
            creation_time,
        }
    }

    fn on_response(&self, response: Result<Message, ResponseError>) {
        self.callback.on_response(response);
    }

    fn on_timeout_response(&self, message_id: &MessageId) {
        self.callback
            .on_response(Err(Box::new(ResponseTimeoutError {
                message_id: *message_id,
            })));
    }

    fn has_expired(&self, clock: &Box<dyn Clock>, expiry_after: &Duration) -> bool {
        clock.duration_since(self.creation_time).gt(expiry_after)
    }

    #[cfg(test)]
    fn get_callback(&self) -> &Arc<dyn Callback> {
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
    pending_responses: Arc<DashMap<MessageId, TimedCallback>>,
    expired_pending_responses_cleaner: Arc<ExpiredPendingResponsesCleaner>,
    clock: Box<dyn Clock>,
}

impl WaitingList {
    pub(crate) fn new(
        waiting_list_options: WaitingListOptions,
        clock: Box<dyn Clock>,
    ) -> Arc<Self> {
        let pending_responses = Arc::new(DashMap::new());
        let cleaner = ExpiredPendingResponsesCleaner::new(
            waiting_list_options,
            pending_responses.clone(),
            clock.clone(),
        );

        let waiting_list = WaitingList {
            pending_responses,
            expired_pending_responses_cleaner: cleaner,
            clock,
        };
        Arc::new(waiting_list)
    }

    pub(crate) fn add(&self, message_id: MessageId, callback: Arc<dyn Callback>) {
        self.pending_responses
            .insert(message_id, TimedCallback::new(callback, self.clock.now()));
    }

    pub(crate) fn contains(&self, message_id: &MessageId) -> bool {
        self.pending_responses.contains_key(message_id)
    }

    pub(crate) fn handle_response(
        &self,
        message_id: MessageId,
        response: Result<Message, ResponseError>,
    ) {
        let key_value_existence = self.pending_responses.remove(&message_id);
        if let Some(callback_by_key) = key_value_existence {
            let callback = callback_by_key.1;
            callback.on_response(response);
        }
    }

    pub(crate) fn stop(&self) {
        self.expired_pending_responses_cleaner.stop();
    }
}

struct ExpiredPendingResponsesCleaner {
    pending_responses: Arc<DashMap<MessageId, TimedCallback>>,
    clock: Box<dyn Clock>,
    expiry_after: Duration,
    should_stop: AtomicBool,
}

impl ExpiredPendingResponsesCleaner {
    fn new(
        waiting_list_options: WaitingListOptions,
        pending_responses: Arc<DashMap<MessageId, TimedCallback>>,
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
    use std::thread;
    use std::time::Duration;

    use crate::net::message::{Message, MessageId};
    use crate::net::wait::waiting_list_tests::setup::{TestCallback, TestError};
    use crate::net::wait::{WaitingList, WaitingListOptions};
    use crate::time::SystemClock;

    mod setup {
        use std::any::Any;
        use std::error::Error;
        use std::fmt::{Display, Formatter};
        use std::sync::{Arc, Mutex};

        use crate::net::callback::{Callback, ResponseError};
        use crate::net::message::Message;

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
            pub(crate) fn new() -> Arc<TestCallback> {
                Arc::new(TestCallback {
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
        let waiting_list = WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        );
        let callback = TestCallback::new();

        let message_id: MessageId = 10;
        waiting_list.add(message_id, callback.clone());
        waiting_list.handle_response(message_id, Ok(Message::shutdown_type()));

        let message = callback.get_message_at(0).unwrap();
        assert!(message.is_shutdown_type());

        waiting_list.stop();
    }

    #[test]
    fn add_failure_callback_to_waiting_list() {
        let waiting_list = WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        );
        let callback = TestCallback::new();

        let message_id: MessageId = 10;
        waiting_list.add(message_id, callback.clone());
        waiting_list.handle_response(
            message_id,
            Err(Box::new(TestError {
                msg: "test error".to_string(),
            })),
        );

        let error = callback.get_error_at(0).unwrap();
        assert_eq!("test error", error.msg);

        waiting_list.stop();
    }

    #[test]
    fn handle_response_for_unknown_message_id() {
        let waiting_list = WaitingList::new(
            WaitingListOptions::new(Duration::from_secs(120), Duration::from_millis(100)),
            SystemClock::new(),
        );
        let callback = TestCallback::new();

        let message_id: MessageId = 10;
        let unknown_message_id: MessageId = 20;

        waiting_list.add(message_id, callback.clone());
        waiting_list.handle_response(unknown_message_id, Ok(Message::shutdown_type()));

        let message = callback.get_message_at(0);
        assert!(message.is_none());

        waiting_list.stop();
    }

    #[test]
    fn expire_a_pending_response() {
        let waiting_list = WaitingList::new(
            WaitingListOptions::new(Duration::from_millis(120), Duration::from_millis(5)),
            SystemClock::new(),
        );
        let callback = TestCallback::new();

        let message_id: MessageId = 10;
        waiting_list.add(message_id, callback);

        thread::sleep(Duration::from_secs(1));

        assert!(waiting_list.pending_responses.is_empty());
        waiting_list.stop();
    }
}

#[cfg(test)]
mod timed_callback_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::net::wait::timed_callback_tests::setup::{FutureClock, NothingCallback};
    use crate::net::wait::TimedCallback;
    use crate::time::{Clock, SystemClock};

    mod setup {
        use std::any::Any;
        use std::ops::Add;
        use std::time::{Duration, SystemTime};

        use crate::net::callback::{Callback, ResponseError};
        use crate::net::message::Message;
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
            TimedCallback::new(Arc::new(NothingCallback), SystemClock::new().now());

        let clock: Box<dyn Clock> = Box::new(FutureClock {
            duration_to_add: Duration::from_secs(5),
        });

        assert!(timed_callback.has_expired(&clock, &Duration::from_secs(2)));
    }

    #[test]
    fn has_not_expired() {
        let timed_callback =
            TimedCallback::new(Arc::new(NothingCallback), SystemClock::new().now());

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

    use crate::net::message::MessageId;
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

        use crate::net::message::{Message, MessageId};
        use crate::net::wait::{Callback, ResponseError, ResponseTimeoutError};
        use crate::time::Clock;

        #[derive(Clone)]
        pub struct FutureClock {
            pub(crate) duration_to_add: Duration,
        }

        pub struct TimeoutErrorResponseCallback {
            pub(crate) failed_message_id: Mutex<MessageId>,
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
        let message_id: MessageId = 1;
        let clock: Box<dyn Clock> = Box::new(FutureClock {
            duration_to_add: Duration::from_secs(5),
        });

        let pending_responses = Arc::new(DashMap::new());
        let error_response_callback = Arc::new(TimeoutErrorResponseCallback {
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
