use std::any::Any;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};

use crate::net::message::Message;
use crate::net::wait::{Callback, ResponseError};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ResponseStatus {
    Ok,
    Err,
}

pub(crate) struct ResponseAwaitingCallback {
    handle: ResponseAwaitingCallbackHandle,
}

pub(crate) struct ResponseAwaitingCallbackHandle {
    response: RwLock<Option<Result<Message, ResponseError>>>,
    waker_state: Mutex<Option<Waker>>,
}

impl ResponseAwaitingCallback {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(ResponseAwaitingCallback {
            handle: ResponseAwaitingCallbackHandle::new(),
        })
    }

    pub(crate) fn handle(&self) -> &ResponseAwaitingCallbackHandle {
        &self.handle
    }
}

impl Callback for ResponseAwaitingCallback {
    fn on_response(&self, response: Result<Message, ResponseError>) {
        self.handle.on_response(response);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ResponseAwaitingCallbackHandle {
    fn new() -> Self {
        ResponseAwaitingCallbackHandle {
            response: RwLock::new(None),
            waker_state: Mutex::new(None),
        }
    }

    fn on_response(&self, response: Result<Message, ResponseError>) {
        let mut guard = self.response.write().unwrap();
        *guard = Some(response);

        if let Some(waker) = &self.waker_state.lock().unwrap().deref() {
            waker.wake_by_ref();
        }
    }
}

impl Future for &ResponseAwaitingCallbackHandle {
    type Output = ResponseStatus;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.waker_state.lock().unwrap();
        if let Some(waker) = guard.as_ref() {
            if !waker.will_wake(ctx.waker()) {
                *guard = Some(ctx.waker().clone());
            }
        } else {
            *guard = Some(ctx.waker().clone());
        }

        let read_guard = self.response.read().unwrap();
        return match read_guard.deref() {
            None => Poll::Pending,
            Some(result) => {
                if result.is_ok() {
                    Poll::Ready(ResponseStatus::Ok)
                } else {
                    Poll::Ready(ResponseStatus::Err)
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::net::callback::{ResponseAwaitingCallback, ResponseStatus};
    use crate::net::message::Message;
    use crate::net::wait::{Callback, ResponseTimeoutError};

    #[tokio::test]
    async fn await_on_callback_with_successful_response() {
        let response_awaiting_callback = ResponseAwaitingCallback::new();
        let response_awaiting_callback_clone = response_awaiting_callback.clone();

        tokio::spawn(async move {
            response_awaiting_callback.on_response(Ok(Message::shutdown_type()));
        });

        let handle = response_awaiting_callback_clone.handle();
        let response_status = handle.await;
        assert_eq!(ResponseStatus::Ok, response_status);
    }

    #[tokio::test]
    async fn await_on_callback_with_error_response() {
        let response_awaiting_callback = ResponseAwaitingCallback::new();
        let response_awaiting_callback_clone = response_awaiting_callback.clone();

        tokio::spawn(async move {
            response_awaiting_callback
                .on_response(Err(Box::new(ResponseTimeoutError { message_id: 10 })));
        });

        let handle = response_awaiting_callback_clone.handle();
        let response_status = handle.await;
        assert_eq!(ResponseStatus::Err, response_status);
    }
}
