use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct SystemClock;

pub trait BoxedClockClone {
    fn clone_box(&self) -> Box<dyn Clock>;
}

pub trait Clock: Send + Sync + BoxedClockClone {
    fn now(&self) -> SystemTime;

    fn now_seconds(&self) -> u64 {
        self.now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    fn duration_since(&self, time: SystemTime) -> Duration {
        self.now().duration_since(time).unwrap()
    }
}

impl<T> BoxedClockClone for T
where
    T: 'static + Clock + Clone,
{
    fn clone_box(&self) -> Box<dyn Clock> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Clock> {
    fn clone(&self) -> Box<dyn Clock> {
        self.clone_box()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl SystemClock {
    pub fn new() -> Box<SystemClock> {
        Box::new(SystemClock)
    }
}
