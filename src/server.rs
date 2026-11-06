use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::store::Store;

/// State shared by every connection.
#[derive(Default)]
pub struct ServerState {
    store: Mutex<Store>,
    /// Signalled after every write command, so blocking commands re-check.
    changed: Condvar,
}

impl ServerState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap()
    }

    pub fn notify_changed(&self) {
        self.changed.notify_all();
    }

    /// Runs `check` until it returns `Some`, waiting for writes in between.
    /// `timeout`: `None` waits forever, `Some(Duration::ZERO)` checks once.
    pub fn wait_for<T>(
        &self,
        timeout: Option<Duration>,
        mut check: impl FnMut(&mut Store) -> Option<T>,
    ) -> Option<T> {
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut store = self.store();
        loop {
            if let Some(found) = check(&mut store) {
                return Some(found);
            }
            store = match deadline {
                None => self.changed.wait(store).unwrap(),
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return None;
                    }
                    self.changed.wait_timeout(store, deadline - now).unwrap().0
                }
            };
        }
    }
}
