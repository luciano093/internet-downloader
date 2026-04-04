use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::watch;
use tokio::time::{Instant, Sleep};

use pin_project_lite::pin_project;
use tokio_stream::wrappers::WatchStream;

#[derive(Debug)]
pub struct DebtClock {
    base_time: Instant,
    clock: AtomicU64,
}

impl DebtClock {
    pub fn new() -> Self {
        Self {
            base_time: Instant::now(),
            clock: AtomicU64::new(0),
        }
    }

    fn clock(&self) -> u64 {
        self.clock.load(Ordering::Acquire)
    }

    pub fn add_debt(&self, debt: Duration) {
        let micros = u64::try_from(debt.as_micros()).unwrap_or(u64::MAX);

        self.clock.fetch_update(Ordering::AcqRel, Ordering::Acquire, |clock| {
            let current_time_micros = self.get_base_time_micros();

            // If current clock is in the past, set it to the present.
            // Otherwise, keep the future deadline.
            let base = current_time_micros.max(clock);

            let new_clock = base.saturating_add(micros);
            Some(new_clock)
        }).unwrap();
    }

    pub fn get_debt(&self) -> Option<Duration> {
        // current clock has to be before current time micros
        // otherwise if the thread stops between them, it might cause a desync and give debt
        // but with this order, the current clock will never be newer than current time on a desync
        let current_clock = self.clock();
        let current_time_micros = self.get_base_time_micros();

        if current_clock > current_time_micros {
            let remaining_micros = current_clock - current_time_micros;
            Some(Duration::from_micros(remaining_micros))
        } else {
            None
        }
    }

    fn get_base_time_micros(&self) -> u64 {
        // might overflow if program runs without restart for half a million years
        u64::try_from(self.base_time.elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    pub fn clear_debt(&self) {
        // Setting the deadline to 0 forces it into the past, instantly expiring it.
        self.clock.store(0, Ordering::Release);
    }
}

impl Default for DebtClock {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BandwidthLimiter {
    bytes_per_second: AtomicU64,
    leftover_bytes: AtomicU64, 
    clock: DebtClock,
    unlimited: AtomicBool,
    notifier: watch::Sender<()>, 
}

impl BandwidthLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: AtomicU64::new(bytes_per_second),
            leftover_bytes: AtomicU64::new(0),
            clock: DebtClock::new(),
            unlimited: AtomicBool::new(false),
            notifier: watch::channel(()).0,
        }
    }

    pub fn set_limit(&self, bytes_per_second: u64) {
        self.bytes_per_second.store(bytes_per_second, Ordering::Release);

        self.leftover_bytes.store(0, Ordering::Release);
        self.clock.clear_debt();

        self.wake_all();
    }

    pub fn get_limit(&self) -> u64 {
        self.bytes_per_second.load(Ordering::Acquire)
    }

    pub fn set_unlimited(&self, is_unlimited: bool) {
        self.unlimited.store(is_unlimited, Ordering::Release);

        if is_unlimited {
            self.leftover_bytes.store(0, Ordering::Release);
            self.clock.clear_debt(); 
        }

        self.wake_all();
    }

    pub fn is_unlimited(&self) -> bool {
        self.unlimited.load(Ordering::Acquire)
    }

    pub fn register_bytes(&self, bytes: u64) {
        let limit = self.get_limit();

        // If the speed is unlimited, we don't need to register any debt
        // Likewise if the speed limit is 0 then we want also don't need to register debt 
        if self.is_unlimited() || limit == 0 {
            return;
        }

        let mut current_leftovers = self.leftover_bytes.load(Ordering::Acquire);

        let micros_cost = loop {
            let total_unpaid = (current_leftovers as u128) + (bytes as u128);
            // Calculate how many microseconds this total is worth
            // we use u128 first to prevent overflows when mulitplying
            let cost = (total_unpaid * 1_000_000) / limit as u128;

            let next_leftovers = if cost > 0 {
                // Calculate exactly how many bytes we just "paid" for
                // u128 to prevent u64 overflows on multiplication
                let paid_bytes = (cost * (limit as u128)) / 1_000_000;
                (total_unpaid - paid_bytes) as u64
            } else {
                total_unpaid as u64
            };

            match self.leftover_bytes.compare_exchange_weak(current_leftovers, next_leftovers, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break cost as u64,
                Err(actual) => current_leftovers = actual,
            }
        };
        
        if micros_cost > 0 {
            self.clock.add_debt(Duration::from_micros(micros_cost));
        }
    }

    pub fn get_debt(&self) -> Option<Duration> {
        let limit = self.get_limit();
        
        // If we are in unlimited mode, we never have debt
        if self.is_unlimited() {
            return None;
        }

        // If limit is 0, we want to sleep forever
        if limit == 0 {
            // We don't return Duration::MAX as some sleep functions don't support this
            // tokio's sleep for example would crash the thread
            return Some(Duration::from_secs(60 * 60 * 24 * 365 * 10 * 100)); // 1000 years
        }
        
        self.clock.get_debt()
    }

    pub fn wake_all(&self) {
        let _ = self.notifier.send(()); 
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.notifier.subscribe()
    }
}

// We use pin_project! here to allow our Pin<&self> to become Pin<&S> (needed to satisfy AsyncRead)
// otherwise there is no way to pin S without unsafe code
pin_project! {
    pub struct ThrottledStream<S> {
        #[pin]
        inner: S,
        limiters: Vec<Arc<BandwidthLimiter>>,
        #[pin]
        sleep: Sleep, 
        receivers: Vec<WatchStream<()>>
    }
}

impl<S> ThrottledStream<S> {
    pub fn new(inner: S, limiters: Vec<Arc<BandwidthLimiter>>) -> Self {
        // We get one receiver per limiter, so they can alert us
        // if there has been a change in config (helps prevent eternal sleeps)
        let receivers = limiters
            .iter()
            .map(|limiter| WatchStream::from_changes(limiter.subscribe()))
            .collect();

        Self {
            inner,
            limiters,
            sleep: tokio::time::sleep(Duration::ZERO),
            receivers,
        }
    }
}

impl<S: AsyncRead> AsyncRead for ThrottledStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        let mut settings_changed = false;

        for receiver in this.receivers.iter_mut() {
            while let Poll::Ready(Some(())) = receiver.poll_next_unpin(cx) {
                settings_changed = true;
            }
        }

        // If the settings of any of our limiters changed, it might mean we exited a 0 bytes/sec state (eternal sleep)
        // We check this before we poll sleep as otherwise it might incorrectly send back a Poll::Pending
        // even if we are not supposed to be sleeping anymore
        if settings_changed {
            let mut debt = Duration::ZERO;

            for limiter in this.limiters.iter() {
                if let Some(local_debt) = limiter.get_debt() {
                    debt = debt.max(local_debt);
                }
            }

            if debt.is_zero() {
                // we have no debt, meaning we should not be sleeping
                this.sleep.as_mut().reset(tokio::time::Instant::now());
            } else {
                // we still have debt, let's add it to our sleep
                // this might mean that either we are still in eternal sleep and 
                // what changed was a limiter not in eternal sleep, or that our
                // new debt is not eternal and we should wake up soon
                this.sleep.as_mut().reset(tokio::time::Instant::now() + debt);
            }
        }

        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                let bytes_before = buf.filled().len();

                match this.inner.as_mut().poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - bytes_before;

                        // We didn't read anything, nothing to do
                        if bytes_read == 0 {
                            return Poll::Ready(Ok(()));
                        }

                        let mut debt = Duration::ZERO;

                        // Let each limiter how many bytes we read so they can calculate their current debt
                        // We get the greatest debt as by then all others' debt will have finished
                        for limiter in this.limiters.iter() {
                            limiter.register_bytes(bytes_read as u64);
                            
                            if let Some(local_debt) = limiter.get_debt() {
                                debt = debt.max(local_debt);
                            }
                        }

                        // No one had debt, so no need to pause
                        if debt.is_zero() {
                            return Poll::Ready(Ok(()));
                        }

                        // Someone had debt, let's store that on sleep so next calls return Pending
                        // until the debt time passes
                        let deadline = tokio::time::Instant::now() + debt;
                        this.sleep.as_mut().reset(deadline);

                        Poll::Ready(Ok(()))
                    }
                    // buffer didn't change, just return
                    other => other,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}