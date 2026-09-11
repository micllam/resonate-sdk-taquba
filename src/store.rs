//! The store: the queue that contains the protocol's state, the lock that
//! serialises requests against it, the claims it keeps and the worker
//! that fires timers.

use std::sync::Arc;
use std::time::Duration;

use taquba::worker::{Worker, WorkerError};
use taquba::{JobRecord, LeaseHandle, OpenOptions, Queue, QueueConfig, WorkerHandle, run_worker};
use tokio::sync::Mutex;
use tracing::warn;

use crate::error::Result;
use crate::keys::TIMER_QUEUE;
use crate::network::TaqubaNetworkBuilder;
use crate::timers::{Timer, fire};
use crate::txn::{Held, Txn};

/// Parameters of a [`TaqubaStore`].
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Time a delivered execute message waits for its acquire before it
    /// is sent again. Defaults to 30 seconds.
    pub retry_timeout: Duration,
    /// Maximum number of sibling promises returned with a task.
    /// Defaults to 10.
    pub preload_limit: usize,
    /// Upper bound on the delay between a timer becoming due and its
    /// delivery. Defaults to 250 milliseconds.
    pub poll_interval: Duration,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            retry_timeout: Duration::from_secs(30),
            preload_limit: 10,
            poll_interval: Duration::from_millis(250),
        }
    }
}

impl StoreOptions {
    /// Set [`Self::retry_timeout`].
    #[must_use]
    pub fn retry_timeout(mut self, retry_timeout: Duration) -> Self {
        self.retry_timeout = retry_timeout;
        self
    }

    /// Set [`Self::preload_limit`].
    #[must_use]
    pub fn preload_limit(mut self, preload_limit: usize) -> Self {
        self.preload_limit = preload_limit;
        self
    }

    /// Set [`Self::poll_interval`].
    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// The queue options [`TaqubaStore::open`] uses: a default queue
    /// configuration with unbounded attempts, no retry backoff and no
    /// retained done jobs, and the timer queue with the default backoff.
    pub fn queue_options() -> OpenOptions {
        OpenOptions::default()
            .default_queue_config(
                QueueConfig::default()
                    .max_attempts(u32::MAX)
                    .retry_backoff_base(Duration::ZERO)
                    .retry_backoff_max(Duration::ZERO)
                    .keep_done_jobs(None),
            )
            .queue_config(TIMER_QUEUE, QueueConfig::default().max_attempts(u32::MAX))
    }
}

/// State shared by the store and its networks.
pub(crate) struct Shared {
    pub queue: Arc<Queue>,
    /// Serialises every request, delivery and timer against the records,
    /// and contains the held claims, which are touched only under it.
    pub lock: Mutex<Held>,
    pub preload_limit: usize,
    pub poll_interval: Duration,
    pub retry_timeout: Duration,
}

impl Shared {
    /// The current time in epoch milliseconds, from the queue's clock.
    pub fn now(&self) -> i64 {
        i64::try_from(self.queue.clock().now_ms()).unwrap_or(i64::MAX)
    }

    /// A request's transaction over the held claims of the lock.
    pub fn txn<'a>(&'a self, held: &'a mut Held) -> Txn<'a> {
        Txn::new(&self.queue, held, self.preload_limit, self.now())
    }
}

/// The protocol's state in one queue. A store opens the queue, runs the
/// timers and creates one [`TaqubaNetwork`](crate::TaqubaNetwork) per SDK
/// instance.
pub struct TaqubaStore {
    shared: Arc<Shared>,
    timers: WorkerHandle,
}

impl TaqubaStore {
    /// Open the store at `path` in `object_store` with default options.
    pub async fn open(
        object_store: Arc<dyn taquba::object_store::ObjectStore>,
        path: &str,
    ) -> Result<Self> {
        let options = StoreOptions::default();
        Self::open_with_options(object_store, path, StoreOptions::queue_options(), options).await
    }

    /// Open the store at `path` in `object_store` with explicit queue
    /// and store options. The queue's clock is the store's clock, and
    /// the queue options are those of [`StoreOptions::queue_options`]
    /// with the caller's changes: a queue must not retain done jobs and
    /// must not bound attempts.
    pub async fn open_with_options(
        object_store: Arc<dyn taquba::object_store::ObjectStore>,
        path: &str,
        queue_options: OpenOptions,
        options: StoreOptions,
    ) -> Result<Self> {
        let queue = Queue::open_with_options(object_store, path, queue_options).await?;
        Ok(Self::from_queue(Arc::new(queue), options))
    }

    /// Run the store on a queue the application opened. The address
    /// queues and the timer queue are named with an address or a
    /// `resonate/` prefix and take the queue's default configuration,
    /// which must not retain done jobs and must not bound attempts.
    pub fn from_queue(queue: Arc<Queue>, options: StoreOptions) -> Self {
        let shared = Arc::new(Shared {
            queue,
            lock: Mutex::new(Held::default()),
            preload_limit: options.preload_limit,
            poll_interval: options.poll_interval,
            retry_timeout: options.retry_timeout,
        });
        let worker = TimerWorker {
            shared: shared.clone(),
        };
        let poll_interval = options.poll_interval;
        let queue = shared.queue.clone();
        let timers = WorkerHandle::spawn(std::future::pending::<()>(), move |stop| async move {
            run_worker(
                &queue,
                TIMER_QUEUE,
                &worker,
                poll_interval,
                stop.cancelled_owned(),
            )
            .await
        });
        Self { shared, timers }
    }

    /// The queue the store runs on.
    pub fn queue(&self) -> &Arc<Queue> {
        &self.shared.queue
    }

    /// A builder for a network of this store. The worker id and the
    /// routing group are set on the builder.
    pub fn network(&self) -> TaqubaNetworkBuilder {
        TaqubaNetworkBuilder::new(self.shared.clone())
    }

    /// Stop the timers and close the queue. The queue closes only when
    /// no network created by this store is alive, and stays open
    /// otherwise.
    pub async fn close(self) -> Result<()> {
        if let Err(e) = self.timers.shutdown().await {
            warn!("timer worker ended with an error: {e}");
        }
        let Self { shared, .. } = self;
        match Arc::try_unwrap(shared) {
            Ok(shared) => match Arc::try_unwrap(shared.queue) {
                Ok(queue) => queue.close().await?,
                Err(_) => warn!("the queue is shared and stays open"),
            },
            Err(_) => warn!("a network of this store is alive and the queue stays open"),
        }
        Ok(())
    }
}

/// Fires the timer queue's jobs.
struct TimerWorker {
    shared: Arc<Shared>,
}

impl Worker for TimerWorker {
    async fn process(
        &self,
        job: &JobRecord,
        _lease: &LeaseHandle,
    ) -> std::result::Result<(), WorkerError> {
        let timer: Timer = serde_json::from_slice(&job.payload)?;
        let mut held = self.shared.lock.lock().await;
        fire(self.shared.txn(&mut held), timer).await?;
        Ok(())
    }
}
