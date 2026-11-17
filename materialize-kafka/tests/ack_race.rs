//! Stress reproduction of a lost delivery-ack wakeup in `AckTrackingContext`.
//!
//! Each worker repeats `increment_pending`, `send`, then
//! `wait_for_all_successful_acks` against an in-process librdkafka mock
//! broker. The test fails when a worker makes no progress for the stall
//! window.
//!
//! Run with:
//!   cargo test --release --test ack_race -- --ignored --nocapture

use materialize_kafka::{binding_info::BindingInfo, configuration::AckTrackingContext};
use rdkafka::{
    producer::{BaseRecord, Producer, ThreadedProducer},
    ClientConfig,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Worker {
    producer: Arc<ThreadedProducer<AckTrackingContext>>,
    transactions: Arc<AtomicU64>,
    handle: JoinHandle<()>,
}

fn spawn_worker(id: u64, linger_ms: Option<u64>) -> Worker {
    let mut config = ClientConfig::new();
    config.set("test.mock.num.brokers", "1");
    config.set("compression.type", "lz4");
    config.set("enable.idempotence", "true");
    if let Some(ms) = linger_ms {
        config.set("linger.ms", ms.to_string());
    }

    let producer: Arc<ThreadedProducer<AckTrackingContext>> =
        Arc::new(config.create_with_context(AckTrackingContext::new()).unwrap());
    let transactions = Arc::new(AtomicU64::new(0));

    let worker_producer = producer.clone();
    let worker_transactions = transactions.clone();

    let handle = std::thread::spawn(move || {
        let (producer, transactions) = (worker_producer, worker_transactions);
        let context = producer.context();
        let topic = format!("ack-race-{id}");
        let bindings = [BindingInfo {
            topic: topic.clone(),
            schema: None,
            key_ptr: Vec::new(),
            field_names: Vec::new(),
        }];
        let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (id + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        // Smoothed time from `send` to the delivery ack, in nanoseconds.
        let mut ack_nanos = 0f64;

        // The first ack waits on the producer's startup, so it is kept out of
        // the latency estimate.
        context.increment_pending();
        producer
            .send(BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, 0).key(b"k").payload(b"v"))
            .map_err(|(err, _)| err)
            .unwrap();
        context.wait_for_all_successful_acks(&bindings).unwrap();

        for n in 0u64.. {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;

            context.increment_pending();
            let sent_at = Instant::now();
            producer
                .send(BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, 0).key(b"k").payload(b"v"))
                .map_err(|(err, _)| err)
                .unwrap();

            // Sweep the delay before the wait across the measured ack latency.
            // Every eighth transaction waits immediately to keep the latency
            // estimate current.
            let measure = n % 8 == 0;
            if !measure {
                let delay = Duration::from_nanos((rng % (1 + (ack_nanos * 1.2) as u64)).max(1));
                while sent_at.elapsed() < delay {
                    std::hint::spin_loop();
                }
            }

            context.wait_for_all_successful_acks(&bindings).unwrap();

            if measure {
                let sample = sent_at.elapsed().as_nanos() as f64;
                ack_nanos = if ack_nanos == 0.0 { sample } else { ack_nanos * 0.9 + sample * 0.1 };
            }
            transactions.fetch_add(1, Ordering::Relaxed);
        }
    });

    Worker {
        producer,
        transactions,
        handle,
    }
}

#[test]
#[ignore = "stress test, runs until a hang or ACK_RACE_BUDGET_SECS"]
fn start_commit_wait_loses_wakeup() {
    let worker_count = env_u64("ACK_RACE_WORKERS", 16);
    let budget = Duration::from_secs(env_u64("ACK_RACE_BUDGET_SECS", 600));
    let stall = Duration::from_secs(env_u64("ACK_RACE_STALL_SECS", 15));
    let linger_ms = std::env::var("ACK_RACE_LINGER_MS").ok().map(|v| v.parse().unwrap());

    let mut workers: Vec<Worker> = (0..worker_count).map(|id| spawn_worker(id, linger_ms)).collect();

    let started = Instant::now();
    let mut last: Vec<(u64, Instant)> = workers.iter().map(|_| (0, Instant::now())).collect();

    while started.elapsed() < budget {
        std::thread::sleep(Duration::from_secs(1));

        let total: u64 = workers.iter().map(|w| w.transactions.load(Ordering::Relaxed)).sum();
        if started.elapsed().as_secs() % 10 == 0 {
            eprintln!("{:>4}s: {total} transactions committed", started.elapsed().as_secs());
        }

        if let Some(id) = workers.iter().position(|w| w.handle.is_finished()) {
            let worker = workers.swap_remove(id);
            let panic = worker.handle.join().unwrap_err();
            eprintln!("worker {id} stopped with a panic, which is not a lost wakeup");
            std::panic::resume_unwind(panic);
        }

        for (id, worker) in workers.iter().enumerate() {
            let count = worker.transactions.load(Ordering::Relaxed);
            if count != last[id].0 {
                last[id] = (count, Instant::now());
                continue;
            }
            if last[id].1.elapsed() >= stall {
                let in_flight = worker.producer.in_flight_count();
                panic!(
                    "worker {id} has made no progress for {:?} \
                     after {count} transactions, with {in_flight} messages in flight \
                     ({total} transactions across {worker_count} workers in {:?})",
                    last[id].1.elapsed(),
                    started.elapsed(),
                );
            }
        }
    }

    let total: u64 = workers.iter().map(|w| w.transactions.load(Ordering::Relaxed)).sum();
    eprintln!("no hang in {budget:?} over {total} transactions");
}
