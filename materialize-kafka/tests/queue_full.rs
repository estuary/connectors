use materialize_kafka::{configuration::AckTrackingContext, transactor::send_when_queue_has_room};
use rdkafka::{
    producer::{BaseRecord, ThreadedProducer},
    ClientConfig,
};
use std::{sync::mpsc, time::Duration};

#[test]
fn send_returns_once_a_full_queue_drains() {
    let mut config = ClientConfig::new();
    config.set("test.mock.num.brokers", "1");
    config.set("enable.idempotence", "true");
    config.set("linger.ms", "0");
    config.set("queue.buffering.max.messages", "1");

    let producer: ThreadedProducer<AckTrackingContext> =
        config.create_with_context(AckTrackingContext::new()).unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (key, payload) = (b"k".to_vec(), b"v".to_vec());
        for _ in 0..200 {
            let msg = BaseRecord::with_opaque_to("queue-full", 0).key(&key).payload(&payload);
            send_when_queue_has_room(&producer, msg).unwrap();
        }
        done_tx.send(()).unwrap();
    });

    done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("a send stayed blocked on a full queue");
}
