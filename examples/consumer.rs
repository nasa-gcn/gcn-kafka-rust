use gcn_kafka::GcnClientConfig;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use std::env::var;
use std::time::Duration;

#[tokio::main]
pub async fn main() {
    let mut config = ClientConfig::new();
    config.set_gcn_auth(
        &var("GCN_KAFKA_CLIENT_ID").expect("env var GCN_KAFKA_CLIENT_ID is not defined"),
        &var("GCN_KAFKA_CLIENT_SECRET").expect("env var GCN_KAFKA_CLIENT_SECRET is not defined"),
        var("GCN_KAFKA_DOMAIN").ok().as_deref(),
    );

    let consumer: StreamConsumer = config.create().unwrap();

    println!("Topics:");
    consumer
        .fetch_metadata(None, Duration::from_secs(3))
        .unwrap()
        .topics()
        .iter()
        .for_each(|topic| println!("  {}", topic.name()));

    consumer.subscribe(&["gcn.heartbeat"]).unwrap();

    loop {
        match consumer.recv().await {
            Err(err) => println!("Receive message failed: {}", err),
            Ok(msg) => {
                if let Some(result) = msg.payload_view::<str>() {
                    match result {
                        Err(err) => println!("Decode message failed: {}", err),
                        Ok(value) => println!("{}", value),
                    }
                }
            }
        }
    }
}
