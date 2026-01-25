use gcn_kafka::*;
use rdkafka::Message;
use rdkafka::{
    ClientConfig,
    consumer::{Consumer, StreamConsumer},
};
use std::env::var;

#[tokio::main]
pub async fn main() {
    let mut config = ClientConfig::new();
    config.set_gcn_auth(
        &var("GCN_KAFKA_CLIENT_ID").expect("env var GCN_KAFKA_CLIENT_ID is not defined"),
        &var("GCN_KAFKA_CLIENT_SECRET").expect("env var GCN_KAFKA_CLIENT_SECRET is not defined"),
        var("GCN_KAFKA_DOMAIN").ok().as_deref(),
    );

    let consumer: StreamConsumer = config.create().unwrap();
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
