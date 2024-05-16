mod handle_events;
mod handle_redis;

use std::{env, sync::Arc};

use models::{Config, Result};
use redis::AsyncCommands;
use revolt_wrapper::{GatewayClient, HttpClient};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let token = env::var("REVOLT_TOKEN").expect("REVOLT_TOKEN must be set");
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());

    let config: Config = serde_yaml::from_str(&std::fs::read_to_string("./config.yml")?)?;

    let client = GatewayClient::new(token.clone());
    let http = Arc::new(HttpClient::new(token));

    let redis = redis::Client::open(redis_url)?;
    log::info!("Connected to Redis {}", redis.get_connection_info().addr);

    let mut conn = redis.get_multiplexed_async_connection().await?;
    for channel in &config {
        if let Some(revolt) = &channel.revolt {
            for id in revolt {
                conn.set(format!("revolt:key:{}", id), &channel.name)
                    .await?;
            }

            conn.sadd(format!("revolt:channels:{}", channel.name), revolt)
                .await?;
        }
    }

    let mut events = client.get_events().await?;
    log::info!("Connected to gateway!");

    let bot_id = http.fetch_self().await?.id;

    let err = tokio::select! {
        e = handle_redis::handle_redis(redis.get_async_pubsub().await?, redis.get_multiplexed_async_connection().await?, http.clone(), config) => {
            log::error!("Events failed first {:?}", e);
            e
        },
        e = handle_events::handle_events(&mut events, redis.get_multiplexed_async_connection().await?, bot_id, http) => {
            log::error!("Websocket failed first {:?}", e);
            e
        },
    };

    err
}
