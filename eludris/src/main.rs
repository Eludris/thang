mod handle_events;
mod handle_redis;

use eludrs::HttpClient;
use futures::future::{abortable, select_all};
use futures_util::FutureExt;
use models::{Config, Result};
use redis::AsyncCommands;
use std::env;

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const URL: &str = "https://api.eludris.gay";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string("./config.yml")?)?;
    let token: String = env::var("ELUDRIS_TOKEN").expect("ELUDRIS_TOKEN must be set");

    let redis = redis::Client::open(redis_url)?;
    log::info!("Connected to Redis {}", redis.get_connection_info().addr);

    let mut conn = redis.get_multiplexed_async_connection().await?;
    for channel in &config {
        if let Some(eludris) = &channel.eludris {
            conn.set(format!("eludris:key:{}", eludris), &channel.name)
                .await?;
            conn.sadd(format!("eludris:instances:{}", channel.name), eludris)
                .await?;
        }
    }

    let mut client = HttpClient::new(&token).rest_url(URL.to_string());
    let gateway = client.create_gateway().await?;
    let effis_url = client.get_instance_info().await?.effis_url.clone();
    let self_id = client.get_user().await?.id;

    let mut futures = Vec::new();

    futures.push(
        handle_events::handle_events(
            redis.get_multiplexed_async_connection().await?,
            gateway,
            config[0].eludris.as_ref().unwrap().to_string(),
            self_id,
            effis_url,
        )
        .boxed(),
    );
    futures
        .push(handle_redis::handle_redis(redis.get_async_pubsub().await?, client, config).boxed());

    let (output, index, futures) = select_all(futures).await;

    if index == 0 {
        log::error!("Redis failed first: {:?}", output);
    } else {
        log::error!("Events failed first: {:?}", output);
    }

    for future in futures {
        let (res, abort_handle) = abortable(future.map(Result::Ok));
        tokio::spawn(async move {
            let _ = res.await;
        });
        abort_handle.abort();
    }

    output
}
