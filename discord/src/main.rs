mod handle_events;
mod handle_redis;

use models::{Config, Result};
use redis::AsyncCommands;
use std::env;
use twilight_gateway::{CloseFrame, Intents, Shard, ShardId};
use twilight_http::Client;
use twilight_model::channel::message::AllowedMentions;

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    let token = env::var("DISCORD_TOKEN")
        .expect("Could not find the \"DISCORD_TOKEN\" environment variable");
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());

    let config: Config = serde_yaml::from_str(&std::fs::read_to_string("./config.yml")?)?;

    let redis = redis::Client::open(redis_url)?;
    log::info!("Connected to Redis {}", redis.get_connection_info().addr);

    let mut conn = redis.get_multiplexed_async_connection().await?;
    for channel in &config {
        if let Some(discord) = &channel.discord {
            for id in discord {
                conn.set(format!("discord:key:{}", id), &channel.name)
                    .await?;
            }

            conn.sadd(format!("discord:channels:{}", channel.name), discord)
                .await?;
        }
    }

    // Get Discord ready.
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;

    let mut shard = Shard::new(ShardId::ONE, token.clone(), intents);

    log::info!("Connected to Discord");

    let http = Client::builder()
        .token(token)
        .default_allowed_mentions(AllowedMentions::default())
        .build();

    let current_user = http.current_user().await?.model().await?.id;

    let err = tokio::select!(
        e  = handle_events::handle_events(
            &mut shard,
            redis.get_multiplexed_async_connection().await?,
        ) => {
            log::error!("Events failed first {:?}", e);
            e
        },
        e = handle_redis::handle_redis(
            redis.get_async_pubsub().await?,
            redis.get_multiplexed_async_connection().await?,
            http,
            config,
            current_user,
        ) => {
            log::error!("Receive failed first {:?}", e);
            e
        },
    );

    shard
        .close(CloseFrame {
            code: 1001,
            reason: std::borrow::Cow::Borrowed("Shutting down"),
        })
        .await?;

    err
}
