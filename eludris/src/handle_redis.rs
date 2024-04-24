use std::collections::HashMap;
use std::sync::Arc;

use eludrs::HttpClient;
use futures::StreamExt;
use models::Config;
use models::Event;
use models::EventData;
use models::Result;
use redis::aio::MultiplexedConnection;
use redis::aio::PubSub;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize)]
struct RatelimitResponse {
    data: RatelimitData,
}

#[derive(Debug, Serialize, Deserialize)]
struct RatelimitData {
    retry_after: u64,
}

pub async fn handle_redis(
    pubsub: PubSub,
    conn: MultiplexedConnection,
    clients: HashMap<String, HttpClient>,
    config: Config,
) -> Result<()> {
    let conn = Arc::new(Mutex::new(conn));

    for channel in config {
        if channel.eludris.is_some() {
            pubsub.subscribe(channel.name).await?;
        }
    }
    let mut pubsub = pubsub.into_on_message();

    while let Some(payload) = pubsub.next().await {
        let channel_name = payload.get_channel_name();
        // TODO: handle more of the errors here
        let payload: String = match payload.get_payload() {
            Ok(payload) => payload,
            Err(err) => {
                log::error!("Could not get pubsub payload: {}", err);
                continue;
            }
        };
        let payload: Event = match serde_json::from_str(&payload) {
            Ok(payload) => payload,
            Err(err) => {
                log::error!("Failed to deserialize event payload: {}", err);
                continue;
            }
        };

        let mut conn = conn.lock().await;
        let urls = conn
            .smembers::<_, Option<Vec<String>>>(format!("eludris:instances:{}", channel_name))
            .await?;
        let required_clients = if let Some(urls) = urls {
            let mut required_clients = Vec::new();
            for url in urls {
                if payload.platform == "eludris" && url == payload.identifier {
                    continue;
                }
                required_clients.push(clients.get(&url).unwrap());
            }
            required_clients
        } else {
            log::warn!("No instance URL found for channel {}", channel_name);
            continue;
        };

        match payload.data {
            EventData::MessageCreate(msg) => {
                let mut name = format!("Bridge-{}", &msg.author);
                if name.len() > 32 {
                    name = name.drain(..32).collect();
                }

                let mut content = msg.content.clone();

                if !msg.replies.is_empty() {
                    let referenced = &msg.replies[0];
                    let mut reply = referenced
                        .content
                        .lines()
                        .map(|l| format!("> {}", l))
                        .collect::<Vec<String>>()
                        .join("\n");
                    let mut name = referenced.author.clone();
                    if name.len() > 32 {
                        name = name.drain(..32).collect();
                    }
                    reply.push_str(&format!("\n@{}", name));
                    content = format!("\n{}\n{}", reply, content);
                }

                let attachments = msg
                    .attachments
                    .iter()
                    .map(|a| a.as_ref())
                    .collect::<Vec<&str>>()
                    .join("\n");

                if !attachments.is_empty() {
                    if !content.is_empty() {
                        content.push('\n');
                    }
                    content.push_str(&attachments);
                }

                // Since attachments cause a message to be empty.
                // This should be fine for now, but shouldn't happen in the future.
                if content.is_empty() {
                    continue;
                }

                log::debug!("Sending message to {} clients", required_clients.len());
                for client in required_clients {
                    client.send_message(&name, &content).await?;
                }
            }
            // Seems unreachable now but is a catchall for future events.
            #[allow(unreachable_patterns)]
            payload => {
                log::warn!("Unhandled payload from pubsub: {:?}", payload)
            }
        }
    }

    Ok(())
}
