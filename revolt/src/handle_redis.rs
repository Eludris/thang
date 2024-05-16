use std::sync::Arc;

use futures::StreamExt;
use models::Config;
use models::Event;
use models::EventData;
use models::Result;
use redis::aio::{MultiplexedConnection, PubSub};
use redis::AsyncCommands;
use revolt_wrapper::models::Masquerade;
use revolt_wrapper::HttpClient;
use tokio::sync::Mutex;

pub async fn handle_redis(
    mut pubsub: PubSub,
    conn: MultiplexedConnection,
    rest: Arc<HttpClient>,
    config: Config,
) -> Result<()> {
    let conn = Arc::new(Mutex::new(conn));

    for channel in config {
        if channel.revolt.is_some() {
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
        let channel_ids = conn
            .smembers::<_, Option<Vec<String>>>(format!("revolt:channels:{}", channel_name))
            .await?;

        let channel_ids: Vec<String> = if let Some(channel_ids) = channel_ids {
            if payload.platform == "revolt" {
                let current_id: String = payload.identifier;
                channel_ids
                    .into_iter()
                    .filter(|id| id != &current_id)
                    .collect()
            } else {
                channel_ids
            }
        } else {
            log::warn!("No channel id found for channel {}", channel_name);
            continue;
        };

        match payload.data {
            EventData::MessageCreate(msg) => {
                let mut content = msg.content.clone();

                // if !msg.replies.is_empty() {
                //     let referenced = &msg.replies[0];
                //     let mut reply = referenced
                //         .content
                //         .lines()
                //         .map(|l| format!("> {}", l))
                //         .collect::<Vec<String>>()
                //         .join("\n");
                //     let mut name = referenced.author.clone();
                //     if name.len() > 32 {
                //         name = name.drain(..32).collect();
                //     }
                //     reply.push_str(&format!("\n@{}", name));
                //     content = format!("\n{}\n{}", reply, content);
                // }

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

                for channel in channel_ids {
                    rest.send_message(&channel)
                        .content(content.clone())
                        .masquerade(Masquerade {
                            name: Some(msg.author.clone()),
                            avatar: msg.avatar.clone(),
                        })
                        .send()
                        .await?;
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
