use std::sync::Arc;

use eludrs::GatewayClient;
use futures::StreamExt;
use models::Event;
use models::EventData;
use models::Message;
use models::Result;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use tokio::sync::Mutex;

const ELUDRIS_AVATAR: &str =
    "https://raw.githubusercontent.com/eludris/.github/main/assets/das_ding.png";

pub async fn handle_events(
    conn: MultiplexedConnection,
    gateway: GatewayClient,
    url: String,
    self_id: u64,
    effis_url: String,
) -> Result<()> {
    let conn = Arc::new(Mutex::new(conn));
    let mut events = gateway.get_events().await?;

    while let Some(event) = events.next().await {
        let conn = Arc::clone(&conn);
        let url = url.clone();
        let effis_url = effis_url.clone();
        tokio::spawn(async move {
            if let eludrs::models::Event::Message(msg) = event {
                if msg.author.id != self_id {
                    let avatar_url = match &msg.author.avatar {
                        Some(avatar) => format!("{effis_url}/avatars/{avatar}"),
                        None => ELUDRIS_AVATAR.to_string(),
                    };
                    let event = Event {
                        platform: "eludris",
                        identifier: url.clone(),
                        data: EventData::MessageCreate(Message {
                            content: msg.message.content,
                            author: msg.author.display_name.unwrap_or(msg.author.username),
                            attachments: Vec::new(),
                            replies: Vec::new(),
                            avatar: Some(avatar_url),
                        }),
                    };

                    let mut conn = conn.lock().await;
                    let channel_name = conn
                        .get::<String, Option<String>>(format!("eludris:key:{}", url))
                        .await
                        .unwrap();
                    match channel_name {
                        Some(channel_name) => conn
                            .publish::<String, String, ()>(
                                channel_name,
                                serde_json::to_string(&event).unwrap(),
                            )
                            .await
                            .unwrap(),
                        None => {
                            log::debug!("Ignoring channel {}", url);
                        }
                    }
                }
            }
        });
    }

    Ok(())
}
