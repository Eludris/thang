use std::sync::Arc;

use models::{Event, EventData, Message, Reply, Result};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use tokio::sync::Mutex;
use twilight_gateway::{Event as GatewayEvent, Shard};

fn get_user_avatar(msg: &twilight_model::channel::Message) -> String {
    if let Some(avatar) = msg.author.avatar {
        format!(
            "https://cdn.discordapp.com/avatars/{}/{}.png",
            msg.author.id, avatar
        )
    } else {
        format!(
            "https://cdn.discordapp.com/embed/avatars/{}.png",
            msg.author.discriminator % 5
        )
    }
}

fn get_avatar(msg: &twilight_model::channel::Message) -> String {
    match &msg.member {
        Some(member) => match member.avatar {
            Some(avatar) => format!(
                "https://cdn.discordapp.com/guilds/{}/users/{}/avatars/{}.png",
                msg.guild_id.unwrap(),
                msg.author.id,
                avatar
            ),
            None => get_user_avatar(msg),
        },
        None => get_user_avatar(msg),
    }
}

fn get_name(msg: &twilight_model::channel::Message) -> String {
    match &msg.member {
        Some(member) => match &member.nick {
            Some(nick) => nick.clone(),
            None => msg.author.name.clone(),
        },
        None => msg.author.name.clone(),
    }
}

pub async fn handle_events(shard: &mut Shard, conn: MultiplexedConnection) -> Result<()> {
    let conn = Arc::new(Mutex::new(conn));
    loop {
        let event = match shard.next_event().await {
            Ok(event) => event,
            Err(source) => {
                log::warn!("error receiving event");

                if source.is_fatal() {
                    return Err(source.into());
                }

                continue;
            }
        };

        let conn = Arc::clone(&conn);
        tokio::spawn(async move {
            let (payload, channel_id) = match event {
                GatewayEvent::MessageCreate(data) => {
                    // Ignore webhook.
                    if data.webhook_id.is_some() {
                        return;
                    }

                    (
                        Event {
                            platform: "discord",
                            identifier: data.channel_id.to_string(),
                            data: EventData::MessageCreate(Message {
                                content: data.content.clone(),
                                author: get_name(&data),
                                attachments: data
                                    .attachments
                                    .clone()
                                    .into_iter()
                                    .map(|a| a.url)
                                    .collect(),
                                replies: match &data.referenced_message {
                                    Some(msg) => vec![Reply {
                                        content: msg.content.clone(),
                                        author: msg.author.name.clone(),
                                    }],
                                    None => Vec::new(),
                                },
                                avatar: Some(get_avatar(&data)),
                            }),
                        },
                        data.channel_id,
                    )
                }
                _ => return,
            };

            let mut conn = conn.lock().await;
            let channel_name = conn
                .get::<String, Option<String>>(format!("discord:key:{}", channel_id))
                .await
                .unwrap();
            match channel_name {
                Some(channel_name) => {
                    conn.publish::<&str, String, ()>(
                        &channel_name,
                        serde_json::to_string(&payload).unwrap(),
                    )
                    .await
                    .unwrap();
                }
                None => {
                    log::debug!("Ignoring channel {}", channel_id);
                }
            }
        });
    }
}
