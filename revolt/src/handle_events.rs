use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use models::{Event, Message};
use models::{EventData, Result};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use revolt_wrapper::gateway::Events;
use revolt_wrapper::models::{Channel, Member, MemberClear, User, UserClear};
use revolt_wrapper::{Event as GatewayEvent, HttpClient};
use tokio::sync::Mutex;

async fn get_server_id(
    channel: &str,
    http: &HttpClient,
    conn: &Mutex<MultiplexedConnection>,
) -> Result<String> {
    let mut conn = conn.lock().await;

    let channel = match conn.get(format!("textchannel:{}", channel)).await? {
        Some(channel) => channel,
        None => {
            let channel_enum = http.fetch_channel(channel).await?;
            let Channel::TextChannel(channel) = channel_enum;
            conn.set(format!("textchannel:{}", channel.id), &channel)
                .await?;
            channel
        }
    };

    log::debug!("Got channel {:?}", channel);

    Ok(channel.server)
}

async fn get_member(
    server: &str,
    member: &str,
    http: &HttpClient,
    conn: &Mutex<MultiplexedConnection>,
) -> Result<Member> {
    let mut conn = conn.lock().await;

    let member = match conn
        .get(format!("server:{}:member:{}", server, member))
        .await?
    {
        Some(member) => {
            log::debug!("Got member from cache {:?}", member);
            member
        }
        None => {
            let member = http.fetch_member(server, member).await?;
            conn.set(
                format!("server:{}:member:{}", server, member.id.user),
                &member,
            )
            .await?;

            member
        }
    };

    log::debug!("Got member {:?}", member);

    Ok(member)
}

async fn get_user(
    user: &str,
    http: &HttpClient,
    conn: &Mutex<MultiplexedConnection>,
) -> Result<User> {
    let mut conn = conn.lock().await;

    let user = match conn.get(format!("user:{}", user)).await? {
        Some(user) => {
            log::debug!("Got user from cache {:?}", user);
            user
        }
        None => {
            let user = http.fetch_user(user).await?;
            conn.set(format!("user:{}", user.id), &user).await?;
            user
        }
    };

    log::debug!("Got user {:?}", user);

    Ok(user)
}

async fn get_name(
    channel: &str,
    user: &str,
    http: &HttpClient,
    conn: &Mutex<MultiplexedConnection>,
) -> Result<String> {
    let server = get_server_id(channel, http, conn).await?;
    let member = get_member(&server, user, http, conn).await?;
    let user = get_user(user, http, conn).await?;

    Ok(match member.nickname {
        Some(nickname) => nickname,
        None => user.username,
    })
}

async fn get_avatar(
    channel: &str,
    user: &str,
    http: &HttpClient,
    conn: &Mutex<MultiplexedConnection>,
) -> Result<Option<String>> {
    let server = get_server_id(channel, http, conn).await?;
    let member = get_member(&server, user, http, conn).await?;
    let user = get_user(user, http, conn).await?;

    Ok(match member.avatar {
        Some(avatar) => Some(avatar.url()),
        None => user.avatar.map(|a| a.url()),
    })
}

async fn handle_event(
    event: GatewayEvent,
    conn: Arc<Mutex<MultiplexedConnection>>,
    bot_id: String,
    http: Arc<HttpClient>,
) -> Result<()> {
    let (payload, channel_id) = match event {
        GatewayEvent::Message(msg) => {
            if msg.author == bot_id && msg.masquerade.is_some() {
                return Ok(());
            }

            (
                Event {
                    platform: "revolt",
                    identifier: msg.channel.clone(),
                    data: EventData::MessageCreate(Message {
                        content: msg.content.unwrap_or("".to_string()),
                        author: get_name(&msg.channel, &msg.author, &http, &conn).await?,
                        attachments: match msg.attachments {
                            Some(attachments) => attachments
                                .into_iter()
                                .map(|a| a.url())
                                .collect::<Vec<String>>(),
                            None => Vec::new(),
                        },
                        // TODO: Requires caching messages waa.
                        replies: Vec::new(),
                        avatar: get_avatar(&msg.channel, &msg.author, &http, &conn).await?,
                    }),
                },
                msg.channel,
            )
        }
        GatewayEvent::Pong { data } => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis();
            log::info!(
                "Received pong! Latency is {}ms",
                now - u128::from_be_bytes(data.try_into().unwrap())
            );
            return Ok(());
        }
        GatewayEvent::Ready {} => {
            log::info!("Ready!");
            return Ok(());
        }
        GatewayEvent::ServerMemberUpdate { id, data, clear } => {
            let mut conn = conn.lock().await;
            let key = format!("server:{}:member:{}", id.server, id.user);

            // Only update if the key exists.
            // As otherwise, there would be a partial cache,
            // without any way to see if a key really doesnt exist or simply hasnt been cached yet.
            if let Some(mut member) = conn.get::<&str, Option<Member>>(&key).await? {
                member.apply_options(data);

                clear.into_iter().for_each(|field| {
                    // clear struct fields into None
                    match field {
                        MemberClear::Nickname => member.nickname = None,
                        MemberClear::Avatar => member.avatar = None,
                    }
                });

                conn.set::<&str, Member, ()>(&key, member).await?;
            }

            return Ok(());
        }
        GatewayEvent::ServerMemberLeave { id, user } => {
            conn.lock()
                .await
                .del::<&str, ()>(format!("servers:{}:member:{}", id, user).as_str())
                .await?;

            return Ok(());
        }
        GatewayEvent::UserUpdate { id, data, clear } => {
            let mut conn = conn.lock().await;
            let key = format!("users:{}", id);

            if let Some(mut user) = conn.get::<&str, Option<User>>(&key).await? {
                user.apply_options(data);

                clear.into_iter().for_each(|field| {
                    // clear struct fields into None
                    match field {
                        UserClear::Avatar => user.avatar = None,
                    }
                });

                conn.set::<&str, User, ()>(&key, user).await?;
            }

            return Ok(());
        }
        _ => return Ok(()),
    };

    let mut conn = conn.lock().await;
    let channel_name = conn
        .get::<String, Option<String>>(format!("revolt:key:{}", channel_id))
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
            log::debug!("Ignoring message from unknown channel {}", channel_id);
        }
    }

    Ok(())
}

pub async fn handle_events(
    events: &mut Events,
    conn: MultiplexedConnection,
    bot_id: String,
    http: Arc<HttpClient>,
) -> Result<()> {
    let conn = Arc::new(Mutex::new(conn));
    while let Some(event) = events.next().await {
        let conn = Arc::clone(&conn);
        let bot_id = bot_id.clone();
        let http = Arc::clone(&http);
        tokio::spawn(async move {
            if let Err(err) = handle_event(event, conn, bot_id, http).await {
                log::error!("Error handling event: {}", err);
            }
        });
    }

    Ok(())
}
