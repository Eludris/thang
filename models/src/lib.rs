use serde::{Deserialize, Serialize};
use todel::models::Payload as EludrisEvent;
use twilight_model::gateway::payload::incoming::*;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "platform")]
pub enum Event {
    Eludris(EludrisEvent),
    Discord(DiscordEvent),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", content = "d")]
pub enum DiscordEvent {
    ChannelPinsUpdate(ChannelPinsUpdate),
    ChannelUpdate(Box<ChannelUpdate>),
    GuildEmojisUpdate(GuildEmojisUpdate),
    GuildStickersUpdate(GuildStickersUpdate),
    MessageCreate(Box<MessageCreate>),
    MessageDelete(MessageDelete),
    MessageDeleteBulk(MessageDeleteBulk),
    MessageUpdate(Box<MessageUpdate>),
    ReactionAdd(Box<ReactionAdd>),
    ReactionRemove(Box<ReactionRemove>),
    ReactionRemoveAll(ReactionRemoveAll),
    ReactionRemoveEmoji(ReactionRemoveEmoji),
}
