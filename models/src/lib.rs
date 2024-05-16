use serde::{Deserialize, Serialize};
use std::error::Error;
use std::num::NonZeroU64;
use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Event<'a> {
    pub platform: &'a str,
    pub identifier: String,
    pub data: EventData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum EventData {
    MessageCreate(Message),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub content: String,
    pub author: String,
    pub attachments: Vec<String>,
    pub replies: Vec<Reply>,
    pub avatar: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    pub content: String,
    pub author: String,
}

pub type Config = Vec<ChannelConfig>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub name: String,
    pub discord: Option<Vec<NonZeroU64>>,
    pub eludris: Option<String>,
    pub revolt: Option<Vec<String>>,
}
