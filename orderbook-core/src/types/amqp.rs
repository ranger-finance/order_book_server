use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2SnapshotMessage {
    pub coin: String,
    pub timestamp: u64,
    pub sequence: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2DeltaMessage {
    pub coin: String,
    pub timestamp: u64,
    pub sequence: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
    pub from_sequence: u64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum L2Message {
    Snapshot(L2SnapshotMessage),
    Delta(L2DeltaMessage),
}
