use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, State},
    response::IntoResponse,
};

use crate::{AppState, Event, InfoHash, Peer, PeerId, ScrapeData};

const PROTOCOL_ID: i64 = 41_727_101_980;

enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    // Server replies only
    Error = 3,
}

struct ConnectionRequest {
    connection_id: i64,
    action: Action,
    transaction_id: i32,
}

struct ConnectionResponse {
    action: Action,
    transaction_id: i32,
    connection_id: i64,
}

struct AnnounceRequest {
    connection_id: i64,
    action: Action,
    transaction_id: i32,
    info_hash: InfoHash,
    peer_id: PeerId,
    downloaded: i64,
    left: i64,
    event: Event,
    // Set to zero if you want to use IP of sender
    ip: u32,
    key: u32,
    num_want: i32,
    port: u16,
    extensions: u16,
}

struct AnnounceResponse {
    action: Action,
    transaction_id: i32,
    interval: i32,
    leechers: i32,
    seeders: i32,
    peers: Vec<Peer>,
}

struct ScrapeRequest {
    connection_id: i64,
    action: Action,
    transaction_id: i32,
    info_hashes: Vec<InfoHash>,
}

struct ScrapeResponse {
    action: Action,
    transaction_id: i32,
    scrapes: Vec<ScrapeData>,
}

pub async fn handle_announce() -> impl IntoResponse {
    todo!()
}

pub async fn handle_scrape() -> impl IntoResponse {
    todo!()
}
