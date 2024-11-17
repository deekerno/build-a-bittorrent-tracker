use std::net::SocketAddr;

use axum::{routing::get, Router};
use build_a_bitorrent_tracker::protocol::udp::{handle_announce, handle_scrape};
use build_a_bitorrent_tracker::{AppState, SwarmStore, TorrentStore};

#[tokio::main]
async fn main() {
    let mut state = AppState {
        swarm_store: SwarmStore::new(),
        torrent_store: TorrentStore::new(),
    };

    state
        .torrent_store
        .add_torrent("aaaaaaaaaaaaaaaaaaaa".as_bytes().to_vec())
        .await;
    state
        .torrent_store
        .add_torrent("bbbbbbbbbbbbbbbbbbbb".as_bytes().to_vec())
        .await;

    let app = Router::new()
        .route("/announce", get(handle_announce))
        .route("/scrape", get(handle_scrape))
        .route("/", get(|| async { "Hello, World!" }))
        .with_state(state);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}
