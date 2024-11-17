use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{header, StatusCode},
    response::IntoResponse,
};
use bendy::encoding::{Error, SingleItemEncoder, ToBencode};
use serde::{de, Deserialize, Deserializer};

use crate::{AppState, Event, InfoHash, Peer, PeerId, Scrape, ScrapeData, ScrapeResponse};

#[derive(Deserialize)]
pub struct AnnounceRequest {
    #[serde(default, deserialize_with = "deserialize_url_encode")]
    info_hash: InfoHash,
    #[serde(default, deserialize_with = "deserialize_url_encode")]
    peer_id: PeerId,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    #[serde(default, deserialize_with = "deserialize_bool")]
    compact: bool,
    #[serde(default, deserialize_with = "deserialize_bool")]
    no_peer_id: bool,
    #[serde(default, deserialize_with = "deserialize_optional_fields")]
    event: Option<Event>,
    #[serde(default, deserialize_with = "deserialize_optional_fields")]
    ip: Option<IpAddr>,
    #[serde(default, deserialize_with = "deserialize_optional_fields")]
    numwant: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_fields")]
    key: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_fields")]
    trackerid: Option<String>,
}

#[derive(Debug)]
struct ScrapeRequest {
    info_hashes: Option<Vec<InfoHash>>,
}

fn deserialize_url_encode<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf: &[u8] = de::Deserialize::deserialize(deserializer)?;
    let decoded = urlencoding::decode_binary(buf).into_owned();
    if decoded.len() == 20 {
        Ok(decoded)
    } else {
        Err(de::Error::custom(
            "URL-encoded parameters should be 20 bytes in length",
        ))
    }
}

fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;
    match s {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        _ => Err(de::Error::unknown_variant(s, &["1", "0", "true", "false"])),
    }
}

fn deserialize_optional_fields<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
}

enum AnnounceResponse {
    Failure {
        failure_reason: String,
    },
    Success {
        interval: u64,
        complete: usize,
        incomplete: usize,
        peers: Vec<Peer>,
        peers6: Vec<Peer>,
        tracker_id: String,
        warning_message: Option<String>,
        min_interval: Option<u64>,
    },
}

impl ToBencode for AnnounceResponse {
    const MAX_DEPTH: usize = 5;
    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), Error> {
        match self {
            Self::Failure { failure_reason } => {
                encoder.emit_dict(|mut e| {
                    e.emit_pair(b"failure_reason", failure_reason)?;
                    Ok(())
                })?;
            }
            Self::Success {
                interval,
                complete,
                incomplete,
                tracker_id,
                peers,
                peers6,
                warning_message,
                min_interval,
            } => {
                encoder.emit_dict(|mut e| {
                    e.emit_pair(b"complete", complete)?;
                    e.emit_pair(b"incomplete", incomplete)?;
                    e.emit_pair(b"interval", interval)?;

                    if let Some(min_interval) = min_interval {
                        e.emit_pair(b"min_interval", min_interval)?;
                    }

                    e.emit_pair(b"peers", peers)?;
                    e.emit_pair(b"peers6", peers6)?;

                    e.emit_pair(b"tracker_id", tracker_id)?;

                    if let Some(warning_message) = warning_message {
                        e.emit_pair(b"warning_message", warning_message)?;
                    }

                    Ok(())
                })?;
            }
        }

        Ok(())
    }
}

pub async fn handle_announce(
    announce: Query<AnnounceRequest>,
    State(mut state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let announce: AnnounceRequest = announce.0;

    let info_hash = announce.info_hash;

    if let Some(event) = announce.event {
        let ip = if let Some(client_ip) = announce.ip {
            client_ip
        } else {
            addr.ip()
        };

        let peer = Peer {
            id: announce.peer_id,
            ip,
            port: announce.port,
        };

        let is_download_complete = announce.left == 0;

        match event {
            Event::Started => {
                state
                    .swarm_store
                    .add_peer(info_hash.clone(), peer, is_download_complete)
                    .await;
            }
            Event::Stopped => {
                state.swarm_store.remove_peer(info_hash.clone(), peer).await;
            }
            Event::Completed => {
                state
                    .swarm_store
                    .promote_peer(info_hash.clone(), peer)
                    .await;
                state
                    .torrent_store
                    .increment_downloaded(info_hash.clone())
                    .await;
            }
            Event::None => {}
        }
    }

    let numwant = if let Some(n) = announce.numwant {
        n
    } else {
        30 // 30 peers is generally a good amount
    };

    let (peers, peers6) = state
        .swarm_store
        .get_peers(info_hash.clone(), numwant)
        .await;
    let (complete, incomplete) = state.swarm_store.get_announce_stats(info_hash).await;

    let response = AnnounceResponse::Success {
        interval: 1800, // 30-minute announce interval
        complete,
        incomplete,
        peers,
        peers6,
        tracker_id: String::from("test"),
        min_interval: None,
        warning_message: None,
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        response.to_bencode().unwrap(),
    )
}

pub async fn handle_scrape(
    scrape: Query<Vec<(String, String)>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let info_hashes: Option<Vec<InfoHash>> = if scrape.0.is_empty() {
        None
    } else {
        let raw_info_hashes: Vec<&(String, String)> = scrape
            .0
            .iter()
            .filter(|(key, _)| key.to_lowercase() == "info_hash")
            .collect();
        if raw_info_hashes.is_empty() {
            None
        } else {
            let decoded_info_hashes = raw_info_hashes
                .into_iter()
                .map(|(_, raw_val)| urlencoding::decode_binary(raw_val.as_bytes()).into_owned())
                .filter(|buf| buf.len() == 20)
                .collect();
            Some(decoded_info_hashes)
        }
    };

    let scrape = ScrapeRequest { info_hashes };

    let scrapes = match scrape.info_hashes {
        Some(info_hashes) => {
            let (seeder_stats, leecher_stats) = state
                .swarm_store
                .get_stats_for_scrapes(info_hashes.clone())
                .await;
            let downloaded_stats = state
                .torrent_store
                .get_stats_for_scrapes(info_hashes.clone())
                .await;
            downloaded_stats
                .iter()
                .enumerate()
                .map(|(idx, downloaded)| {
                    let data = ScrapeData {
                        complete: seeder_stats[idx],
                        incomplete: leecher_stats[idx],
                        downloaded: *downloaded,
                    };
                    Scrape {
                        info_hash: info_hashes[idx].clone(),
                        data,
                    }
                })
                .collect::<Vec<Scrape>>()
        }
        None => {
            let seeder_leecher_map = state.swarm_store.get_global_scrape_stats().await;
            let downloaded_map = state.torrent_store.get_global_scrape_stats().await;

            downloaded_map
                .into_iter()
                .map(|(info_hash, downloaded)| {
                    let (complete, incomplete) = match seeder_leecher_map.get(&info_hash) {
                        Some((c, i)) => (*c, *i),
                        None => (0, 0),
                    };
                    let data = ScrapeData {
                        complete,
                        incomplete,
                        downloaded,
                    };
                    Scrape { info_hash, data }
                })
                .collect::<Vec<Scrape>>()
        }
    };

    let response = ScrapeResponse { files: scrapes };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        response.to_bencode().unwrap(),
    )
}
