use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::{fmt, str::FromStr};

use axum::extract::{ConnectInfo, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{extract::Query, routing::get, Router};

use bendy::encoding::{Error, SingleItemEncoder, ToBencode};
use rand::seq::SliceRandom;
use serde::{de, Deserialize, Deserializer};
use tokio::sync::RwLock;

type InfoHash = Vec<u8>;
type PeerId = Vec<u8>;

pub mod protocol;

#[derive(Hash, Eq, PartialEq, Clone)]
struct Peer {
    id: PeerId,
    ip: IpAddr,
    port: u16,
}

impl ToBencode for Peer {
    const MAX_DEPTH: usize = 1;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), Error> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"ip", self.ip.to_string())?;
            e.emit_pair(b"peer_id", self.id.clone())?;
            e.emit_pair(b"port", self.port)?;
            Ok(())
        })?;

        Ok(())
    }
}

#[derive(Clone)]
struct Swarm {
    seeders: HashSet<Peer>,
    leechers: HashSet<Peer>,
}

impl Swarm {
    fn new() -> Swarm {
        Swarm {
            seeders: HashSet::new(),
            leechers: HashSet::new(),
        }
    }

    fn add_seeder(&mut self, peer: Peer) {
        self.seeders.insert(peer);
    }

    fn add_leecher(&mut self, peer: Peer) {
        self.leechers.insert(peer);
    }

    fn remove_seeder(&mut self, peer: Peer) {
        self.seeders.remove(&peer);
    }

    fn remove_leecher(&mut self, peer: Peer) {
        self.leechers.remove(&peer);
    }

    fn promote_leecher(&mut self, peer: Peer) {
        match self.leechers.take(&peer) {
            Some(leecher) => self.seeders.insert(leecher),
            None => self.seeders.insert(peer),
        };
    }
}

#[derive(Clone)]
pub struct SwarmStore(Arc<RwLock<HashMap<InfoHash, Swarm>>>);

impl SwarmStore {
    pub fn new() -> SwarmStore {
        SwarmStore(Arc::new(RwLock::new(HashMap::new())))
    }

    async fn add_peer(&mut self, info_hash: InfoHash, peer: Peer, is_download_complete: bool) {
        let mut write_locked_store = self.0.write().await;
        match write_locked_store.get_mut(&info_hash) {
            Some(swarm) => {
                if is_download_complete {
                    swarm.add_seeder(peer);
                } else {
                    swarm.add_leecher(peer);
                }
            }
            None => {
                let mut swarm = Swarm::new();
                if is_download_complete {
                    swarm.add_seeder(peer);
                } else {
                    swarm.add_leecher(peer);
                }
                write_locked_store.insert(info_hash, swarm);
            }
        }
    }

    async fn remove_peer(&mut self, info_hash: InfoHash, peer: Peer) {
        let mut write_locked_store = self.0.write().await;
        if let Some(swarm) = write_locked_store.get_mut(&info_hash) {
            swarm.remove_seeder(peer.clone());
            swarm.remove_leecher(peer);
        }
    }

    async fn promote_peer(&mut self, info_hash: InfoHash, peer: Peer) {
        let mut write_locked_store = self.0.write().await;
        if let Some(swarm) = write_locked_store.get_mut(&info_hash) {
            swarm.promote_leecher(peer)
        }
    }

    async fn get_peers(&self, info_hash: InfoHash, numwant: usize) -> (Vec<Peer>, Vec<Peer>) {
        let (mut peers, mut peers6): (Vec<Peer>, Vec<Peer>) = (Vec::new(), Vec::new());
        let read_locked_store = self.0.read().await;
        if let Some(swarm) = read_locked_store.get(&info_hash) {
            for peer in swarm.seeders.clone().into_iter() {
                if peer.ip.is_ipv4() {
                    peers.push(peer);
                } else {
                    peers6.push(peer);
                }
            }

            for peer in swarm.leechers.clone().into_iter() {
                if peer.ip.is_ipv4() {
                    peers.push(peer);
                } else {
                    peers6.push(peer);
                }
            }

            if (swarm.seeders.len() + swarm.leechers.len()) > numwant {
                let mut rng = rand::thread_rng();
                peers.shuffle(&mut rng);
                peers6.shuffle(&mut rng);
                peers.truncate(numwant);
                peers6.truncate(numwant);
            }
        }

        (peers, peers6)
    }

    async fn get_announce_stats(&self, info_hash: InfoHash) -> (usize, usize) {
        let read_locked_store = self.0.read().await;
        if let Some(swarm) = read_locked_store.get(&info_hash) {
            (swarm.seeders.len(), swarm.leechers.len())
        } else {
            (0, 0)
        }
    }

    async fn get_stats_for_scrapes(&self, info_hashes: Vec<InfoHash>) -> (Vec<usize>, Vec<usize>) {
        let read_locked_store = self.0.read().await;
        let (complete_vec, incomplete_vec) = info_hashes
            .iter()
            .map(|info_hash| {
                if let Some(swarm) = read_locked_store.get(info_hash) {
                    (swarm.seeders.len(), swarm.leechers.len())
                } else {
                    (0, 0)
                }
            })
            .unzip();
        (complete_vec, incomplete_vec)
    }

    async fn get_global_scrape_stats(&self) -> HashMap<InfoHash, (usize, usize)> {
        let read_locked_store = self.0.read().await;
        let mut global_stats = HashMap::new();

        for (info_hash, swarm) in read_locked_store.iter() {
            global_stats.insert(
                info_hash.clone(),
                (swarm.seeders.len(), swarm.leechers.len()),
            );
        }

        global_stats
    }
}

#[derive(Deserialize)]
struct Torrent {
    info_hash: InfoHash,
    downloaded: usize,
}

#[derive(Clone)]
pub struct TorrentStore(Arc<RwLock<HashMap<InfoHash, Torrent>>>);

impl TorrentStore {
    pub fn new() -> TorrentStore {
        TorrentStore(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn add_torrent(&mut self, info_hash: InfoHash) {
        let mut write_locked_store = self.0.write().await;
        write_locked_store.insert(
            info_hash.clone(),
            Torrent {
                info_hash,
                downloaded: 0,
            },
        );
    }

    async fn increment_downloaded(&mut self, info_hash: InfoHash) {
        let mut write_locked_store = self.0.write().await;
        if let Some(torrent) = write_locked_store.get_mut(&info_hash) {
            torrent.downloaded += 1;
        }
    }

    async fn get_stats_for_scrapes(&self, info_hashes: Vec<InfoHash>) -> Vec<usize> {
        let read_locked_store = self.0.read().await;
        let stats = info_hashes
            .iter()
            .map(|info_hash| {
                if let Some(torrent) = read_locked_store.get(info_hash) {
                    torrent.downloaded
                } else {
                    0
                }
            })
            .collect();
        stats
    }

    async fn get_global_scrape_stats(&self) -> HashMap<InfoHash, usize> {
        let read_locked_store = self.0.read().await;
        let mut global_stats = HashMap::new();

        for (info_hash, torrent) in read_locked_store.iter() {
            global_stats.insert(info_hash.clone(), torrent.downloaded);
        }

        global_stats
    }
}

#[derive(Deserialize)]
enum Event {
    None = 0,
    Started = 1,
    Stopped = 2,
    Completed = 3,
}

#[derive(Debug, PartialEq, Eq)]
struct EventParseError(String);

impl Display for EventParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse event: {}", self.0)
    }
}

impl FromStr for Event {
    type Err = EventParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Started" | "started" | "STARTED" => Ok(Self::Started),
            "Stopped" | "stopped" | "STOPPED" => Ok(Self::Stopped),
            "Completed" | "completed" | "COMPLETED" => Ok(Self::Completed),
            _ => Err(EventParseError(s.to_string())),
        }
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Started => write!(f, "started"),
            Self::Stopped => write!(f, "stopped"),
            Self::None => todo!(),
        }
    }
}

#[derive(Clone)]
struct ScrapeData {
    complete: usize,
    downloaded: usize,
    incomplete: usize,
}

#[derive(Clone)]
struct Scrape {
    info_hash: InfoHash,
    data: ScrapeData,
}

#[derive(Clone)]
struct ScrapeResponse {
    files: Vec<Scrape>,
}

impl ToBencode for ScrapeData {
    const MAX_DEPTH: usize = 2;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), Error> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"complete", self.complete)?;
            e.emit_pair(b"downloaded", self.downloaded)?;
            e.emit_pair(b"incomplete", self.incomplete)?;

            Ok(())
        })?;
        Ok(())
    }
}

impl ToBencode for Scrape {
    const MAX_DEPTH: usize = 2;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), Error> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(&self.info_hash, self.data.clone())?;

            Ok(())
        })?;
        Ok(())
    }
}

impl ToBencode for ScrapeResponse {
    const MAX_DEPTH: usize = 5;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), Error> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"files", self.files.clone())?;

            Ok(())
        })?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub swarm_store: SwarmStore,
    pub torrent_store: TorrentStore,
}
