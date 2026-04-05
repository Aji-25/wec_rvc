use anyhow::{Context, Result};
use base64::Engine as _;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use libp2p::{
    mdns,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::time;
use rvc_core::protocol::{RVC_PROTOCOL, RvcCodec, RvcRequest, RvcResponse};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "rvcd", about = "RvC peer daemon")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Scan the LAN for peers (~3 s) and print their names
    Discover,
}

const DISCOVER_SCAN_SECS: u64 = 5;

use rvc_core::sync::{SyncRequest, SyncResponse};

// ─── NetworkBehaviour ─────────────────────────────────────────────────────────

#[derive(NetworkBehaviour)]
struct RvcBehaviour {
    mdns: mdns::tokio::Behaviour,
    rr:   request_response::Behaviour<RvcCodec>,
}

// ─── PeerInfo ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PeerInfo {
    name:      String,
    addr:      Multiaddr,
    head_hash: Option<String>,
    last_seen: i64,
}

// ─── Config / Repo helpers ────────────────────────────────────────────────────

fn find_rvc_dir() -> Result<std::path::PathBuf> {
    let mut dir = std::env::current_dir()?;
    loop {
        let c = dir.join(".rvc");
        if c.is_dir() { return Ok(c); }
        if !dir.pop() { anyhow::bail!("not an rvc repository (no .rvc found)"); }
    }
}

fn config_field(rvc: &Path, key: &str) -> Option<String> {
    let s = std::fs::read_to_string(rvc.join("config")).ok()?;
    s.lines().find_map(|l| l.strip_prefix(&format!("{key}=")).map(str::to_string))
}

fn default_peer_name() -> String {
    let user = std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".to_string());
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "device".to_string());
    format!("{user}@{host}")
}

fn read_head(rvc: &Path) -> Option<String> {
    let s = std::fs::read_to_string(rvc.join("refs").join("HEAD")).ok()?;
    let t = s.trim().to_string();
    if t.is_empty() { None } else { Some(t) }
}

fn persist_peers(rvc: &Path, peers: &HashMap<PeerId, PeerInfo>) -> Result<()> {
    let mut lines = Vec::new();
    for (peer_id, info) in peers {
        let head_hash = info.head_hash.clone().unwrap_or_default();
        lines.push(format!(
            "{}|{}|{}|{}|{}",
            info.name,
            peer_id,
            info.addr,
            head_hash,
            info.last_seen
        ));
    }
    lines.sort();
    std::fs::write(rvc.join("peers"), lines.join("\n")).context("write peers cache")?;
    Ok(())
}

fn now_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}

// ─── ed25519 identity (ring + libp2p) ────────────────────────────────────────

fn generate_or_load_keypair(rvc: &Path) -> Result<libp2p::identity::Keypair> {
    if let Some(encoded) = config_field(rvc, "keypair") {
        let bytes = base64::engine::general_purpose::STANDARD.decode(&encoded)
            .context("base64 decode keypair")?;
        return libp2p::identity::Keypair::from_protobuf_encoding(&bytes)
            .map_err(|e| anyhow::anyhow!("keypair load: {e}"));
    }

    // Generate 32-byte seed via ring's SystemRandom, then build libp2p ed25519 keypair
    let rng = SystemRandom::new();
    let mut seed = [0u8; 32];
    rng.fill(&mut seed).map_err(|_| anyhow::anyhow!("RNG failed"))?;

    let secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(&mut seed)
        .map_err(|e| anyhow::anyhow!("ed25519 secret: {e}"))?;
    let keypair = libp2p::identity::Keypair::from(
        libp2p::identity::ed25519::Keypair::from(secret)
    );

    // Persist to config
    let encoded = base64::engine::general_purpose::STANDARD.encode(
        keypair.to_protobuf_encoding().map_err(|e| anyhow::anyhow!("{e}"))?,
    );
    let existing = std::fs::read_to_string(rvc.join("config")).unwrap_or_default();
    let new_content = format!("{}\nkeypair={encoded}\n", existing.trim_end());
    std::fs::write(rvc.join("config"), new_content).context("write config keypair")?;

    Ok(keypair)
}

// ─── Swarm builder ────────────────────────────────────────────────────────────

fn build_swarm(keypair: libp2p::identity::Keypair) -> Result<libp2p::Swarm<RvcBehaviour>> {
    use libp2p::noise;
    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key| {
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                key.public().to_peer_id(),
            )?;
            let rr_config = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30));
            let rr = request_response::Behaviour::new(
                [(RVC_PROTOCOL, ProtocolSupport::Full)],
                rr_config,
            );
            Ok(RvcBehaviour { mdns, rr })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    Ok(swarm)
}

// ─── Handshake helper ─────────────────────────────────────────────────────────

fn send_handshake(
    swarm: &mut libp2p::Swarm<RvcBehaviour>,
    peer: &PeerId,
    name: &str,
    rvc: &Path,
) {
    swarm.behaviour_mut().rr.send_request(
        peer,
        RvcRequest::Handshake { name: name.to_string(), head_hash: read_head(rvc) },
    );
}

// ─── Daemon mode ──────────────────────────────────────────────────────────────

async fn run_daemon(rvc: &Path, my_name: String) -> Result<()> {
    let keypair = generate_or_load_keypair(rvc)?;
    let mut swarm = build_swarm(keypair)?;
    let mut peers: HashMap<PeerId, PeerInfo> = HashMap::new();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?).context("listen")?;
    println!("rvcd: peer_id={} name={my_name}", swarm.local_peer_id());

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                if address.to_string().starts_with("/ip4/127.0.0.1/") {
                    println!("rvcd: listening on {address}");
                }
            }
            SwarmEvent::Behaviour(RvcBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, addr) in list {
                    if peer_id == *swarm.local_peer_id() { continue; }
                    peers
                        .entry(peer_id)
                        .and_modify(|info| info.addr = addr.clone())
                        .or_insert_with(|| PeerInfo {
                            name: peer_id.to_string(),
                            addr: addr.clone(),
                            head_hash: None,
                            last_seen: now_timestamp(),
                        });
                    if let Some(info) = peers.get_mut(&peer_id) {
                        info.last_seen = now_timestamp();
                    }
                    let _ = persist_peers(rvc, &peers);
                    swarm.add_peer_address(peer_id, addr);
                    send_handshake(&mut swarm, &peer_id, &my_name, rvc);
                }
            }
            SwarmEvent::Behaviour(RvcBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer_id, _) in list { peers.remove(&peer_id); }
                let _ = persist_peers(rvc, &peers);
            }
            SwarmEvent::Behaviour(RvcBehaviourEvent::Rr(
                request_response::Event::Message { peer, message }
            )) => {
                match message {
                    request_response::Message::Request { request, channel, .. } => {
                        match request {
                            RvcRequest::Handshake { name, head_hash } => {
                                let addr = peers
                                    .get(&peer)
                                    .map(|info| info.addr.clone())
                                    .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".parse().unwrap());
                                peers.insert(peer, PeerInfo { name, addr, head_hash, last_seen: now_timestamp() });
                                let _ = persist_peers(rvc, &peers);
                                let _ = swarm.behaviour_mut().rr.send_response(
                                    channel,
                                    RvcResponse::HandshakeAck {
                                        name: my_name.clone(),
                                        head_hash: read_head(rvc),
                                    },
                                );
                            }
                            RvcRequest::Sync(req) => {
                                match req {
                                    SyncRequest::GetHead => {
                                        let head = read_head(rvc).unwrap_or_default();
                                        let _ = swarm.behaviour_mut().rr.send_response(
                                            channel,
                                            RvcResponse::Sync(SyncResponse::Head(head)),
                                        );
                                    }
                                    SyncRequest::GetMissingFrom(their_hash) => {
                                        let hashes = rvc_core::sync::find_missing(&their_hash, rvc).unwrap_or_default();
                                        let _ = swarm.behaviour_mut().rr.send_response(
                                            channel,
                                            RvcResponse::Sync(SyncResponse::MissingHashes(hashes)),
                                        );
                                    }
                                    SyncRequest::GetObjects(hashes) => {
                                        let mut objs = Vec::new();
                                        for h in hashes {
                                            if let Ok(bytes) = rvc_core::sync::read_raw_object(rvc, &h) {
                                                objs.push((h.clone(), bytes));
                                            }
                                        }
                                        let _ = swarm.behaviour_mut().rr.send_response(
                                            channel,
                                            RvcResponse::Sync(SyncResponse::Objects(objs)),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    request_response::Message::Response { response, .. } => {
                        if let RvcResponse::HandshakeAck { name, head_hash } = response {
                            let addr = peers
                                .get(&peer)
                                .map(|info| info.addr.clone())
                                .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".parse().unwrap());
                            peers.insert(peer, PeerInfo { name, addr, head_hash, last_seen: now_timestamp() });
                            let _ = persist_peers(rvc, &peers);
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(RvcBehaviourEvent::Rr(
                request_response::Event::OutboundFailure { peer, error, .. }
            )) => {
                eprintln!("rvcd: outbound failure → {}: {error}", &peer.to_string()[..8]);
            }
            SwarmEvent::Behaviour(RvcBehaviourEvent::Rr(
                request_response::Event::InboundFailure { peer, error, .. }
            )) => {
                eprintln!("rvcd: inbound failure → {}: {:?}", &peer.to_string()[..8], error);
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                let addr = endpoint.get_remote_address().clone();
                peers
                    .entry(peer_id)
                    .and_modify(|info| info.addr = addr.clone())
                    .or_insert_with(|| PeerInfo {
                        name: peer_id.to_string(),
                        addr,
                        head_hash: None,
                        last_seen: now_timestamp(),
                    });
                if let Some(info) = peers.get_mut(&peer_id) {
                    info.last_seen = now_timestamp();
                }
                let _ = persist_peers(rvc, &peers);
            }
            SwarmEvent::ConnectionClosed { .. } => {}
            _other => {
                // optionally log everything else
                // println!("rvcd: event {:?}", other);
            }
        }
    }
}

// ─── Discover mode ────────────────────────────────────────────────────────────

async fn run_discover(rvc: &Path, my_name: String) -> Result<()> {
    let keypair = generate_or_load_keypair(rvc)?;
    let mut swarm = build_swarm(keypair)?;
    let mut found: HashMap<PeerId, String> = HashMap::new();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?).context("listen")?;

    // Flat deadline — exits early once all handshakes resolve.
    let deadline = time::sleep(Duration::from_secs(DISCOVER_SCAN_SECS));
    tokio::pin!(deadline);
    let mut pending: std::collections::HashSet<PeerId> = std::collections::HashSet::new();
    let mut listener_up = false;

    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => break,
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { .. } => {
                    listener_up = true;
                }
                SwarmEvent::Behaviour(RvcBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    if listener_up {
                        for (peer_id, addr) in list {
                            if peer_id == *swarm.local_peer_id() { continue; }
                            if !found.contains_key(&peer_id) && !pending.contains(&peer_id) {
                                swarm.add_peer_address(peer_id, addr);
                                send_handshake(&mut swarm, &peer_id, &my_name, rvc);
                                pending.insert(peer_id);
                            }
                        }
                    }
                }
                SwarmEvent::Behaviour(RvcBehaviourEvent::Rr(
                    request_response::Event::Message { peer, message }
                )) => {
                    match message {
                        // Inbound handshake from the remote daemon
                        request_response::Message::Request { request, channel, .. } => {
                            if let RvcRequest::Handshake { name, .. } = request {
                                found.insert(peer, name);
                                pending.remove(&peer);
                                let _ = swarm.behaviour_mut().rr.send_response(
                                    channel,
                                    RvcResponse::HandshakeAck {
                                        name: my_name.clone(),
                                        head_hash: read_head(rvc),
                                    },
                                );
                            }
                        }
                        // Ack to our outbound handshake
                        request_response::Message::Response { response, .. } => {
                            if let RvcResponse::HandshakeAck { name, .. } = response {
                                found.insert(peer, name);
                                pending.remove(&peer);
                            }
                        }
                    }
                }
                SwarmEvent::Behaviour(RvcBehaviourEvent::Rr(
                    request_response::Event::OutboundFailure { peer, error, .. }
                )) => {
                    eprintln!("discover: outbound failure → {}: {error}", &peer.to_string()[..8]);
                }
                _other => {}
            }
        }
        // Early exit once all outstanding handshakes resolved
        if listener_up && !pending.is_empty() && pending.iter().all(|p| found.contains_key(p)) {
            break;
        }
    }

    // Print in exact CLAUDE.md format
    println!("peers found:");
    if found.is_empty() {
        println!("(none)");
    } else {
        for name in found.values() {
            println!("{name}");
        }
    }
    Ok(())
}


// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Best-effort: find .rvc dir; if not found, some ops still work (e.g. daemon where repo is TBD)
    let rvc_result = find_rvc_dir();

    let my_name = match &rvc_result {
        Ok(rvc) => {
            config_field(rvc, "peer_name")
                .or_else(|| config_field(rvc, "author"))
                .unwrap_or_else(default_peer_name)
        }
        Err(_) => {
            default_peer_name()
        }
    };

    // For keypair persistence we need a real .rvc dir
    let rvc = rvc_result.context("run `rvc init` first to create a repository")?;

    match cli.command {
        Some(Command::Discover) => run_discover(&rvc, my_name).await,
        None                    => run_daemon(&rvc, my_name).await,
    }
}
