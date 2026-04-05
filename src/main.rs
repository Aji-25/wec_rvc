use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rvc_core::protocol::{RVC_PROTOCOL, RvcCodec, RvcRequest, RvcResponse};
use rvc_core::sync::{SyncRequest, SyncResponse};
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    mdns, noise, request_response::{self, ProtocolSupport}, tcp, yamux,
    swarm::{SwarmEvent, NetworkBehaviour},
};
use futures::StreamExt;
use std::time::Duration;
use tokio::time;
use rvc_core::{
    read_blob, read_commit, read_tree, store_commit, write_object, Blob, Commit, MergeResult, Tree,
    TreeEntry,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

// ─── CLI Definition ─────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "rvc", about = "Decentralized peer-to-peer version control")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialise a new rvc repository in the current directory
    Init,

    /// Snapshot the working directory and create a new commit
    Commit {
        #[arg(short, long, help = "Commit message")]
        message: String,
    },

    /// Walk and print the commit chain from HEAD
    Log,

    /// Sync commits with a specific peer
    Sync {
        /// Target peer address
        peer: String,
    },

    /// Sync commits with a known peer by cached name
    SyncPeer {
        /// Peer name from `rvc peers`
        name: String,
    },

    /// Show repository status
    Status,

    /// Discover peers on the LAN and refresh the local peer cache
    Discover,

    /// Show known peers from the local peer cache
    Peers,

    /// Show files involved in a pending merge conflict
    Conflicts,

    /// Show local author and peer identity information
    Whoami,

    /// Update the author name stored in `.rvc/config`
    SetAuthor {
        /// New author name
        name: String,
    },

    /// Update the peer name advertised to other devices
    SetPeerName {
        /// New peer name
        name: String,
    },

    /// Set a local alias for a cached peer
    AliasPeer {
        /// Peer name, peer id, or alias to target
        peer: String,
        /// Alias to assign locally
        alias: String,
    },

    /// Mark conflicted files as resolved after removing conflict markers
    Resolve {
        /// Specific files to resolve; resolves all clean conflicts if omitted
        paths: Vec<String>,
    },

    /// Abort a merge in progress and restore the local HEAD tree
    AbortMerge,
}

// ─── Repo Paths ──────────────────────────────────────────────────────────────

/// Find the `.rvc` directory starting from CWD, walking up.
fn find_rvc_root() -> Result<PathBuf> {
    let mut dir = std::env::current_dir()?;
    loop {
        let candidate = dir.join(".rvc");
        if candidate.is_dir() {
            return Ok(candidate);
        }
        if !dir.pop() {
            anyhow::bail!("not an rvc repository (no .rvc directory found)");
        }
    }
}

fn head_path(rvc: &Path) -> PathBuf { rvc.join("refs").join("HEAD") }
fn config_path(rvc: &Path) -> PathBuf { rvc.join("config") }
fn merge_head_path(rvc: &Path) -> PathBuf { rvc.join("refs").join("MERGE_HEAD") }
fn merge_tree_path(rvc: &Path) -> PathBuf { rvc.join("refs").join("MERGE_TREE") }
fn merge_conflicts_path(rvc: &Path) -> PathBuf { rvc.join("refs").join("MERGE_CONFLICTS") }
fn peer_aliases_path(rvc: &Path) -> PathBuf { rvc.join("peer_aliases") }

fn default_author() -> String {
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn default_peer_name() -> String {
    let user = default_author();
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "device".to_string());
    let host = host
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect::<String>();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| format!("{:04x}", duration.subsec_nanos() & 0xffff))
        .unwrap_or_else(|_| "0000".to_string());
    format!("{user}@{host}-{suffix}")
}

fn read_config_map(rvc: &Path) -> Result<BTreeMap<String, String>> {
    let path = config_path(rvc);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&path).context("read config")?;
    let mut map = BTreeMap::new();
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Some((key, value)) = line.split_once('=') {
            map.insert(key.to_string(), value.to_string());
        }
    }
    Ok(map)
}

fn write_config_map(rvc: &Path, config: &BTreeMap<String, String>) -> Result<()> {
    let mut content = String::new();
    for (key, value) in config {
        content.push_str(key);
        content.push('=');
        content.push_str(value);
        content.push('\n');
    }
    fs::write(config_path(rvc), content).context("write config")
}

fn read_config_value(rvc: &Path, key: &str) -> Result<Option<String>> {
    Ok(read_config_map(rvc)?.remove(key))
}

fn write_config_value(rvc: &Path, key: &str, value: &str) -> Result<()> {
    let mut config = read_config_map(rvc)?;
    config.insert(key.to_string(), value.to_string());
    write_config_map(rvc, &config)
}

// ─── HEAD helpers ────────────────────────────────────────────────────────────

fn read_head(rvc: &Path) -> Result<Option<String>> {
    let p = head_path(rvc);
    if !p.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&p).context("read HEAD")?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn write_head(rvc: &Path, hash: &str) -> Result<()> {
    let p = head_path(rvc);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&p, hash).context("write HEAD")
}

fn read_merge_head(rvc: &Path) -> Result<Option<String>> {
    let p = merge_head_path(rvc);
    if !p.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&p).context("read MERGE_HEAD")?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn read_merge_tree(rvc: &Path) -> Result<Option<String>> {
    let p = merge_tree_path(rvc);
    if !p.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&p).context("read MERGE_TREE")?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

fn write_merge_state(rvc: &Path, merge_head: &str, merge_tree: &str, conflicts: &[String]) -> Result<()> {
    let merge_head_file = merge_head_path(rvc);
    if let Some(parent) = merge_head_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&merge_head_file, merge_head).context("write MERGE_HEAD")?;
    fs::write(merge_tree_path(rvc), merge_tree).context("write MERGE_TREE")?;
    fs::write(merge_conflicts_path(rvc), conflicts.join("\n")).context("write MERGE_CONFLICTS")?;
    Ok(())
}

fn clear_merge_state(rvc: &Path) -> Result<()> {
    for path in [merge_head_path(rvc), merge_tree_path(rvc), merge_conflicts_path(rvc)] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
        }
    }
    Ok(())
}

fn read_merge_conflicts(rvc: &Path) -> Result<Vec<String>> {
    let path = merge_conflicts_path(rvc);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(&path).context("read MERGE_CONFLICTS")?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

#[derive(Debug, Clone)]
struct CachedPeer {
    name: String,
    peer_id: String,
    addr: String,
    head: Option<String>,
    last_seen: Option<i64>,
    alias: Option<String>,
}

fn read_cached_peers(rvc: &Path) -> Result<Vec<CachedPeer>> {
    let peers_path = rvc.join("peers");
    if !peers_path.exists() {
        return Ok(Vec::new());
    }

    let raw = fs::read_to_string(&peers_path).context("read peers cache")?;
    let aliases = read_peer_aliases(rvc)?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            let mut parts = line.split('|');
            let name = parts.next()?.to_string();
            let peer_id = parts.next()?.to_string();
            let addr = parts.next()?.to_string();
            if peer_id.is_empty() || addr.is_empty() {
                return None;
            }
            let head = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let last_seen = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .and_then(|value| value.parse::<i64>().ok());

            Some(CachedPeer {
                name,
                alias: aliases.get(&peer_id).cloned(),
                peer_id,
                addr,
                head,
                last_seen,
            })
        })
        .collect())
}

fn print_cached_peers(peers: &[CachedPeer]) {
    println!("Peers: {}", peers.len());
    for peer in peers {
        let short_peer_id = &peer.peer_id[..peer.peer_id.len().min(8)];
        let name = match &peer.alias {
            Some(alias) => format!("{} [{}]", peer.name, alias),
            None => peer.name.clone(),
        };
        let head_text = peer
            .head
            .as_ref()
            .map(|head| format!(" head {}", &head[..head.len().min(8)]))
            .unwrap_or_default();
        let seen_text = peer
            .last_seen
            .map(format_last_seen)
            .unwrap_or_else(|| "last seen unknown".to_string());
        let stale = if peer_is_stale(peer) { " stale" } else { "" };
        println!(
            "  {} ({}) @ {}{} [{}{}]",
            name,
            short_peer_id,
            peer.addr,
            head_text,
            seen_text,
            stale
        );
    }
}

fn write_cached_peers(rvc: &Path, peers: &[CachedPeer]) -> Result<()> {
    let mut lines: Vec<String> = peers
        .iter()
        .map(|peer| {
            format!(
                "{}|{}|{}|{}|{}",
                peer.name,
                peer.peer_id,
                peer.addr,
                peer.head.clone().unwrap_or_default(),
                peer.last_seen.map(|ts| ts.to_string()).unwrap_or_default()
            )
        })
        .collect();
    lines.sort();
    fs::write(rvc.join("peers"), lines.join("\n")).context("write peers cache")
}

fn merge_cached_peers(rvc: &Path, discovered: &[CachedPeer]) -> Result<Vec<CachedPeer>> {
    let mut map: HashMap<String, CachedPeer> = read_cached_peers(rvc)?
        .into_iter()
        .map(|peer| (peer.peer_id.clone(), peer))
        .collect();
    for peer in discovered {
        map.insert(peer.peer_id.clone(), peer.clone());
    }
    let mut peers: Vec<CachedPeer> = map.into_values().collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name).then(a.peer_id.cmp(&b.peer_id)));
    write_cached_peers(rvc, &peers)?;
    Ok(peers)
}

fn resolve_peer_query(peers: &[CachedPeer], query: &str) -> Result<CachedPeer> {
    let query_lower = query.to_lowercase();
    if let Some(peer) = peers.iter().find(|peer| peer.addr == query) {
        return Ok(peer.clone());
    }
    if let Some(peer) = peers.iter().find(|peer| peer.alias.as_deref() == Some(query)) {
        return Ok(peer.clone());
    }

    if let Some(peer) = peers.iter().find(|peer| peer.peer_id == query) {
        return Ok(peer.clone());
    }

    let peer_id_prefix_matches: Vec<CachedPeer> = peers
        .iter()
        .filter(|peer| peer.peer_id.starts_with(query))
        .cloned()
        .collect();
    if peer_id_prefix_matches.len() == 1 {
        return Ok(peer_id_prefix_matches[0].clone());
    }

    let exact_name_matches: Vec<CachedPeer> = peers
        .iter()
        .filter(|peer| peer.name == query)
        .cloned()
        .collect();
    if exact_name_matches.len() == 1 {
        return Ok(exact_name_matches[0].clone());
    }
    if exact_name_matches.len() > 1 {
        let ids = exact_name_matches
            .iter()
            .map(|peer| peer.peer_id[..peer.peer_id.len().min(8)].to_string())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!("peer name `{query}` is ambiguous; use an alias or peer id ({ids})");
    }

    let fuzzy_matches: Vec<CachedPeer> = peers
        .iter()
        .filter(|peer| {
            peer.name.to_lowercase().contains(&query_lower)
                || peer.peer_id.to_lowercase().starts_with(&query_lower)
                || peer
                    .alias
                    .as_ref()
                    .map(|alias| alias.to_lowercase().contains(&query_lower))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();
    if fuzzy_matches.len() == 1 {
        return Ok(fuzzy_matches[0].clone());
    }
    if fuzzy_matches.len() > 1 {
        let names = fuzzy_matches
            .iter()
            .map(|peer| match &peer.alias {
                Some(alias) => format!("{} [{}]", peer.name, alias),
                None => peer.name.clone(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!("peer query `{query}` matched multiple peers: {names}");
    }

    anyhow::bail!("unknown peer `{query}`; run `rvc peers` to list cached peers")
}

fn read_peer_aliases(rvc: &Path) -> Result<HashMap<String, String>> {
    let path = peer_aliases_path(rvc);
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let raw = fs::read_to_string(&path).context("read peer aliases")?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once('='))
        .map(|(peer_id, alias)| (peer_id.to_string(), alias.to_string()))
        .collect())
}

fn write_peer_aliases(rvc: &Path, aliases: &HashMap<String, String>) -> Result<()> {
    let mut lines: Vec<String> = aliases
        .iter()
        .map(|(peer_id, alias)| format!("{peer_id}={alias}"))
        .collect();
    lines.sort();
    fs::write(peer_aliases_path(rvc), lines.join("\n")).context("write peer aliases")
}

fn set_peer_alias(rvc: &Path, peer_id: &str, alias: &str) -> Result<()> {
    let mut aliases = read_peer_aliases(rvc)?;
    if let Some((existing_peer_id, _)) = aliases
        .iter()
        .find(|(existing_peer_id, existing_alias)| existing_alias == &alias && existing_peer_id.as_str() != peer_id)
    {
        anyhow::bail!("alias `{alias}` is already assigned to peer {}", &existing_peer_id[..existing_peer_id.len().min(8)]);
    }
    aliases.insert(peer_id.to_string(), alias.to_string());
    write_peer_aliases(rvc, &aliases)
}

fn now_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}

fn format_last_seen(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "last seen invalid".to_string())
}

fn peer_is_stale(peer: &CachedPeer) -> bool {
    peer.last_seen
        .map(|timestamp| now_timestamp().saturating_sub(timestamp) > 300)
        .unwrap_or(true)
}

// ─── Config helpers ──────────────────────────────────────────────────────────

fn read_author(rvc: &Path) -> Result<String> {
    Ok(read_config_value(rvc, "author")?.unwrap_or_else(default_author))
}

fn read_peer_name(rvc: &Path) -> Result<String> {
    Ok(read_config_value(rvc, "peer_name")?.unwrap_or_else(default_peer_name))
}

// ─── Working tree snapshot ───────────────────────────────────────────────────

/// Walk the working directory (ignoring `.rvc/`), store each file as a Blob,
/// collect entries into a Tree, store the Tree, and return its hash.
fn snapshot_tree(rvc: &Path) -> Result<String> {
    // The working dir is the parent of `.rvc/`
    let workdir = rvc.parent().context("rvc dir has no parent")?;
    let mut entries: Vec<TreeEntry> = Vec::new();

    collect_files(workdir, workdir, rvc, &mut entries)?;

    // Sort deterministically
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    let tree = Tree { entries };
    write_object(rvc, &tree)
}

fn collect_files(
    base: &Path,
    dir: &Path,
    rvc: &Path,
    entries: &mut Vec<TreeEntry>,
) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read dir {:?}", dir))? {
        let entry = entry?;
        let path = entry.path();

        // Skip .rvc and hidden directories
        let fname = entry.file_name();
        let fname_str = fname.to_string_lossy();
        if fname_str.starts_with('.') {
            continue;
        }

        if path.is_dir() {
            collect_files(base, &path, rvc, entries)?;
        } else if path.is_file() {
            let content = fs::read(&path)
                .with_context(|| format!("read file {:?}", path))?;
            let blob = Blob { content };
            let hash = write_object(rvc, &blob)?;

            // Relative path from working directory root
            let rel = path
                .strip_prefix(base)
                .with_context(|| format!("strip prefix {:?}", base))?;
            entries.push(TreeEntry {
                name: rel.to_string_lossy().to_string(),
                hash,
            });
        }
    }
    Ok(())
}

fn tree_paths(rvc: &Path, tree_hash: Option<&str>) -> Result<Vec<String>> {
    let Some(hash) = tree_hash else {
        return Ok(Vec::new());
    };

    Ok(read_tree(rvc, hash)?
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect())
}

fn contains_conflict_markers(content: &[u8]) -> bool {
    [
        b"<<<<<<< LOCAL".as_slice(),
        b"=======".as_slice(),
        b">>>>>>> REMOTE".as_slice(),
    ]
    .iter()
    .any(|needle| content.windows(needle.len()).any(|window| window == *needle))
}

fn prune_empty_dirs(mut dir: PathBuf, stop_at: &Path) -> Result<()> {
    while dir.starts_with(stop_at) && dir != stop_at {
        match fs::read_dir(&dir) {
            Ok(entries) => {
                let mut entries = entries;
                if entries.next().is_some() {
                    break;
                }
                fs::remove_dir(&dir)
                    .with_context(|| format!("remove empty directory {}", dir.display()))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => return Err(err).with_context(|| format!("read directory {}", dir.display())),
        }
        if !dir.pop() {
            break;
        }
    }
    Ok(())
}

fn materialize_tree(rvc: &Path, tree_hash: &str, previous_tree_hash: Option<&str>) -> Result<()> {
    let workdir = rvc.parent().context("rvc dir has no parent")?;
    let previous = tree_paths(rvc, previous_tree_hash)?;
    let previous_set: HashSet<String> = previous.iter().cloned().collect();
    let tree = read_tree(rvc, tree_hash)?;

    for path in previous {
        if tree.entries.iter().any(|entry| entry.name == path) {
            continue;
        }
        let full_path = workdir.join(&path);
        if full_path.exists() {
            if full_path.is_file() {
                fs::remove_file(&full_path)
                    .with_context(|| format!("remove stale file {}", full_path.display()))?;
                if let Some(parent) = full_path.parent() {
                    prune_empty_dirs(parent.to_path_buf(), workdir)?;
                }
            }
        }
    }

    for entry in tree.entries {
        let blob = read_blob(rvc, &entry.hash)?;
        let full_path = workdir.join(&entry.name);
        if full_path.exists() && !previous_set.contains(&entry.name) {
            if full_path.is_dir() {
                anyhow::bail!("refusing to overwrite untracked directory {}", full_path.display());
            }
            let existing = fs::read(&full_path)
                .with_context(|| format!("read existing file {}", full_path.display()))?;
            if existing != blob.content {
                anyhow::bail!("refusing to overwrite untracked file {}", full_path.display());
            }
        }
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create parent directory {}", parent.display()))?;
        }
        fs::write(&full_path, blob.content)
            .with_context(|| format!("write file {}", full_path.display()))?;
    }

    Ok(())
}

// ─── Commands ────────────────────────────────────────────────────────────────

fn cmd_init() -> Result<()> {
    let cwd = std::env::current_dir()?;
    let rvc = cwd.join(".rvc");

    if rvc.exists() {
        println!("Repository already exists at {:?}", rvc);
        return Ok(());
    }

    fs::create_dir_all(rvc.join("objects")).context("create objects dir")?;
    fs::create_dir_all(rvc.join("refs")).context("create refs dir")?;

    // Write a default config
    let mut config = BTreeMap::new();
    config.insert("author".to_string(), default_author());
    config.insert("peer_name".to_string(), default_peer_name());
    write_config_map(&rvc, &config)?;

    println!("Initialised empty rvc repository in {}", rvc.display());
    Ok(())
}

fn cmd_commit(message: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    let author = read_author(&rvc)?;
    let merge_conflicts = read_merge_conflicts(&rvc)?;
    anyhow::ensure!(
        merge_conflicts.is_empty(),
        "merge has unresolved conflicts; run `rvc conflicts` and `rvc resolve` first"
    );

    // Snapshot working tree → blob objects + tree object
    let tree_hash = snapshot_tree(&rvc).context("snapshot working tree")?;

    // Get current HEAD as parent
    let mut parent_hashes = match read_head(&rvc)? {
        Some(h) => vec![h],
        None => vec![],
    };
    if let Some(merge_head) = read_merge_head(&rvc)? {
        parent_hashes.push(merge_head);
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs() as i64;

    let commit = Commit {
        tree_hash,
        parent_hashes,
        message: message.to_string(),
        timestamp,
        author,
    };

    let hash = store_commit(&rvc, &commit).context("store commit")?;
    write_head(&rvc, &hash).context("update HEAD")?;
    clear_merge_state(&rvc)?;

    println!("[{}] {}", &hash[..8], message);
    Ok(())
}

fn cmd_log() -> Result<()> {
    let rvc = find_rvc_root()?;

    let mut current = read_head(&rvc).context("read HEAD")?;

    if current.is_none() {
        println!("No commits yet.");
        return Ok(());
    }

    while let Some(hash) = current {
        let commit = read_commit(&rvc, &hash)
            .with_context(|| format!("read commit {hash}"))?;

        // Human-readable timestamp
        let dt = chrono::DateTime::from_timestamp(commit.timestamp, 0)
            .unwrap_or_default()
            .format("%Y-%m-%d %H:%M:%S UTC");

        println!("commit {}", &hash[..8]);
        println!("Author: {}", commit.author);
        println!("Date:   {dt}");
        println!();
        println!("    {}", commit.message);
        println!();

        // Walk to first parent (linear history)
        current = commit.parent_hashes.into_iter().next();
    }

    Ok(())
}

#[derive(NetworkBehaviour)]
struct RvcSyncBehaviour {
    rr: request_response::Behaviour<RvcCodec>,
}

#[derive(NetworkBehaviour)]
struct RvcDiscoverBehaviour {
    mdns: mdns::tokio::Behaviour,
    rr: request_response::Behaviour<RvcCodec>,
}

async fn cmd_sync(peer_addr: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    let addr: Multiaddr = peer_addr.parse().context("invalid peer multiaddr")?;
    cmd_sync_addr(&rvc, addr).await
}

async fn cmd_sync_peer(name: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    let peers = read_cached_peers(&rvc)?;
    let peer = resolve_peer_query(&peers, name)?;
    anyhow::ensure!(
        !peer.addr.contains("/ip4/0.0.0.0/"),
        "cached peer `{name}` does not have a dialable address yet; wait for discovery to settle and try again"
    );
    if peer_is_stale(&peer) {
        println!("Warning: cached peer `{}` is stale; attempting sync anyway.", peer.name);
    }
    let addr: Multiaddr = peer
        .addr
        .parse()
        .with_context(|| format!("invalid cached multiaddr for peer `{name}`"))?;
    println!("Using cached peer `{}` at {}", peer.name, peer.addr);
    cmd_sync_addr(&rvc, addr).await
}

async fn cmd_discover() -> Result<()> {
    let rvc = find_rvc_root()?;
    let my_name = read_peer_name(&rvc)?;
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key| {
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                key.public().to_peer_id(),
            )?;
            let rr = request_response::Behaviour::new(
                [(RVC_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default().with_request_timeout(Duration::from_secs(30)),
            );
            Ok(RvcDiscoverBehaviour { mdns, rr })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let mut found: HashMap<PeerId, CachedPeer> = HashMap::new();
    let mut pending: HashSet<PeerId> = HashSet::new();
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?).context("listen")?;

    let deadline = time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut listener_up = false;

    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => break,
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { .. } => listener_up = true,
                SwarmEvent::Behaviour(RvcDiscoverBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    if listener_up {
                        for (peer_id, addr) in list {
                            if peer_id == *swarm.local_peer_id() { continue; }
                            swarm.add_peer_address(peer_id, addr.clone());
                            pending.insert(peer_id);
                            found.entry(peer_id).or_insert(CachedPeer {
                                name: peer_id.to_string(),
                                alias: None,
                                peer_id: peer_id.to_string(),
                                addr: addr.to_string(),
                                head: None,
                                last_seen: Some(now_timestamp()),
                            });
                            swarm.behaviour_mut().rr.send_request(
                                &peer_id,
                                RvcRequest::Handshake {
                                    name: my_name.clone(),
                                    head_hash: read_head(&rvc)?,
                                }
                            );
                        }
                    }
                }
                SwarmEvent::Behaviour(RvcDiscoverBehaviourEvent::Rr(request_response::Event::Message { peer, message })) => {
                    match message {
                        request_response::Message::Request { request, channel, .. } => {
                            if let RvcRequest::Handshake { name, head_hash } = request {
                                let existing_addr = found
                                    .get(&peer)
                                    .map(|cached| cached.addr.clone())
                                    .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string());
                                found.insert(peer, CachedPeer {
                                    name,
                                    alias: None,
                                    peer_id: peer.to_string(),
                                    addr: existing_addr,
                                    head: head_hash,
                                    last_seen: Some(now_timestamp()),
                                });
                                pending.remove(&peer);
                                let _ = swarm.behaviour_mut().rr.send_response(
                                    channel,
                                    RvcResponse::HandshakeAck {
                                        name: my_name.clone(),
                                        head_hash: read_head(&rvc)?,
                                    },
                                );
                            }
                        }
                        request_response::Message::Response { response, .. } => {
                            if let RvcResponse::HandshakeAck { name, head_hash } = response {
                                let existing_addr = found
                                    .get(&peer)
                                    .map(|cached| cached.addr.clone())
                                    .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string());
                                found.insert(peer, CachedPeer {
                                    name,
                                    alias: None,
                                    peer_id: peer.to_string(),
                                    addr: existing_addr,
                                    head: head_hash,
                                    last_seen: Some(now_timestamp()),
                                });
                                pending.remove(&peer);
                            }
                        }
                    }
                }
                SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    let addr = endpoint.get_remote_address().to_string();
                    if let Some(peer) = found.get_mut(&peer_id) {
                        peer.addr = addr;
                        peer.last_seen = Some(now_timestamp());
                    }
                }
                SwarmEvent::Behaviour(RvcDiscoverBehaviourEvent::Rr(request_response::Event::OutboundFailure { peer, .. })) => {
                    pending.remove(&peer);
                }
                _ => {}
            }
        }
    }

    let peers: Vec<CachedPeer> = found.into_values().collect();
    let merged = merge_cached_peers(&rvc, &peers)?;
    let display: Vec<CachedPeer> = merged.into_iter().filter(|peer| peers.iter().any(|found| found.peer_id == peer.peer_id)).collect();
    println!("peers found:");
    if display.is_empty() {
        println!("(none)");
    } else {
        for peer in &display {
            let label = peer.alias.as_ref().unwrap_or(&peer.name);
            println!("{label}");
        }
    }
    Ok(())
}

async fn cmd_sync_addr(rvc: &Path, addr: Multiaddr) -> Result<()> {
    let author = read_author(&rvc)?;
    let previous_head = read_head(&rvc)?;
    let previous_tree_hash = previous_head
        .as_deref()
        .map(|hash| read_commit(&rvc, hash))
        .transpose()?
        .map(|commit| commit.tree_hash);

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let rr_config = request_response::Config::default()
        .with_request_timeout(Duration::from_secs(30));
    
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_behaviour(|_| {
            let rr = request_response::Behaviour::new(
                [(RVC_PROTOCOL, request_response::ProtocolSupport::Full)],
                rr_config,
            );
            Ok(RvcSyncBehaviour { rr })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.dial(addr.clone()).context("failed to dial peer")?;

    // Wait for connection
    println!("Connecting to peer...");
    let peer_id = loop {
        tokio::select! {
            _ = time::sleep(Duration::from_secs(5)) => anyhow::bail!("timeout connecting to peer"),
            event = swarm.select_next_some() => {
                match &event {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        println!("Connected to {}", peer_id);
                        break *peer_id;
                    }
                    SwarmEvent::OutgoingConnectionError { .. } => {}
                    _ => {}
                }
            }
        }
    };

    // Round 1: GetHead
    let req_id = swarm.behaviour_mut().rr.send_request(
        &peer_id, RvcRequest::Sync(SyncRequest::GetHead),
    );
    let remote_head = loop {
        tokio::select! {
            _ = time::sleep(Duration::from_secs(5)) => anyhow::bail!("timeout waiting for GetHead response"),
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(RvcSyncBehaviourEvent::Rr(request_response::Event::Message { message: request_response::Message::Response { request_id, response }, .. })) = event {
                    if request_id == req_id {
                        if let RvcResponse::Sync(SyncResponse::Head(hash)) = response {
                            break hash;
                        }
                    }
                }
            }
        }
    };
    let local_head = read_head(&rvc)?.unwrap_or_default();

    if remote_head == local_head {
        println!("Already up to date.");
        return Ok(());
    }

    // Round 2: GetMissingFrom
    let req_id = swarm.behaviour_mut().rr.send_request(
        &peer_id, RvcRequest::Sync(SyncRequest::GetMissingFrom(local_head.clone())),
    );
    let hashes = loop {
        tokio::select! {
            _ = time::sleep(Duration::from_secs(10)) => anyhow::bail!("timeout waiting for GetMissingFrom response"),
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(RvcSyncBehaviourEvent::Rr(request_response::Event::Message { message: request_response::Message::Response { request_id, response }, .. })) = event {
                    if request_id == req_id {
                        if let RvcResponse::Sync(SyncResponse::MissingHashes(hashes)) = response {
                            break hashes;
                        }
                    }
                }
            }
        }
    };
    if hashes.is_empty() {
        println!("Already up to date.");
        return Ok(());
    }

    // Round 3: GetObjects
    let req_id = swarm.behaviour_mut().rr.send_request(
        &peer_id, RvcRequest::Sync(SyncRequest::GetObjects(hashes)),
    );
    let objects = loop {
        tokio::select! {
            _ = time::sleep(Duration::from_secs(30)) => anyhow::bail!("timeout waiting for GetObjects response"),
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(RvcSyncBehaviourEvent::Rr(request_response::Event::Message { message: request_response::Message::Response { request_id, response }, .. })) = event {
                    if request_id == req_id {
                        if let RvcResponse::Sync(SyncResponse::Objects(objs)) = response {
                            break objs;
                        }
                    }
                }
            }
        }
    };
    
    // Attempt to parse objects to count commits (heuristic)
    let mut commit_count = 0;
    for (expected_hash, compressed_bytes) in objects {
        rvc_core::sync::save_raw_object(&rvc, &expected_hash, &compressed_bytes)
            .context("save missing object")?;
        
        // Very basic heuristic: if it deserializes as a commit successfully, count it
        if rvc_core::read_commit(&rvc, &expected_hash).is_ok() {
            commit_count += 1;
        }
    }

    if local_head.is_empty() {
        let remote_commit = read_commit(&rvc, &remote_head)?;
        materialize_tree(&rvc, &remote_commit.tree_hash, previous_tree_hash.as_deref())?;
        write_head(&rvc, &remote_head)?;
        clear_merge_state(&rvc)?;
        println!(
            "Received {} commit(s). Fast-forwarded to {}",
            commit_count,
            &remote_head[..8]
        );
        return Ok(());
    }

    match rvc_core::merge_commits(&rvc, &local_head, &remote_head, &author)? {
        MergeResult::AlreadyUpToDate => {
            println!("Already up to date.");
        }
        MergeResult::FastForward { new_head, tree_hash } => {
            materialize_tree(&rvc, &tree_hash, previous_tree_hash.as_deref())?;
            write_head(&rvc, &new_head)?;
            clear_merge_state(&rvc)?;
            println!(
                "Received {} commit(s). Fast-forwarded to {}",
                commit_count,
                &new_head[..8]
            );
        }
        MergeResult::MergeSuccess { new_head, tree_hash } => {
            materialize_tree(&rvc, &tree_hash, previous_tree_hash.as_deref())?;
            write_head(&rvc, &new_head)?;
            clear_merge_state(&rvc)?;
            println!("Merged remote changes. New HEAD is {}", &new_head[..8]);
        }
        MergeResult::Conflicts { tree_hash, files } => {
            materialize_tree(&rvc, &tree_hash, previous_tree_hash.as_deref())?;
            let conflict_paths: Vec<String> = files.iter().map(|file| file.path.clone()).collect();
            write_merge_state(&rvc, &remote_head, &tree_hash, &conflict_paths)?;
            println!("Merge conflicts in:");
            for path in conflict_paths {
                println!("  {}", path);
            }
            println!("Conflict markers written to the working tree. Resolve them and run `rvc commit`.");
        }
    }

    Ok(())
}

fn cmd_status() -> Result<()> {
    let rvc = find_rvc_root()?;

    match read_head(&rvc)? {
        Some(hash) => {
            let commit = read_commit(&rvc, &hash)?;
            println!("HEAD: {} — {}", &hash[..8], commit.message);

            // Count commits in chain
            let mut count = 1usize;
            let mut cur = commit.parent_hashes.into_iter().next();
            while let Some(h) = cur {
                count += 1;
                let c = read_commit(&rvc, &h)?;
                cur = c.parent_hashes.into_iter().next();
            }
            println!("History: {count} commit(s)");
        }
        None => println!("HEAD: (no commits yet)"),
    }

    if let Some(merge_head) = read_merge_head(&rvc)? {
        println!("Merge: in progress with {}", &merge_head[..merge_head.len().min(8)]);
        let conflicts = read_merge_conflicts(&rvc)?;
        if conflicts.is_empty() {
            println!("Conflicts: none recorded");
        } else {
            println!("Conflicts: {}", conflicts.len());
            for path in conflicts {
                println!("  {path}");
            }
        }
    }

    let peers = read_cached_peers(&rvc)?;
    print_cached_peers(&peers);

    Ok(())
}

fn cmd_peers() -> Result<()> {
    let rvc = find_rvc_root()?;
    let peers = read_cached_peers(&rvc)?;
    print_cached_peers(&peers);
    Ok(())
}

fn cmd_conflicts() -> Result<()> {
    let rvc = find_rvc_root()?;

    let Some(merge_head) = read_merge_head(&rvc)? else {
        println!("No merge in progress.");
        return Ok(());
    };

    let conflicts = read_merge_conflicts(&rvc)?;
    println!(
        "Merge in progress with {}",
        &merge_head[..merge_head.len().min(8)]
    );
    if conflicts.is_empty() {
        println!("No conflicted files recorded.");
    } else {
        println!("Conflicted files:");
        for path in conflicts {
            println!("  {path}");
        }
    }
    println!("Resolve the files and run `rvc commit` to finish the merge.");

    Ok(())
}

fn cmd_whoami() -> Result<()> {
    let rvc = find_rvc_root()?;
    let author = read_author(&rvc)?;
    let peer_name = read_peer_name(&rvc)?;
    println!("Author: {author}");
    println!("Peer:   {peer_name}");
    Ok(())
}

fn cmd_set_author(name: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    write_config_value(&rvc, "author", name)?;
    println!("Updated author to {name}");
    Ok(())
}

fn cmd_set_peer_name(name: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    let peers = read_cached_peers(&rvc)?;
    if peers.iter().any(|peer| peer.name == name) {
        anyhow::bail!("peer name `{name}` is already used by a cached peer; choose a unique name");
    }
    write_config_value(&rvc, "peer_name", name)?;
    println!("Updated peer name to {name}");
    Ok(())
}

fn cmd_alias_peer(peer_query: &str, alias: &str) -> Result<()> {
    let rvc = find_rvc_root()?;
    let peers = read_cached_peers(&rvc)?;
    let peer = resolve_peer_query(&peers, peer_query)?;
    set_peer_alias(&rvc, &peer.peer_id, alias)?;
    println!(
        "Set alias `{alias}` for peer {} ({})",
        peer.name,
        &peer.peer_id[..peer.peer_id.len().min(8)]
    );
    Ok(())
}

fn cmd_resolve(paths: &[String]) -> Result<()> {
    let rvc = find_rvc_root()?;
    let merge_head = read_merge_head(&rvc)?;
    anyhow::ensure!(merge_head.is_some(), "no merge in progress");

    let mut conflicts = read_merge_conflicts(&rvc)?;
    let targets: Vec<String> = if paths.is_empty() {
        conflicts.clone()
    } else {
        paths.to_vec()
    };
    anyhow::ensure!(!targets.is_empty(), "no conflicted files recorded");

    let workdir = rvc.parent().context("rvc dir has no parent")?;
    for target in &targets {
        anyhow::ensure!(
            conflicts.iter().any(|path| path == target),
            "`{target}` is not a recorded conflicted file"
        );
        let full_path = workdir.join(target);
        let content = fs::read(&full_path)
            .with_context(|| format!("read conflicted file {}", full_path.display()))?;
        anyhow::ensure!(
            !contains_conflict_markers(&content),
            "`{target}` still contains conflict markers"
        );
    }

    conflicts.retain(|path| !targets.iter().any(|target| target == path));
    if let Some(merge_head) = merge_head {
        let merge_tree = read_merge_tree(&rvc)?.context("MERGE_TREE missing during merge")?;
        write_merge_state(&rvc, &merge_head, &merge_tree, &conflicts)?;
    }

    if conflicts.is_empty() {
        println!("All conflicts resolved. Run `rvc commit` to finish the merge.");
    } else {
        println!("Marked {} file(s) as resolved.", targets.len());
    }
    Ok(())
}

fn cmd_abort_merge() -> Result<()> {
    let rvc = find_rvc_root()?;
    anyhow::ensure!(read_merge_head(&rvc)?.is_some(), "no merge in progress");
    let local_head = read_head(&rvc)?.context("local HEAD missing during merge")?;
    let local_commit = read_commit(&rvc, &local_head)?;
    let merge_tree = read_merge_tree(&rvc)?.or_else(|| Some(local_commit.tree_hash.clone()));
    materialize_tree(&rvc, &local_commit.tree_hash, merge_tree.as_deref())?;
    clear_merge_state(&rvc)?;
    println!("Aborted merge. Restored working tree to {}", &local_head[..8]);
    Ok(())
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Commit { message } => cmd_commit(&message),
        Commands::Log => cmd_log(),
        Commands::Sync { peer } => cmd_sync(&peer).await,
        Commands::SyncPeer { name } => cmd_sync_peer(&name).await,
        Commands::Status => cmd_status(),
        Commands::Discover => cmd_discover().await,
        Commands::Peers => cmd_peers(),
        Commands::Conflicts => cmd_conflicts(),
        Commands::Whoami => cmd_whoami(),
        Commands::SetAuthor { name } => cmd_set_author(&name),
        Commands::SetPeerName { name } => cmd_set_peer_name(&name),
        Commands::AliasPeer { peer, alias } => cmd_alias_peer(&peer, &alias),
        Commands::Resolve { paths } => cmd_resolve(&paths),
        Commands::AbortMerge => cmd_abort_merge(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_rvc() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::create_dir_all(dir.path().join(".rvc").join("refs")).expect("create refs");
        dir
    }

    #[test]
    fn malformed_config_lines_are_ignored() {
        let dir = temp_rvc();
        let rvc = dir.path().join(".rvc");
        std::fs::write(
            rvc.join("config"),
            "author=alice\nnot-a-valid-line\npeer_name=peer-a\nbroken=\n",
        )
        .expect("write config");

        let config = read_config_map(&rvc).expect("read config");
        assert_eq!(config.get("author").map(String::as_str), Some("alice"));
        assert_eq!(config.get("peer_name").map(String::as_str), Some("peer-a"));
        assert!(!config.contains_key("not-a-valid-line"));
    }

    #[test]
    fn malformed_peer_cache_lines_are_skipped() {
        let dir = temp_rvc();
        let rvc = dir.path().join(".rvc");
        std::fs::write(
            rvc.join("peers"),
            "ok|peer1|/ip4/127.0.0.1/tcp/1||123\nbroken-line\nmissing|parts\n",
        )
        .expect("write peers");

        let peers = read_cached_peers(&rvc).expect("read peers");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].name, "ok");
    }

    #[test]
    fn duplicate_peer_names_require_disambiguation() {
        let peers = vec![
            CachedPeer {
                name: "peer-a".into(),
                alias: None,
                peer_id: "peer1".into(),
                addr: "/ip4/127.0.0.1/tcp/1".into(),
                head: None,
                last_seen: Some(now_timestamp()),
            },
            CachedPeer {
                name: "peer-a".into(),
                alias: None,
                peer_id: "peer2".into(),
                addr: "/ip4/127.0.0.1/tcp/2".into(),
                head: None,
                last_seen: Some(now_timestamp()),
            },
        ];

        let err = resolve_peer_query(&peers, "peer-a").expect_err("expected ambiguity");
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn alias_resolution_and_stale_detection_work() {
        let stale_peer = CachedPeer {
            name: "peer-a".into(),
            alias: Some("laptop".into()),
            peer_id: "peer1".into(),
            addr: "/ip4/127.0.0.1/tcp/1".into(),
            head: None,
            last_seen: Some(now_timestamp() - 3600),
        };
        assert!(peer_is_stale(&stale_peer));

        let resolved = resolve_peer_query(&[stale_peer], "laptop").expect("resolve alias");
        assert_eq!(resolved.peer_id, "peer1");
    }
}
