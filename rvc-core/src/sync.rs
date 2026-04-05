use crate::{read_commit, read_object, hash_object, Tree};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    GetHead,
    GetMissingFrom(String),      // caller's HEAD hash
    GetObjects(Vec<String>),     // list of hashes to fetch
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncResponse {
    Head(String),
    MissingHashes(Vec<String>),
    Objects(Vec<(String, Vec<u8>)>),  // (hash, compressed_bytes)
}

fn read_head(root: &Path) -> Option<String> {
    let head_path = root.join("refs").join("HEAD");
    if head_path.exists() {
        if let Ok(content) = fs::read_to_string(&head_path) {
            let hash = content.trim().to_string();
            if !hash.is_empty() {
                return Some(hash);
            }
        }
    }
    None
}

/// Given a hash that the remote (caller) has as HEAD, walk the local object store
/// backwards from our HEAD through parent_hashes[0] and return all hashes in that chain
/// that do NOT exist in the caller's object store (i.e. hashes we have that they want).
/// Stop when you hit a hash that exists locally (that's the common ancestor) or exhaust the chain.
pub fn find_missing(remote_head: &str, root: &Path) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    let mut current_hash = match read_head(root) {
        Some(h) => h,
        None => return Ok(missing), // our store is empty
    };

    while current_hash != remote_head {
        if current_hash.is_empty() {
            break;
        }

        // Try to read our local commit
        let commit = match read_commit(root, &current_hash) {
            Ok(c) => c,
            Err(_) => break, // Reached end of valid local chain
        };

        // Add commit hash
        missing.push(current_hash.clone());

        // Add tree hash
        missing.push(commit.tree_hash.clone());

        // Add blob hashes from tree
        if let Ok(tree) = read_object::<Tree>(root, &commit.tree_hash) {
            for entry in tree.entries {
                missing.push(entry.hash);
            }
        }

        if commit.parent_hashes.is_empty() {
            break;
        }
        current_hash = commit.parent_hashes[0].clone();
    }

    // We collected from newest to oldest. A sync is usually better oldest to newest.
    // The prompt just says "return all hashes in that chain".
    // We reverse so that objects dependencies (blobs, trees) and chronological commits
    // are ordered logically for the caller.
    missing.reverse();
    // Also deduplicate while preserving order (e.g. blobs unchanged between commits)
    let mut deduped = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for hash in missing {
        if seen.insert(hash.clone()) {
            deduped.push(hash);
        }
    }

    Ok(deduped)
}

/// Read raw gzipped bytes of an object without decompressing or deserializing
pub fn read_raw_object(store_root: &Path, hash: &str) -> Result<Vec<u8>> {
    let (prefix, rest) = hash.split_at(2);
    let path = store_root.join("objects").join(prefix).join(rest);
    fs::read(&path).with_context(|| format!("read raw object file for hash {hash}"))
}

/// Save raw gzipped bytes to the object store
pub fn save_raw_object(store_root: &Path, expected_hash: &str, compressed_bytes: &[u8]) -> Result<()> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    
    // First, verify the hash. The hash is of the UNCOMPRESSED JSON bytes.
    let mut decoder = GzDecoder::new(compressed_bytes);
    let mut uncompressed = Vec::new();
    decoder.read_to_end(&mut uncompressed).context("decompress object for verification")?;
    
    let actual_hash = hash_object(&uncompressed);
    anyhow::ensure!(
        actual_hash == expected_hash,
        "hash mismatch during sync: expected {expected_hash}, got {actual_hash}"
    );

    let (prefix, rest) = expected_hash.split_at(2);
    let obj_dir = store_root.join("objects").join(prefix);
    if !obj_dir.exists() {
        fs::create_dir_all(&obj_dir).context("create objects directory")?;
    }
    
    let path = obj_dir.join(rest);
    if !path.exists() {
        fs::write(&path, compressed_bytes).context("write received object file")?;
    }
    Ok(())
}
