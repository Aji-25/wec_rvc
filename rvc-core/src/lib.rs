use anyhow::{Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub mod sync;
pub mod protocol;


// ─── Data Model ──────────────────────────────────────────────────────────────

/// Raw file content stored content-addressed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    pub content: Vec<u8>,
}

/// A single entry in a Tree (name → hash of blob/subtree).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub hash: String,
}

/// A directory snapshot: ordered list of (name, hash) pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    pub entries: Vec<TreeEntry>,
}

/// A version-control commit object.
/// parent_hashes holds:
///   - 0 entries  → initial commit (no parent)
///   - 1 entry    → normal commit
///   - 2 entries  → merge commit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub tree_hash: String,
    pub parent_hashes: Vec<String>,
    pub message: String,
    /// Unix timestamp (seconds since epoch).
    pub timestamp: i64,
    pub author: String,
}

#[derive(Debug, Clone)]
pub struct ConflictFile {
    pub path: String,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum MergeResult {
    FastForward { new_head: String, tree_hash: String },
    AlreadyUpToDate,
    MergeSuccess { new_head: String, tree_hash: String },
    Conflicts { tree_hash: String, files: Vec<ConflictFile> },
}

// ─── Hashing ─────────────────────────────────────────────────────────────────

/// Hash arbitrary bytes with SHA-256, returning a lowercase hex string.
pub fn hash_object(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

// ─── Object Store ─────────────────────────────────────────────────────────────

/// Return the path for a given hash under `store_root/objects/`.
/// Mirrors Git: first two hex chars form the directory, the rest is the filename.
fn object_path(store_root: &Path, hash: &str) -> PathBuf {
    let (prefix, rest) = hash.split_at(2);
    store_root.join("objects").join(prefix).join(rest)
}

/// Serialize `data` to JSON, gzip-compress it, write to content-addressed path.
/// Returns the hex SHA-256 hash of the **uncompressed** JSON bytes.
pub fn write_object<T: Serialize>(store_root: &Path, value: &T) -> Result<String> {
    let json = serde_json::to_vec(value).context("serialize object to JSON")?;
    let hash = hash_object(&json);

    let path = object_path(store_root, &hash);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("create object directory")?;
        }
        // Gzip-compress before writing
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json).context("compress object")?;
        let compressed = encoder.finish().context("finish compression")?;
        fs::write(&path, &compressed).context("write object file")?;
    }

    Ok(hash)
}

/// Read and decompress an object file, then deserialize into `T`.
pub fn read_object<T: for<'de> Deserialize<'de>>(store_root: &Path, hash: &str) -> Result<T> {
    let path = object_path(store_root, hash);
    let compressed = fs::read(&path)
        .with_context(|| format!("read object file for hash {hash}"))?;

    let mut decoder = GzDecoder::new(compressed.as_slice());
    let mut json = Vec::new();
    decoder
        .read_to_end(&mut json)
        .context("decompress object")?;

    // Verify hash integrity
    let actual_hash = hash_object(&json);
    anyhow::ensure!(
        actual_hash == hash,
        "hash mismatch: expected {hash}, got {actual_hash}"
    );

    serde_json::from_slice(&json).context("deserialize object from JSON")
}

// ─── Typed Wrappers ───────────────────────────────────────────────────────────

/// Store a `Commit` and return its content hash.
pub fn store_commit(store_root: &Path, commit: &Commit) -> Result<String> {
    write_object(store_root, commit)
}

/// Read a `Commit` by its hash.
pub fn read_commit(store_root: &Path, hash: &str) -> Result<Commit> {
    read_object::<Commit>(store_root, hash)
}

pub fn read_tree(store_root: &Path, hash: &str) -> Result<Tree> {
    read_object::<Tree>(store_root, hash)
}

pub fn read_blob(store_root: &Path, hash: &str) -> Result<Blob> {
    read_object::<Blob>(store_root, hash)
}

fn commit_ancestor_depths(store_root: &Path, start_hash: &str) -> Result<HashMap<String, usize>> {
    let mut depths = HashMap::new();
    let mut queue = VecDeque::from([(start_hash.to_string(), 0usize)]);

    while let Some((hash, depth)) = queue.pop_front() {
        if depths.contains_key(&hash) {
            continue;
        }
        depths.insert(hash.clone(), depth);

        let commit = read_commit(store_root, &hash)
            .with_context(|| format!("read commit {hash} while walking ancestors"))?;
        for parent in commit.parent_hashes {
            queue.push_back((parent, depth + 1));
        }
    }

    Ok(depths)
}

pub fn is_ancestor(store_root: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    Ok(commit_ancestor_depths(store_root, descendant)?.contains_key(ancestor))
}

pub fn find_lca(store_root: &Path, left: &str, right: &str) -> Result<Option<String>> {
    let left_depths = commit_ancestor_depths(store_root, left)?;
    let right_depths = commit_ancestor_depths(store_root, right)?;

    let best = left_depths
        .iter()
        .filter_map(|(hash, left_depth)| {
            right_depths
                .get(hash)
                .map(|right_depth| (hash.clone(), left_depth + right_depth))
        })
        .min_by_key(|(_, score)| *score)
        .map(|(hash, _)| hash);

    Ok(best)
}

fn tree_to_map(tree: &Tree) -> HashMap<String, String> {
    tree.entries
        .iter()
        .map(|entry| (entry.name.clone(), entry.hash.clone()))
        .collect()
}

fn blob_content(store_root: &Path, hash: Option<&str>) -> Result<Vec<u8>> {
    match hash {
        Some(h) => Ok(read_blob(store_root, h)?.content),
        None => Ok(Vec::new()),
    }
}

fn make_conflict_content(
    store_root: &Path,
    path: &str,
    local_hash: Option<&str>,
    remote_hash: Option<&str>,
) -> Result<ConflictFile> {
    let local = blob_content(store_root, local_hash)?;
    let remote = blob_content(store_root, remote_hash)?;

    let mut content = Vec::new();
    content.extend_from_slice(b"<<<<<<< LOCAL\n");
    content.extend_from_slice(&local);
    if !local.ends_with(b"\n") {
        content.push(b'\n');
    }
    content.extend_from_slice(b"=======\n");
    content.extend_from_slice(&remote);
    if !remote.ends_with(b"\n") {
        content.push(b'\n');
    }
    content.extend_from_slice(b">>>>>>> REMOTE\n");

    Ok(ConflictFile {
        path: path.to_string(),
        content,
    })
}

pub fn merge_commits(
    store_root: &Path,
    local_head: &str,
    remote_head: &str,
    author: &str,
) -> Result<MergeResult> {
    if local_head == remote_head {
        return Ok(MergeResult::AlreadyUpToDate);
    }

    if is_ancestor(store_root, local_head, remote_head)? {
        let remote = read_commit(store_root, remote_head)?;
        return Ok(MergeResult::FastForward {
            new_head: remote_head.to_string(),
            tree_hash: remote.tree_hash,
        });
    }

    if is_ancestor(store_root, remote_head, local_head)? {
        return Ok(MergeResult::AlreadyUpToDate);
    }

    let lca_hash = find_lca(store_root, local_head, remote_head)?
        .context("no common ancestor found for merge")?;

    let local_commit = read_commit(store_root, local_head)?;
    let remote_commit = read_commit(store_root, remote_head)?;
    let base_commit = read_commit(store_root, &lca_hash)?;

    let local_tree = tree_to_map(&read_tree(store_root, &local_commit.tree_hash)?);
    let remote_tree = tree_to_map(&read_tree(store_root, &remote_commit.tree_hash)?);
    let base_tree = tree_to_map(&read_tree(store_root, &base_commit.tree_hash)?);

    let mut all_paths = BTreeSet::new();
    all_paths.extend(base_tree.keys().cloned());
    all_paths.extend(local_tree.keys().cloned());
    all_paths.extend(remote_tree.keys().cloned());

    let mut merged_entries = Vec::new();
    let mut conflicts = Vec::new();

    for path in all_paths {
        let base = base_tree.get(&path);
        let local = local_tree.get(&path);
        let remote = remote_tree.get(&path);

        let chosen = if local == remote {
            local.cloned()
        } else if local == base {
            remote.cloned()
        } else if remote == base {
            local.cloned()
        } else {
            conflicts.push(make_conflict_content(
                store_root,
                &path,
                local.map(String::as_str),
                remote.map(String::as_str),
            )?);
            None
        };

        if conflicts.last().map(|c| c.path.as_str()) == Some(path.as_str()) {
            continue;
        }

        if let Some(hash) = chosen {
            merged_entries.push(TreeEntry { name: path, hash });
        }
    }

    if !conflicts.is_empty() {
        for conflict in &conflicts {
            let hash = write_object(
                store_root,
                &Blob {
                    content: conflict.content.clone(),
                },
            )?;
            merged_entries.push(TreeEntry {
                name: conflict.path.clone(),
                hash,
            });
        }

        let merged_tree = Tree {
            entries: merged_entries,
        };
        let tree_hash = write_object(store_root, &merged_tree)?;
        return Ok(MergeResult::Conflicts {
            tree_hash,
            files: conflicts,
        });
    }

    let merged_tree = Tree { entries: merged_entries };
    let tree_hash = write_object(store_root, &merged_tree)?;
    let merge_commit = Commit {
        tree_hash: tree_hash.clone(),
        parent_hashes: vec![local_head.to_string(), remote_head.to_string()],
        message: format!("Merge {remote_head:.8} into {local_head:.8}"),
        timestamp: chrono::Utc::now().timestamp(),
        author: author.to_string(),
    };
    let new_head = store_commit(store_root, &merge_commit)?;
    Ok(MergeResult::MergeSuccess { new_head, tree_hash })
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: create a temporary `.rvc`-like directory.
    fn temp_store() -> TempDir {
        let dir = tempfile::tempdir().expect("create tempdir");
        std::fs::create_dir_all(dir.path().join("objects"))
            .expect("create objects dir");
        dir
    }

    fn commit_with_files(
        root: &Path,
        parent_hashes: Vec<String>,
        files: &[(&str, &[u8])],
        message: &str,
    ) -> (String, String) {
        let mut entries = Vec::new();
        for (name, content) in files {
            let blob_hash = write_object(root, &Blob {
                content: content.to_vec(),
            })
            .expect("write blob");
            entries.push(TreeEntry {
                name: (*name).to_string(),
                hash: blob_hash,
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree_hash = write_object(root, &Tree { entries }).expect("write tree");
        let commit_hash = store_commit(
            root,
            &Commit {
                tree_hash: tree_hash.clone(),
                parent_hashes,
                message: message.to_string(),
                timestamp: 1_700_000_000,
                author: "tester".to_string(),
            },
        )
        .expect("store commit");

        (commit_hash, tree_hash)
    }

    #[test]
    fn test_commit_roundtrip() {
        let store = temp_store();
        let root = store.path();

        // Initial commit (0 parents)
        let commit = Commit {
            tree_hash: "abc123".to_string(),
            parent_hashes: vec![],
            message: "initial commit".to_string(),
            timestamp: 1_700_000_000,
            author: "test-user".to_string(),
        };

        // Store and capture hash
        let hash = store_commit(root, &commit).expect("store commit");
        assert_eq!(hash.len(), 64, "SHA-256 hex should be 64 chars");

        // Read back and compare fields
        let recovered: Commit = read_commit(root, &hash).expect("read commit");
        assert_eq!(recovered.tree_hash, commit.tree_hash);
        assert_eq!(recovered.parent_hashes, commit.parent_hashes);
        assert_eq!(recovered.message, commit.message);
        assert_eq!(recovered.timestamp, commit.timestamp);
        assert_eq!(recovered.author, commit.author);

        // Verify that hash_object on the JSON produces the same hash
        let json = serde_json::to_vec(&commit).unwrap();
        let recomputed = hash_object(&json);
        assert_eq!(recomputed, hash, "hash must be stable across calls");

        // Normal commit (1 parent)
        let commit2 = Commit {
            tree_hash: "def456".to_string(),
            parent_hashes: vec![hash.clone()],
            message: "second commit".to_string(),
            timestamp: 1_700_001_000,
            author: "test-user".to_string(),
        };
        let hash2 = store_commit(root, &commit2).expect("store second commit");
        let recovered2: Commit = read_commit(root, &hash2).expect("read second commit");
        assert_eq!(recovered2.parent_hashes, vec![hash.clone()]);

        // Merge commit (2 parents)
        let merge = Commit {
            tree_hash: "ghi789".to_string(),
            parent_hashes: vec![hash.clone(), hash2.clone()],
            message: "merge commit".to_string(),
            timestamp: 1_700_002_000,
            author: "test-user".to_string(),
        };
        let hash_merge = store_commit(root, &merge).expect("store merge commit");
        let recovered_merge: Commit = read_commit(root, &hash_merge).expect("read merge commit");
        assert_eq!(recovered_merge.parent_hashes.len(), 2);
    }

    #[test]
    fn test_blob_roundtrip() {
        let store = temp_store();
        let root = store.path();

        let blob = Blob { content: b"hello, rvc!".to_vec() };
        let hash = write_object(root, &blob).expect("write blob");
        let recovered: Blob = read_object(root, &hash).expect("read blob");
        assert_eq!(recovered.content, blob.content);
    }

    #[test]
    fn test_tree_roundtrip() {
        let store = temp_store();
        let root = store.path();

        let tree = Tree {
            entries: vec![
                TreeEntry { name: "README.md".into(), hash: "deadbeef".into() },
                TreeEntry { name: "src/main.rs".into(), hash: "cafebabe".into() },
            ],
        };
        let hash = write_object(root, &tree).expect("write tree");
        let recovered: Tree = read_object(root, &hash).expect("read tree");
        assert_eq!(recovered.entries.len(), 2);
        assert_eq!(recovered.entries[0].name, "README.md");
    }

    #[test]
    fn test_merge_fast_forward() {
        let store = temp_store();
        let root = store.path();

        let (base_hash, _) = commit_with_files(root, vec![], &[("a.txt", b"base")], "base");
        let (remote_hash, remote_tree_hash) = commit_with_files(
            root,
            vec![base_hash.clone()],
            &[("a.txt", b"remote")],
            "remote",
        );

        let result = merge_commits(root, &base_hash, &remote_hash, "tester").expect("merge");
        match result {
            MergeResult::FastForward { new_head, tree_hash } => {
                assert_eq!(new_head, remote_hash);
                assert_eq!(tree_hash, remote_tree_hash);
            }
            other => panic!("expected fast-forward, got {other:?}"),
        }
    }

    #[test]
    fn test_merge_conflict_writes_markers() {
        let store = temp_store();
        let root = store.path();

        let (base_hash, _) = commit_with_files(root, vec![], &[("a.txt", b"base\n")], "base");
        let (local_hash, _) = commit_with_files(
            root,
            vec![base_hash.clone()],
            &[("a.txt", b"local\n")],
            "local",
        );
        let (remote_hash, _) = commit_with_files(
            root,
            vec![base_hash],
            &[("a.txt", b"remote\n")],
            "remote",
        );

        let result = merge_commits(root, &local_hash, &remote_hash, "tester").expect("merge");
        match result {
            MergeResult::Conflicts { tree_hash, files } => {
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].path, "a.txt");
                let rendered = String::from_utf8(files[0].content.clone()).expect("utf8");
                assert!(rendered.contains("<<<<<<< LOCAL"));
                assert!(rendered.contains("local"));
                assert!(rendered.contains("remote"));

                let tree = read_tree(root, &tree_hash).expect("read merged tree");
                assert_eq!(tree.entries.len(), 1);
            }
            other => panic!("expected conflicts, got {other:?}"),
        }
    }
}
