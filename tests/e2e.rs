use std::{
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::TempDir;

fn rvc_bin() -> &'static str {
    env!("CARGO_BIN_EXE_rvc")
}

fn rvcd_bin() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("target").join("debug").join("rvcd")
}

fn run_command(program: impl AsRef<Path>, args: &[&str], cwd: &Path) -> String {
    let output = Command::new(program.as_ref())
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {:?}: {err}", program.as_ref()));

    if !output.status.success() {
        panic!(
            "command {:?} {:?} failed\nstdout:\n{}\nstderr:\n{}",
            program.as_ref(),
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8_lossy(&output.stdout).to_string()
}

fn wait_for(condition: impl Fn() -> bool, timeout: Duration, message: &str) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }

    panic!("{message}");
}

fn peer_count(output: &str) -> usize {
    output
        .lines()
        .find_map(|line| line.strip_prefix("Peers: "))
        .and_then(|count| count.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

fn peer_addr_for_head(output: &str, head_prefix: &str) -> Option<String> {
    output.lines().find_map(|line| {
        if !line.contains(head_prefix) {
            return None;
        }
        let at = line.find('@')?;
        let after_at = line[at + 1..].trim_start();
        let addr = after_at.split_whitespace().next()?;
        Some(addr.to_string())
    })
}

struct TestRepo {
    _dir: TempDir,
    path: PathBuf,
}

impl TestRepo {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().to_path_buf();
        run_command(rvc_bin(), &["init"], &path);
        Self { _dir: dir, path }
    }

    fn write_file(&self, relative: &str, content: &str) {
        let full_path = self.path.join(relative);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).expect("create parent directories");
        }
        fs::write(full_path, content).expect("write file");
    }

    fn read_file(&self, relative: &str) -> String {
        fs::read_to_string(self.path.join(relative)).expect("read file")
    }

    fn commit(&self, message: &str) -> String {
        run_command(rvc_bin(), &["commit", "-m", message], &self.path)
    }

    fn log(&self) -> String {
        run_command(rvc_bin(), &["log"], &self.path)
    }

    fn status(&self) -> String {
        run_command(rvc_bin(), &["status"], &self.path)
    }

    fn peers(&self) -> String {
        run_command(rvc_bin(), &["peers"], &self.path)
    }

    fn conflicts(&self) -> String {
        run_command(rvc_bin(), &["conflicts"], &self.path)
    }

    fn discover(&self) -> String {
        run_command(rvc_bin(), &["discover"], &self.path)
    }

    fn sync(&self, addr: &str) -> String {
        run_command(rvc_bin(), &["sync", addr], &self.path)
    }

    fn head(&self) -> String {
        fs::read_to_string(self.path.join(".rvc").join("refs").join("HEAD"))
            .expect("read head")
            .trim()
            .to_string()
    }

    fn set_peer_name(&self, name: &str) {
        run_command(rvc_bin(), &["set-peer-name", name], &self.path);
    }

    fn alias_peer(&self, peer: &str, alias: &str) {
        run_command(rvc_bin(), &["alias-peer", peer, alias], &self.path);
    }

    fn resolve(&self, path: &str) -> String {
        run_command(rvc_bin(), &["resolve", path], &self.path)
    }

    fn abort_merge(&self) -> String {
        run_command(rvc_bin(), &["abort-merge"], &self.path)
    }
}

struct Daemon {
    child: Child,
    addr: String,
}

impl Daemon {
    fn start(repo: &TestRepo) -> Self {
        let mut child = Command::new(rvcd_bin())
            .current_dir(&repo.path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn rvcd");

        let stdout = child.stdout.take().expect("take stdout");
        let mut reader = BufReader::new(stdout);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut line = String::new();
        let mut addr = None;

        while Instant::now() < deadline {
            line.clear();
            if reader.read_line(&mut line).expect("read daemon output") == 0 {
                if let Some(status) = child.try_wait().expect("check daemon status") {
                    let mut stderr = String::new();
                    if let Some(mut pipe) = child.stderr.take() {
                        let _ = pipe.read_to_string(&mut stderr);
                    }
                    panic!("rvcd exited early with status {status}: {stderr}");
                }
                thread::sleep(Duration::from_millis(50));
                continue;
            }

            if let Some(found) = line.trim().strip_prefix("rvcd: listening on ") {
                if found.starts_with("/ip4/127.0.0.1/tcp/") {
                    addr = Some(found.to_string());
                    break;
                }
            }
        }

        let addr = addr.expect("daemon did not report a loopback listen address");
        let stdout = reader.into_inner();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut sink = String::new();
            loop {
                sink.clear();
                if reader.read_line(&mut sink).ok().filter(|count| *count > 0).is_none() {
                    break;
                }
            }
        });
        if let Some(stderr) = child.stderr.take() {
            thread::spawn(move || {
                let mut reader = BufReader::new(stderr);
                let mut sink = String::new();
                loop {
                    sink.clear();
                    if reader.read_line(&mut sink).ok().filter(|count| *count > 0).is_none() {
                        break;
                    }
                }
            });
        }
        Self { child, addr }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn fast_forward_sync_materializes_files() {
    let repo_a = TestRepo::new();
    let repo_b = TestRepo::new();
    repo_a.set_peer_name("peer-a");
    repo_b.set_peer_name("peer-b");

    repo_a.write_file("note.txt", "hello from A\n");
    repo_a.commit("first from A");

    let daemon_a = Daemon::start(&repo_a);
    let _daemon_b = Daemon::start(&repo_b);

    let sync_output = repo_b.sync(&daemon_a.addr);
    assert!(sync_output.contains("Fast-forwarded"));

    wait_for(
        || repo_b.path.join("note.txt").exists(),
        Duration::from_secs(5),
        "synced file did not appear in working tree",
    );
    wait_for(
        || peer_count(&repo_b.peers()) > 0,
        Duration::from_secs(5),
        "peer cache did not record the discovered peer",
    );
    assert_eq!(repo_b.read_file("note.txt"), "hello from A\n");
    assert!(repo_b.log().contains("first from A"));
    let peers = repo_b.peers();
    assert!(peer_count(&peers) > 0);
    let remote_head = repo_a.head();
    let remote_addr = peer_addr_for_head(&peers, &remote_head[..8]).expect("peer addr for synced head");
    repo_b.alias_peer(&remote_addr, "seeded-a");
    let aliased_peers = repo_b.peers();
    assert!(aliased_peers.contains("seeded-a"));
    let discover_output = repo_b.discover();
    assert!(discover_output.contains("peers found:"));
}

#[test]
fn conflicting_sync_writes_conflict_markers_and_keeps_local_head() {
    let repo_a = TestRepo::new();
    let repo_b = TestRepo::new();
    repo_a.set_peer_name("peer-a");
    repo_b.set_peer_name("peer-b");

    repo_a.write_file("note.txt", "base\n");
    repo_a.commit("base");

    let daemon_a = Daemon::start(&repo_a);
    let _daemon_b = Daemon::start(&repo_b);
    repo_b.sync(&daemon_a.addr);

    repo_a.write_file("note.txt", "remote edit from A\n");
    repo_b.write_file("note.txt", "local edit from B\n");
    repo_a.commit("remote change on A");
    repo_b.commit("local change on B");
    let local_head_before_sync = repo_b.head();

    let sync_output = repo_b.sync(&daemon_a.addr);
    assert!(sync_output.contains("Merge conflicts in:"));

    wait_for(
        || repo_b.read_file("note.txt").contains("<<<<<<< LOCAL"),
        Duration::from_secs(5),
        "conflict markers were not written to the working tree",
    );

    let note = repo_b.read_file("note.txt");
    assert!(note.contains("local edit from B"));
    assert!(note.contains("remote edit from A"));
    assert_eq!(repo_b.head(), local_head_before_sync);
}

#[test]
fn resolved_conflict_commit_finishes_merge_and_clears_merge_state() {
    let repo_a = TestRepo::new();
    let repo_b = TestRepo::new();
    repo_a.set_peer_name("peer-a");
    repo_b.set_peer_name("peer-b");

    repo_a.write_file("note.txt", "base\n");
    repo_a.commit("base");

    let daemon_a = Daemon::start(&repo_a);
    let _daemon_b = Daemon::start(&repo_b);
    repo_b.sync(&daemon_a.addr);

    repo_a.write_file("note.txt", "remote edit from A\n");
    repo_b.write_file("note.txt", "local edit from B\n");
    repo_a.commit("remote change on A");
    let remote_head = repo_a.head();
    repo_b.commit("local change on B");
    let local_head = repo_b.head();

    repo_b.sync(&daemon_a.addr);
    let status = repo_b.status();
    assert!(status.contains("Merge: in progress"));
    assert!(status.contains("note.txt"));
    let conflicts = repo_b.conflicts();
    assert!(conflicts.contains("Merge in progress"));
    assert!(conflicts.contains("note.txt"));

    repo_b.write_file("note.txt", "resolved content\n");
    let resolve_output = repo_b.resolve("note.txt");
    assert!(resolve_output.contains("All conflicts resolved") || resolve_output.contains("Marked"));
    repo_b.commit("resolve conflict");

    let status = repo_b.status();
    assert!(!status.contains("Merge: in progress"));
    assert!(repo_b.conflicts().contains("No merge in progress."));

    let head = repo_b.head();
    let head_path = repo_b
        .path
        .join(".rvc")
        .join("objects")
        .join(&head[..2])
        .join(&head[2..]);
    let output = Command::new("gzip")
        .arg("-cd")
        .arg(head_path)
        .output()
        .expect("read merge commit object");
    assert!(output.status.success());
    let commit_json = String::from_utf8_lossy(&output.stdout);
    assert!(commit_json.contains(&local_head));
    assert!(commit_json.contains(&remote_head));
    assert!(repo_b.log().contains("resolve conflict"));
}

#[test]
fn abort_merge_restores_local_state_and_clears_merge_state() {
    let repo_a = TestRepo::new();
    let repo_b = TestRepo::new();
    repo_a.set_peer_name("peer-a");
    repo_b.set_peer_name("peer-b");

    repo_a.write_file("note.txt", "base\n");
    repo_a.commit("base");

    let daemon_a = Daemon::start(&repo_a);
    let _daemon_b = Daemon::start(&repo_b);
    repo_b.sync(&daemon_a.addr);

    repo_a.write_file("note.txt", "remote edit from A\n");
    repo_b.write_file("note.txt", "local edit from B\n");
    repo_a.commit("remote change on A");
    repo_b.commit("local change on B");
    let local_head = repo_b.head();
    let local_content = repo_b.read_file("note.txt");

    repo_b.sync(&daemon_a.addr);
    assert!(repo_b.status().contains("Merge: in progress"));

    let abort_output = repo_b.abort_merge();
    assert!(abort_output.contains("Aborted merge"));
    assert!(!repo_b.status().contains("Merge: in progress"));
    assert_eq!(repo_b.read_file("note.txt"), local_content);
    assert_eq!(repo_b.head(), local_head);
}

#[test]
fn clean_divergent_sync_creates_merge_commit_and_keeps_both_files() {
    let repo_a = TestRepo::new();
    let repo_b = TestRepo::new();
    repo_a.set_peer_name("peer-a");
    repo_b.set_peer_name("peer-b");

    repo_a.write_file("common.txt", "base\n");
    repo_a.commit("base");

    let daemon_a = Daemon::start(&repo_a);
    let _daemon_b = Daemon::start(&repo_b);
    repo_b.sync(&daemon_a.addr);

    repo_a.write_file("only-a.txt", "from A\n");
    repo_b.write_file("only-b.txt", "from B\n");
    repo_a.commit("add only-a");
    repo_b.commit("add only-b");

    let sync_output = repo_b.sync(&daemon_a.addr);
    assert!(sync_output.contains("Merged remote changes."));

    wait_for(
        || repo_b.path.join("only-a.txt").exists() && repo_b.path.join("only-b.txt").exists(),
        Duration::from_secs(5),
        "merged files did not appear in working tree",
    );

    assert_eq!(repo_b.read_file("only-a.txt"), "from A\n");
    assert_eq!(repo_b.read_file("only-b.txt"), "from B\n");
    let log = repo_b.log();
    assert!(log.contains("Merge "));
    assert!(log.contains("add only-b"));
}
