# RvC

RvC is a decentralized peer-to-peer version control system written in Rust. It stores content-addressed objects locally and syncs commits directly between devices over LAN using `libp2p`, without relying on a central server.

## Workspace

- `rvc`: main CLI for repository operations
- `rvc-core`: shared object store, merge logic, and sync protocol
- `rvcd`: peer daemon for LAN discovery and sync serving

## Features

- repository initialization
- full working-tree commits
- commit history via `log`
- LAN peer discovery through `rvcd`
- object sync over `libp2p` request-response
- fast-forward sync
- clean merge commit creation
- conflict marker generation on divergent edits
- merge-in-progress tracking
- peer cache and name-based sync via `sync-peer`

## Build

```bash
cargo build --workspace
```

## Main Commands

```bash
rvc init
rvc commit -m "message"
rvc log
rvc status
rvc peers
rvc sync <multiaddr>
rvc sync-peer <peer-name>
rvc conflicts
rvc whoami
rvc set-author "Your Name"
rvc set-peer-name "laptop-a"
rvcd
rvcd discover
```

## Typical Workflow

1. Initialize a repo:

```bash
rvc init
```

2. Set your identity if you want custom names:

```bash
rvc set-author "Ajitesh"
rvc set-peer-name "ajitesh-laptop"
```

3. Start the daemon in each repo you want to participate in sync:

```bash
rvcd
```

4. Commit changes:

```bash
rvc commit -m "Initial snapshot"
```

5. Inspect known peers:

```bash
rvc peers
```

6. Sync by address or by cached name:

```bash
rvc sync /ip4/127.0.0.1/tcp/59113
rvc sync-peer ajitesh-laptop
```

7. If conflicts occur, inspect and resolve them:

```bash
rvc conflicts
```

Then edit the conflicted files and finish the merge:

```bash
rvc commit -m "Resolve merge conflict"
```

## Testing

The workspace includes both library tests and live end-to-end tests that exercise daemon-backed sync flows.

```bash
cargo test --workspace
```

## Notes

- `sync-peer` relies on the cached peer list written by `rvcd`.
- conflict resolution is manual after markers are written into the working tree.
- the project is designed and tested primarily on macOS/Linux-style environments.
