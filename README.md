## tgfs

Telegram as filesystem — a Rust CLI that uses Telegram as a free, durable
storage backend, primarily for backups.

```
tgfs init                 # log in, create the private storage channel
tgfs sync <folder>        # incremental one-way backup
tgfs ls [prefix]          # list remote files
tgfs get <remote> [dest]  # restore
```

Status: early scaffold. See [docs/SPEC.md](docs/SPEC.md) for the design
(Telegram API limits, chunked uploads, metadata index, optional FUSE mount).
