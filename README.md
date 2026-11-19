# eventsalsa

A component-based, PostgreSQL-native stack for event sourcing in Go — hosted at [`eventsalsa.dev`](https://eventsalsa.dev).

## Components

| Component | Import Path | Description |
| :--- | :--- | :--- |
| **Store** | `eventsalsa.dev/store` | Append-only event store, authoritative `stream_heads` optimistic concurrency, range partitioning, SQL migrations, and `eventmap-gen` |
| **Projector** | `eventsalsa.dev/projector` | Horizontally scalable async projection daemon with PostgreSQL MVCC visibility-horizon checkpointing, leader election, and telemetry |
| **Snapshot** | `eventsalsa.dev/snapshot` | Generic (`[T any]`) stream state snapshotting with pluggable codecs, payload transformers, and schema upcasters |
| **Encryption** | `eventsalsa.dev/encryption` | KMS-native envelope encryption (`kek/local`, `kek/awskms`, `kek/gcpkms`), AAD scope binding, GDPR crypto-shredding, and blind indexing |

## Installation

```bash
go get eventsalsa.dev/store
go get eventsalsa.dev/projector
go get eventsalsa.dev/snapshot
go get eventsalsa.dev/encryption

# Optional Cloud KMS providers (isolated Go sub-modules)
go get eventsalsa.dev/encryption/kek/awskms
go get eventsalsa.dev/encryption/kek/gcpkms
```

Requires Go `1.27.4+` and PostgreSQL (`pgx/v5`).

## Documentation

Full guides, component reference, and architectural documentation live at **[https://eventsalsa.dev](https://eventsalsa.dev)** (source in [`docs/`](./docs/)).

## Repository Layout

```text
eventsalsa/
├── store/                  # Module: eventsalsa.dev/store
├── snapshot/               # Module: eventsalsa.dev/snapshot
├── projector/              # Module: eventsalsa.dev/projector
├── encryption/             # Module: eventsalsa.dev/encryption
│   ├── kek/awskms/         # Module: eventsalsa.dev/encryption/kek/awskms
│   ├── kek/gcpkms/         # Module: eventsalsa.dev/encryption/kek/gcpkms
│   └── examples/           # Non-published runnable examples
└── docs/                   # Astro + Starlight website & Go vanity endpoint
```

## Development

```bash
# Run full suite across all Go modules and docs (requires Docker for testcontainers)
make check

# Run per-module checks
make check-store
make check-snapshot
make check-projector
make check-encryption
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
