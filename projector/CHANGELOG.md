# Changelog

> **Monorepo Cutover (`v0.6.0`+):** After `v0.5.0`, `projector` moved from `github.com/eventsalsa/projector` into [`github.com/eventsalsa/eventsalsa`](https://github.com/eventsalsa/eventsalsa) under `eventsalsa.dev/projector`. Historical entries below are preserved from the original repository.

## [0.7.0](https://github.com/eventsalsa/eventsalsa/compare/projector/v0.6.0...projector/v0.7.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **store,projector:** EventReader.ReadEvents now takes store.Cursor and GlobalPositionReader is replaced by HeadReader; projection_checkpoints requires last_transaction_id (xid8) and gap/frontier options and projection_gap_skips are removed.

### Features

* **store,projector:** replace gap heuristics with MVCC visibility horizon ([#20](https://github.com/eventsalsa/eventsalsa/issues/20)) ([b8e51c0](https://github.com/eventsalsa/eventsalsa/commit/b8e51c02318b882f9bbb0b5500a77a926b6c72ee))

## [0.6.0](https://github.com/eventsalsa/eventsalsa/compare/projector/v0.5.0...projector/v0.6.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **projector:** Module path changed from github.com/eventsalsa/projector to eventsalsa.dev/projector requiring eventsalsa.dev/store v0.3.0.

### Features

* **projector:** migrate to eventsalsa.dev/projector and Go 1.27.4 ([96d992c](https://github.com/eventsalsa/eventsalsa/commit/96d992cdbcc227b8d47877fc96607fbc73d23a2d))

## [0.5.0](https://github.com/eventsalsa/projector/compare/v0.4.0...v0.5.0) (2026-10-11)


### Features

* **projector:** add detached projections and a registry builder ([#19](https://github.com/eventsalsa/projector/issues/19)) ([f17cd1a](https://github.com/eventsalsa/projector/commit/f17cd1ae5fffb4b1c56af9c83a3f69e79cf70ae3))
* **projector:** add error classification and per-projection failure policy ([#23](https://github.com/eventsalsa/projector/issues/23)) ([bfd156f](https://github.com/eventsalsa/projector/commit/bfd156ff39ccb95c009cd6abf6932b6f63aadb55))
* **projector:** infer handler shapes in FromProjections ([4ca07c2](https://github.com/eventsalsa/projector/commit/4ca07c268b660843521a3fb0b516ce7676466036))


### Bug Fixes

* **projector:** avoid racing the leader connection on forced shutdown ([#24](https://github.com/eventsalsa/projector/issues/24)) ([e2e6652](https://github.com/eventsalsa/projector/commit/e2e665212f5489f4d46e4760b52ad2b75915918b))
* **projector:** give the detached checkpoint its own timeout budget ([48d66b8](https://github.com/eventsalsa/projector/commit/48d66b8d83e2394e19201ea79d1ef903d6abe8d4))

## [0.4.0](https://github.com/eventsalsa/projector/compare/v0.3.0...v0.4.0) (2026-08-22)


### Features

* **daemon:** refine lifecycle and coordination for errgroup compatibility ([8ffb003](https://github.com/eventsalsa/projector/commit/8ffb003ce1978d6f342020b7f64a5322b9cfa2fd))

## [0.3.0](https://github.com/eventsalsa/projector/compare/v0.2.0...v0.3.0) (2026-08-22)


### Features

* **observer:** introduce pluggable telemetry and lifecycle observer ([7dd0852](https://github.com/eventsalsa/projector/commit/7dd08526b389d56e08f729777e5d7cc93cfc8a28))

## [0.2.0](https://github.com/eventsalsa/projector/compare/v0.1.0...v0.2.0) (2026-08-21)


### Features

* **projector:** rename worker to projector and upgrade to store v0.2.0 ([#13](https://github.com/eventsalsa/projector/issues/13)) ([312f2fe](https://github.com/eventsalsa/projector/commit/312f2fe87d6717ad86f01cd7c34f9580fe4b40ac))

## [0.1.0](https://github.com/eventsalsa/worker/compare/v0.0.3...v0.1.0) (2026-08-20)


### ⚠ BREAKING CHANGES

* **worker:** replace aggregate terminology with stream across consumer contracts and event filtering

### Code Refactoring

* **worker:** align with stream terminology and upgrade store to v0.1.0 ([#11](https://github.com/eventsalsa/worker/issues/11)) ([e07b3c9](https://github.com/eventsalsa/worker/commit/e07b3c93f5e389b074a721b169a9d67119cacb9e))

## v0.0.2

### Added

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating worker infrastructure migrations with the same defaults and table-name overrides as `migrations.Config`.

### Documentation

- Documented the quick CLI flow and the package-level migration API in `README.md` and `migrations` package docs.
