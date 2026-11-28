# Changelog

> **Monorepo Cutover (`v0.1.0`+):** After `v0.0.6`, `encryption` moved from `github.com/eventsalsa/encryption` into [`github.com/eventsalsa/eventsalsa`](https://github.com/eventsalsa/eventsalsa) under `eventsalsa.dev/encryption` (with `kek/awskms` and `kek/gcpkms` tagged starting at `v0.0.1`). Historical entries below are preserved from the original repository.

## [0.2.0](https://github.com/eventsalsa/eventsalsa/compare/encryption/v0.1.0...encryption/v0.2.0) (2026-11-28)


### Features

* **cqrs:** add transport-free cqrs module, spectest, docs, and CI wiring ([#28](https://github.com/eventsalsa/eventsalsa/issues/28)) ([a1a47e1](https://github.com/eventsalsa/eventsalsa/commit/a1a47e14294e5879cf6d9166dcab3ec6c7baae93))

## [0.1.0](https://github.com/eventsalsa/eventsalsa/compare/encryption/v0.0.6...encryption/v0.1.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **encryption/kek:** Module path moved to eventsalsa.dev/encryption/kek/gcpkms requiring eventsalsa.dev/encryption v0.1.0.
* **encryption/kek:** Module path moved to eventsalsa.dev/encryption/kek/awskms requiring eventsalsa.dev/encryption v0.1.0.
* **encryption:** Module path changed from github.com/eventsalsa/encryption to eventsalsa.dev/encryption.

### Features

* **encryption/kek:** initialize awskms submodule at eventsalsa.dev ([fae67a3](https://github.com/eventsalsa/eventsalsa/commit/fae67a303def5022bbded4cddf407e97d329e461))
* **encryption/kek:** initialize gcpkms submodule at eventsalsa.dev ([d34894e](https://github.com/eventsalsa/eventsalsa/commit/d34894e9d3a869c2c24213b360c4dd3fdfd2cf44))
* **encryption:** migrate to eventsalsa.dev/encryption and enforce strict lint ([29b0d43](https://github.com/eventsalsa/eventsalsa/commit/29b0d432f8eb2bc436f1037082d1455667a8be49))


### Bug Fixes

* **deps:** bump encryption dependencies (pgx v5.13.0, testcontainers v0.45.0) ([#9](https://github.com/eventsalsa/eventsalsa/issues/9)) ([c40928f](https://github.com/eventsalsa/eventsalsa/commit/c40928f42ba1f2f4176f5fd57f2cd4898c7e05ef))
* **deps:** bump github.com/jackc/pgx/v5 from 5.12.0 to 5.13.0 in /encryption/examples ([#6](https://github.com/eventsalsa/eventsalsa/issues/6)) ([7ff25b5](https://github.com/eventsalsa/eventsalsa/commit/7ff25b5f6cc5c5b26120948bb69923b84060930a))

## [0.0.6](https://github.com/eventsalsa/encryption/compare/v0.0.5...v0.0.6) (2026-10-15)


### ⚠ BREAKING CHANGES

* redesign envelope encryption for KMS-native KEK providers ([#17](https://github.com/eventsalsa/encryption/issues/17))

### Features

* redesign envelope encryption for KMS-native KEK providers ([#17](https://github.com/eventsalsa/encryption/issues/17)) ([7ff377c](https://github.com/eventsalsa/encryption/commit/7ff377c66d331be3701a194c1b78cfd81e3dcbaa))

## [0.0.5](https://github.com/eventsalsa/encryption/compare/v0.0.4...v0.0.5) (2026-08-31)


### ⚠ BREAKING CHANGES

* pure in-memory envelope crypto and consolidated postgres keystore ([#10](https://github.com/eventsalsa/encryption/issues/10))

### Refactoring

* pure in-memory envelope crypto and consolidated postgres keystore ([#10](https://github.com/eventsalsa/encryption/issues/10)) ([a56398e](https://github.com/eventsalsa/encryption/commit/a56398ee5a91d907e3b3a6f38d9ba6406f12f2e0))

## v0.0.4 - 2026-06-07

### Added

- Migrated database keystore and transaction utilities to native `pgx/v5` (#7).

### Changed

- Migrated repository agent instructions and workflow configuration (#6).

## v0.0.3 - 2026-04-18

### Added

- Added `(*postgres.Store).RewrapSystemKeys`, a PostgreSQL administrative API for re-encrypting stored DEKs from one system key ID to another in short batches, with dry-run support and progress counters.

### Changed

- Updated the README to document the PostgreSQL system-key rewrap workflow, including the recommended operator sequence and why rewrap remains a library API instead of a built-in CLI.
- Expanded PostgreSQL coverage for system-key rewrap option validation, dry runs, idempotency, concurrent runs, and historical ciphertext decryptability after retiring the old key.
- Expanded `make check` into the full truthful local validation suite and widened integration-tagged test coverage to `go test -race -count=1 -tags=integration ./...`.
- Clarified repository contributor context so `make check` matches the locally runnable required checks, with GitHub-only CodeQL called out separately.

## v0.0.2 - 2026-04-17

### Added

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating or printing the PostgreSQL encryption keystore migration.
- Added package helpers in `keystore/postgres/migrations` for rendering and writing migration SQL with `postgres.Config` schema and table overrides.

### Changed

- Updated the README to document the CLI-first migration flow and the package-based advanced alternative.

### Fixed

- Pinned the GitHub Actions gosec step to `securego/gosec@v2.25.0`, fixing workflow resolution failures caused by the missing `v2` ref.

## v0.0.1 - 2026-04-16

### Added

- Initial release of envelope encryption library with AES-256-GCM cipher, system keyring, and PostgreSQL keystore.
