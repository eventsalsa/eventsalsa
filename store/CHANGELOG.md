# Changelog

> **Monorepo Cutover (`v0.3.0`+):** After `v0.2.0`, `store` moved from `github.com/eventsalsa/store` into [`github.com/eventsalsa/eventsalsa`](https://github.com/eventsalsa/eventsalsa) under the vanity import path `eventsalsa.dev/store`. Historical entries below are preserved from the original repository.

## [0.3.0](https://github.com/eventsalsa/eventsalsa/compare/store/v0.2.0...store/v0.3.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **store:** Module path changed from github.com/eventsalsa/store to eventsalsa.dev/store.

### Features

* **store:** migrate to eventsalsa.dev/store and add transaction_id xid8 ([282e142](https://github.com/eventsalsa/eventsalsa/commit/282e142d8a0ee39368c16ca1ec14a552ff904ace))


### Bug Fixes

* **deps:** bump store dependencies (pgx v5.13.0, testcontainers postgres v0.45.0) ([#8](https://github.com/eventsalsa/eventsalsa/issues/8)) ([2c20a53](https://github.com/eventsalsa/eventsalsa/commit/2c20a53464093e793a100a22b3ba8a098070bde7))

## [0.2.0](https://github.com/eventsalsa/store/compare/v0.1.0...v0.2.0) (2026-08-21)


### ⚠ BREAKING CHANGES

* remove consumer and scoped consumer interfaces ([#12](https://github.com/eventsalsa/store/issues/12))

### Code Refactoring

* remove consumer and scoped consumer interfaces ([#12](https://github.com/eventsalsa/store/issues/12)) ([ffa6faa](https://github.com/eventsalsa/store/commit/ffa6faaaeb33236cccb81b1d6180b885295eb227))

## [0.1.0](https://github.com/eventsalsa/store/compare/v0.0.4...v0.1.0) (2026-08-20)


### ⚠ BREAKING CHANGES

* **store:** replace aggregate terminology with stream across codebase

### Features

* range partitioning on global_position & authoritative stream_heads concurrency control ([#11](https://github.com/eventsalsa/store/issues/11)) ([ca1319c](https://github.com/eventsalsa/store/commit/ca1319ca3b7b8885e53f9e8d538915d9b5abc4ab))


### Code Refactoring

* **store:** replace aggregate terminology with stream across codebase ([#7](https://github.com/eventsalsa/store/issues/7)) ([b22759d](https://github.com/eventsalsa/store/commit/b22759d594e1d2ee36df95e22b60466e427212c0))

## [0.0.4](https://github.com/eventsalsa/store/compare/v0.0.3...v0.0.4) (2026-08-14)


### Bug Fixes

* **eventmap:** ensure deterministic FromESEvent switch cases and gofmt output ([#4](https://github.com/eventsalsa/store/issues/4)) ([aa4e6af](https://github.com/eventsalsa/store/commit/aa4e6af3b98d8b1c7b9392309839301075f430a2))

## [0.0.3](https://github.com/eventsalsa/store/compare/v0.0.2...v0.0.3) (2026-06-07)

### Bug Fixes

* **postgres:** support simple protocol JSONB parameter binding ([#3](https://github.com/eventsalsa/store/pull/3))

## [0.0.2](https://github.com/eventsalsa/store/compare/v0.0.1...v0.0.2) (2026-06-07)

### Code Refactoring

* **postgres:** migrate database/sql to native pgx/v5 ([#2](https://github.com/eventsalsa/store/pull/2))

### Miscellaneous Chores

* **agents:** convert GitHub Copilot instructions to workspace agent skills ([#1](https://github.com/eventsalsa/store/pull/1))

## 0.0.1 (2026-04-16)

### Features

* add initial eventsalsa store library
