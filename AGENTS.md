# Agent Guidelines for eventsalsa

This document defines the architecture, conventions, cross-module development workflow, and verification requirements for contributors and AI agents working in the `eventsalsa` monorepo (`github.com/eventsalsa/eventsalsa`, Go vanity domain `eventsalsa.dev`).

---

## 1. Repository Layout & Modules

| Path | Go Module / Role | Release Please Component |
| :--- | :--- | :--- |
| `store/` | `eventsalsa.dev/store` — PostgreSQL event store, migrations, `eventmap-gen` | `store` |
| `snapshot/` | `eventsalsa.dev/snapshot` — Generic stream snapshotting, codecs, upcasters | `snapshot` |
| `projector/` | `eventsalsa.dev/projector` — Distributed async projection daemon & MVCC visibility-horizon checkpointing | `projector` |
| `encryption/` | `eventsalsa.dev/encryption` — KMS-native envelope encryption, AAD binding, keystore | `encryption` |
| `encryption/kek/awskms/` | `eventsalsa.dev/encryption/kek/awskms` — AWS KMS KEK `Wrapper` / `Rewrapper` | `encryption/kek/awskms` |
| `encryption/kek/gcpkms/` | `eventsalsa.dev/encryption/kek/gcpkms` — Google Cloud KMS KEK `Wrapper` | `encryption/kek/gcpkms` |
| `encryption/examples/` | `eventsalsa.dev/encryption/examples` — Internal runnable examples (non-published) | *(excluded)* |
| `cqrs/` | `eventsalsa.dev/cqrs` — Transport-free `cqrs/command` (`Bus.Dispatch`), `cqrs/query` (`Execute`), `ObservabilityMiddleware`, retry, and `spectest` | `cqrs` |
| `docs/` | Astro + Starlight site deployed to `https://eventsalsa.dev` via Cloudflare Workers (`?go-get=1` handler in `docs/worker/index.ts`) | *(excluded)* |

Module-specific guidelines and historical plans remain inside `<component>/AGENTS.md` and `<component>/.agents/`.

---

## 2. Cross-Module Development (`go.work`) & Topological Release Order

1. **No `replace` directives in published modules**: Published library `go.mod` files (`store`, `snapshot`, `projector`, `encryption`, `encryption/kek/awskms`, `encryption/kek/gcpkms`, `cqrs`) must **never** carry relative `replace` directives. Only `encryption/examples/go.mod` uses `replace eventsalsa.dev/encryption => ../`.
2. **Local & PR development via root `go.work`**: The committed root `go.work` links all 8 Go modules so edits in `store/` or `encryption/` are immediately exercised by `snapshot/`, `projector/`, and `kek/*` without temporary `replace` edits, while keeping `cqrs/` standalone.
3. **Topological Release Please workflow**:
   - When a change spans a dependency (`store` or `encryption`) and a downstream module (`projector`, `snapshot`, `encryption/kek/awskms`, `encryption/kek/gcpkms`), **merge the leaf module's Release Please PR first** so GitHub creates its `<module>/vX.Y.Z` tag.
   - Then run `GOWORK=off go get <dep>@vX.Y.Z && GOWORK=off go mod tidy` inside the downstream module(s), push the dependency bump commit to `main`, and merge the downstream Release Please PR(s).
   - Release Please PRs run an automated `GOWORK=off` standalone check in CI so a dependent module cannot be released against a stale `go.mod` requirement.

---

## 3. Git Workflow & Commit Conventions

1. **Branching**: Do not work directly on `main` for normal feature/fix tasks; open a conventional branch (`feat/...`, `fix/...`, `chore/...`, `refactor/...`, `docs/...`, `test/...`).
2. **Conventional Commits**: Format subject lines as `<type>(<scope>): <summary>` or `<type>: <summary>`. Scope with the component name (`store`, `snapshot`, `projector`, `encryption`, `cqrs`, `docs`, `repo`) so Release Please scopes changelogs cleanly.
3. **Multiline Commit Bodies**: Every commit must include a descriptive body after a blank line. Never embed literal `\n` characters inside a single `-m` string; pass multiple `-m` flags (`git commit -m "subject" -m "body paragraph"`).

---

## 4. Verification Gates

Before finishing any code change, run the full monorepo validation suite (or the affected component check plus downstream dependents):

```bash
# Full monorepo validation (fmt-check, vet, golangci-lint, -race unit tests,
# integration tests, gosec, govulncheck, docs build)
make check

# Targeted module check
make check-store
make check-snapshot
make check-projector
make check-encryption
make check-cqrs
```

Always check formatting (`gofmt -w -s .`; `make fmt` intentionally omits `goimports` because Go 1.27 generic methods such as `spectest.Spec.When[C any]` are not supported by `goimports`), `golangci-lint` against the root `.golangci.yml`, race-enabled unit tests (`-race`), integration tests (`testcontainers-go` for PostgreSQL-backed modules; in-process suite for `cqrs`), `gosec`, and `govulncheck`. For `docs/` changes, verify `make docs-build` (`(cd docs && ./node_modules/.bin/tsc -p tsconfig.worker.json)` and `npm --prefix docs run build`).
