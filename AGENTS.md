# Agent Guidelines for eventsalsa

This document defines the architecture, conventions, cross-module development workflow, and verification requirements for contributors and AI agents working in the `eventsalsa` monorepo (`github.com/eventsalsa/eventsalsa`, Go vanity domain `eventsalsa.dev`).

---

## 1. Repository Layout & Modules

| Path | Go Module / Role | Release Please Component |
| :--- | :--- | :--- |
| `store/` | `eventsalsa.dev/store` — PostgreSQL event store, migrations, `eventmap-gen` | `store` |
| `snapshot/` | `eventsalsa.dev/snapshot` — Generic stream snapshotting, codecs, upcasters | `snapshot` |
| `projector/` | `eventsalsa.dev/projector` — Distributed async projection daemon & gap-aware frontier | `projector` |
| `encryption/` | `eventsalsa.dev/encryption` — KMS-native envelope encryption, AAD binding, keystore | `encryption` |
| `encryption/kek/awskms/` | `eventsalsa.dev/encryption/kek/awskms` — AWS KMS KEK `Wrapper` / `Rewrapper` | `encryption/kek/awskms` |
| `encryption/kek/gcpkms/` | `eventsalsa.dev/encryption/kek/gcpkms` — Google Cloud KMS KEK `Wrapper` | `encryption/kek/gcpkms` |
| `encryption/examples/` | `eventsalsa.dev/encryption/examples` — Internal runnable examples (non-published) | *(excluded)* |
| `docs/` | Astro + Starlight site deployed to `https://eventsalsa.dev` via Cloudflare Pages (`?go-get=1` middleware in `docs/functions/_middleware.ts`) | *(excluded)* |

Module-specific guidelines and historical plans remain inside `<component>/AGENTS.md` and `<component>/.agents/`.

---

## 2. Cross-Module Development (`go.work`) & Topological Release Order

1. **No `replace` directives in published modules**: Published library `go.mod` files (`store`, `snapshot`, `projector`, `encryption`, `encryption/kek/awskms`, `encryption/kek/gcpkms`) must **never** carry relative `replace` directives. Only `encryption/examples/go.mod` uses `replace eventsalsa.dev/encryption => ../`.
2. **Local & PR development via root `go.work`**: The committed root `go.work` links all 7 Go modules so edits in `store/` or `encryption/` are immediately exercised by `snapshot/`, `projector/`, and `kek/*` without temporary `replace` edits.
3. **Topological Release Please workflow**:
   - When a change spans a dependency (`store` or `encryption`) and a downstream module (`projector`, `snapshot`, `encryption/kek/awskms`, `encryption/kek/gcpkms`), **merge the leaf module's Release Please PR first** so GitHub creates its `<module>/vX.Y.Z` tag.
   - Then run `GOWORK=off go get <dep>@vX.Y.Z && GOWORK=off go mod tidy` inside the downstream module(s), push the dependency bump commit to `main`, and merge the downstream Release Please PR(s).
   - Release Please PRs run an automated `GOWORK=off` standalone check in CI so a dependent module cannot be released against a stale `go.mod` requirement.

---

## 3. Git Workflow & Commit Conventions

1. **Branching**: Do not work directly on `main` for normal feature/fix tasks; open a conventional branch (`feat/...`, `fix/...`, `chore/...`, `refactor/...`, `docs/...`, `test/...`).
2. **Conventional Commits**: Format subject lines as `<type>(<scope>): <summary>` or `<type>: <summary>`. Scope with the component name (`store`, `snapshot`, `projector`, `encryption`, `docs`, `repo`) so Release Please scopes changelogs cleanly.
3. **Multiline Commit Bodies**: Every commit must include a descriptive body after a blank line. Never embed literal `\n` characters inside a single `-m` string; pass multiple `-m` flags (`git commit -m "subject" -m "body paragraph"`).

---

## 4. Verification Gates

Before finishing any code change, run the full monorepo validation suite (or the affected component check plus downstream dependents):

```bash
# Full monorepo validation (fmt-check, vet, golangci-lint, -race unit tests,
# Docker testcontainers integration tests, gosec, govulncheck, docs build)
make check

# Targeted module check
make check-store
make check-snapshot
make check-projector
make check-encryption
```

Always check formatting (`gofmt`), `golangci-lint` against the root `.golangci.yml`, race-enabled unit tests (`-race`), `testcontainers-go` integration tests, `gosec`, and `govulncheck`. For `docs/` changes, verify `npx --prefix docs tsc --noEmit` and `npm --prefix docs run build`.
