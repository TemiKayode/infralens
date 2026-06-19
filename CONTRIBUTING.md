# Contributing to InfraLens

Thank you for your interest in contributing. InfraLens is a multi-language monorepo
(Rust workspace + Go service + Python service) — this guide covers every layer.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [How to Contribute](#how-to-contribute)
3. [Development Environment](#development-environment)
4. [Project Structure](#project-structure)
5. [Rust Codebase](#rust-codebase)
6. [Go API Gateway](#go-api-gateway)
7. [Python LLM Copilot](#python-llm-copilot)
8. [Testing](#testing)
9. [Commit Messages](#commit-messages)
10. [Pull Request Process](#pull-request-process)
11. [Issue Reporting](#issue-reporting)
12. [Architecture Decisions](#architecture-decisions)

---

## Code of Conduct

Be direct, constructive, and respectful. Disagreements on design are expected and
welcome — keep them technical. Harassment of any kind will not be tolerated.

---

## How to Contribute

| Type | Action |
|------|--------|
| Bug report | [Open an issue](#issue-reporting) with the reproduction steps |
| Feature idea | Open a discussion issue before writing code |
| Small fix (typo, doc) | Open a PR directly — no issue needed |
| New crate or service | Discuss the design first in an issue |
| Performance improvement | Include benchmark numbers in the PR |

---

## Development Environment

### Required tools

| Tool | Version | Install |
|------|---------|---------|
| Rust (stable) | ≥ 1.79 | `rustup install stable` |
| Go | ≥ 1.23 | https://go.dev/dl/ |
| Python | ≥ 3.11 | https://python.org |
| Docker + Compose v2 | 24 / v2 | https://docs.docker.com/get-docker/ |
| `cargo-nextest` | latest | `cargo install cargo-nextest` (optional, faster test runner) |

No external `protoc` is needed. Proto compilation is handled by the pure-Rust `protox`
crate inside `crates/infralens-proto/build.rs` and `crates/infralens-rpc/build.rs`.

### First-time setup

```bash
# 1. Clone
git clone https://github.com/TemiKayode/infralens.git
cd infralens

# 2. Build the Rust workspace
cargo build --workspace

# 3. Build the Go gateway
cd services/api-gateway && go mod tidy && go build . && cd ../..

# 4. Install Python deps
cd services/llm-copilot
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cd ../..

# 5. Start backing services for development
docker compose up -d etcd minio prometheus grafana

# 6. Run the server in development mode
INFRALENS_ENV=development cargo run --bin infralens-server
```

---

## Project Structure

```
infralens/
├── crates/                     Rust workspace members
│   ├── infralens-common/       Shared types and Arrow schemas — no external deps
│   ├── infralens-proto/        Generated OTLP protobuf stubs
│   ├── infralens-storage/      LSM engine (WAL, MemTable, SSTable, compaction)
│   ├── infralens-ingest/       OTLP receivers and ingest pipeline
│   ├── infralens-server/       Binary entry point
│   ├── infralens-cluster/      Distributed coordination (etcd, ring, replication)
│   ├── infralens-rpc/          Scatter/gather gRPC
│   └── infralens-query/        IQL lexer, parser, planner, optimizer, executor
│
├── services/
│   ├── api-gateway/            Go chi HTTP gateway
│   └── llm-copilot/            Python FastAPI + llama-cpp
│
├── deploy/
│   ├── helm/infralens/         Helm chart
│   ├── kubernetes/operator/    kube-rs CRD operator
│   └── prometheus.yml
│
├── proto/                      Source .proto files (OTLP + internal RPC)
├── vendor/etcd-client/         Vendored etcd client (protox build, no protoc)
├── config/                     TOML configuration layers
└── demos/                      Runnable demo scripts and sample payloads
```

### Dependency rules

- `infralens-common` must not depend on any other workspace crate.
- `infralens-proto` depends only on `infralens-common`.
- `infralens-storage` depends only on `infralens-common`.
- `infralens-ingest` may depend on `infralens-common`, `infralens-proto`, `infralens-storage`.
- `infralens-cluster` and `infralens-rpc` may depend on any crate above them.
- `infralens-query` may depend on `infralens-common` and `infralens-storage`.
- `infralens-server` may depend on everything.

Circular dependencies between crates are a build error — keep the layering clean.

---

## Rust Codebase

### Formatting and linting

```bash
# Format — must pass before a PR is mergeable
cargo fmt --all

# Clippy — treat warnings as errors
cargo clippy --workspace --all-targets -- -D warnings
```

Both are enforced in CI. Run them before pushing.

### Style notes

- Follow the standard Rust API Guidelines: https://rust-lang.github.io/api-guidelines/
- Errors must use `thiserror` for library crates; `anyhow` is acceptable only in `infralens-server/main.rs`.
- All public items in library crates need a doc comment.
- Async code uses Tokio. Do not introduce a second async runtime.
- Prefer `Arc<T>` for shared ownership across tasks; avoid `Mutex<T>` wrapping a `Vec` — use `DashMap` or channels instead.
- `unsafe` is prohibited unless you include a `// SAFETY:` comment explaining the invariant.

### Adding a new crate

1. Create `crates/infralens-<name>/` with `Cargo.toml` and `src/lib.rs`.
2. Add it to the `members` list in the root `Cargo.toml`.
3. Pin all dependencies in `[workspace.dependencies]` so crate `Cargo.toml` files use `workspace = true`.
4. Add a test module in `src/lib.rs` and integration tests under `tests/` if applicable.

---

## Go API Gateway

### Setup

```bash
cd services/api-gateway
go mod tidy
```

### Formatting and linting

```bash
gofmt -w .
go vet ./...

# Optional but recommended
go install golang.org/x/lint/golint@latest
golint ./...
```

### Style notes

- Follow Effective Go: https://go.dev/doc/effective_go
- All HTTP handlers must return structured JSON error bodies — no plain-text errors to clients.
- Middleware must be composable (chi middleware pattern).
- Context propagation: every handler must accept and forward `context.Context`.
- Log with `log/slog` (already in use) — do not add `logrus` or `zap`.

---

## Python LLM Copilot

### Setup

```bash
cd services/llm-copilot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Formatting and linting

```bash
pip install ruff mypy
ruff check .
ruff format .
mypy copilot/
```

### Style notes

- Python ≥ 3.11 type hints are required on all public functions.
- FastAPI route handlers must declare Pydantic models for request/response bodies.
- The copilot must work in **stub mode** (no GGUF model loaded) — never gate the server startup on model availability.
- Feedback storage uses the SQLite module from stdlib — do not add ORM dependencies.

---

## Testing

### Rust

```bash
# All tests
cargo test --workspace

# Single crate
cargo test -p infralens-storage

# Faster with nextest
cargo nextest run --workspace

# With output (debugging)
cargo test --workspace -- --nocapture
```

Test coverage expectations:
- `infralens-storage`: integration tests in `tests/engine_integration.rs` must cover WAL recovery, flush, and compaction.
- `infralens-query`: unit tests for the lexer, parser, and each optimizer rule.
- `infralens-cluster`: unit tests for the consistent-hash ring.

### Go

```bash
cd services/api-gateway
go test ./...
```

### Python

```bash
cd services/llm-copilot
python -m pytest tests/ -v
```

### End-to-end

The `demos/quickstart.sh` (Linux/macOS) and `demos/quickstart.ps1` (Windows) scripts
run a full round-trip: start the compose stack, ingest sample data, run queries, and
verify results. Run them before submitting a PR that touches the ingest or query path.

```bash
bash demos/quickstart.sh
```

---

## Commit Messages

Use the conventional commits format:

```
<type>(<scope>): <short summary>

<optional body>

<optional footer>
```

**Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`, `ci`

**Scopes** (use the crate or service name): `storage`, `query`, `ingest`, `cluster`,
`rpc`, `server`, `api-gateway`, `copilot`, `helm`, `operator`, `demo`

**Examples:**

```
feat(storage): add zone-map pruning to compaction worker

fix(query): fix off-by-one in time_bucket boundary alignment

perf(storage): reduce lock contention in MemTable flush path

docs(api-gateway): add JWT configuration example to README
```

- Keep the first line under 72 characters.
- Use the imperative mood: "add", "fix", "remove" — not "added", "fixes", "removed".
- Reference issues in the footer: `Closes #42` or `See #17`.

---

## Pull Request Process

1. **Fork** the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/storage-bloom-upgrade
   ```

2. **Write tests** for any new behaviour. PRs without tests for new code paths will not be merged.

3. **Run the full check suite** before pushing:
   ```bash
   cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   cd services/api-gateway && go fmt ./... && go vet ./... && go test ./...
   cd services/llm-copilot && ruff check . && python -m pytest tests/ -v
   ```

4. **Open the PR** against `main`. Fill in the PR template:
   - What problem does this solve?
   - How was it tested?
   - Any breaking changes?
   - Screenshots or benchmark numbers if applicable.

5. **Address review comments** — push new commits, do not force-push during review.

6. **Squash on merge** — the maintainer squashes to keep `git log` clean.

### What gets reviewed

- Correctness of the implementation.
- Test coverage for new code paths.
- Adherence to the dependency layering rules above.
- No regression in compile time (avoid adding heavy build dependencies).
- Accurate documentation for any changed public API.

---

## Issue Reporting

### Bug reports

Include:
- InfraLens version or commit SHA (`git rev-parse --short HEAD`).
- OS and architecture.
- Exact steps to reproduce.
- Expected vs. actual behaviour.
- Relevant log output (redact any sensitive values).

### Feature requests

Describe the use case first, not the implementation. "I want X because when I do Y the
current behaviour is Z" is far more useful than "please add flag --foo".

### Security issues

**Do not open a public issue for security vulnerabilities.** Email the maintainer
directly or use GitHub's private security advisory feature. We aim to respond within
72 hours and will credit reporters in the release notes.

---

## Architecture Decisions

Significant design changes (new subsystem, protocol change, storage format change)
should be proposed as an issue with an ADR (Architecture Decision Record) before any
code is written. Use this template:

```markdown
## Context
What problem are we solving and why now?

## Decision
What are we doing?

## Alternatives considered
What else was evaluated and why was it rejected?

## Consequences
What breaks, what improves, what new constraints are introduced?
```

See `docs/architecture/` for the existing design documentation.
