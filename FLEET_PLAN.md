# PocketKafka Fleet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bangun Fleet (kontrol in-process terisolasi + agent terpisah + zona UI) sesuai FLEET_DESIGN.md.

**Architecture:** `internal/fleet/` (types, registry, jobs, audit, server mux terpisah) + `cmd/fleet-agent/` (biner TLS outbound). UI zona Kafka|Fleet di SPA. Authz numpang `authz.Authorizer` bersama dengan resource `fleet_*`.

**Tech Stack:** Go 1.26 stdlib only (zero-deps tetap), Docker Engine API via HTTP socket (tanpa SDK luar), SPA `web/dist/index.html`.

## Global Constraints

- `go.mod` tetap zero third-party dependencies.
- `fleet.enabled=false` (default): listener tak dibuka, zona UI hilang, `curl /api/v1/fleet/*` → 404/403.
- Tiap handler Fleet: `recover()` + batas body + batas log.
- Secret agent tersimpan hash, bukan plaintext.
- HMAC atas arah+op+SHA256-body+timestamp+message_id; tolak replay.
- Revokasi dicek saat connect DAN sebelum kirim job.
- Pre-commit tiap task: `gofmt -l .` kosong, `go test ./...` hijau.

---

### Task 1: Config `fleet.*`

**Files:**
- Modify: `internal/config/config.go` (struct Fleet: Enabled, Listen, TLS cert/key, HeartbeatTimeout, RetentionDays/Entries)
- Modify: `internal/config/yaml.go` (parse key `fleet.*` + env `KAFKA_FLEET_*`)
- Modify: `internal/config/validate.go` (listen publik tanpa TLS+auth = gagal; default listen `127.0.0.1:18090`)

**Interfaces:**
- Consumes: pola `web.*`/`security.*` yang ada.
- Produces: `config.Fleet` dipakai Task 5 (wiring listener).

- [ ] **Step 1: Write the failing test**

```go
func TestFleetDefaults(t *testing.T) {
    cfg := Default()
    if cfg.Fleet.Enabled { t.Fatal("fleet must default disabled") }
    if cfg.Fleet.Listen != "127.0.0.1:18090" { t.Fatalf("listen = %q", cfg.Fleet.Listen) }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config -run TestFleetDefaults -v`
Expected: FAIL (struct Fleet belum ada)

- [ ] **Step 3: Write minimal implementation**

Tambah struct + default + parse + validasi mengikuti pola field `web.*` di ketiga file.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -count=1 ./internal/config -v`
Expected: PASS semua

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "feat(fleet): config fleet.* with loopback default and public-listener validation"
```

### Task 2: Fleet types + registry

**Files:**
- Create: `internal/fleet/types.go` (Server, Agent, Enrollment, Job, JobStatus, AuditEvent, HMAC envelope: protocol_version, message_id, issued_at, expiry, op, body_sha256, hmac)
- Create: `internal/fleet/registry.go` (load/save `__fleet_agents.json` via `internal/atomicfile`, mutex, secret = hash via `internal/credential`)
- Test: `internal/fleet/registry_test.go` (enroll sekali-pakai, reuse ditolak, revoke menutup akses, secret tersimpan hash)

**Interfaces:**
- Consumes: `internal/atomicfile`, `internal/credential`.
- Produces: `fleet.Registry` (Enroll, ConsumeToken, Revoke, Lookup) dipakai Task 4–5.

- [ ] **Step 1: Write the failing test**

```go
func TestEnrollTokenSingleUse(t *testing.T) {
    r := NewRegistry(t.TempDir());
    tok, _ := r.Enroll("srv-1")
    if _, err := r.ConsumeToken(tok); err != nil { t.Fatal(err) }
    if _, err := r.ConsumeToken(tok); err == nil { t.Fatal("reuse must fail") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/fleet -run TestEnrollTokenSingleUse -v`
Expected: FAIL (package belum ada)

- [ ] **Step 3: Write minimal implementation**

Types + registry file JSON pola `authz/store.go` (marshal penuh + atomic write + mutex). Token: random 32 byte, expiry 15 menit, single-use flag.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -count=1 ./internal/fleet -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/fleet/
git commit -m "feat(fleet): registry with single-use enrollment and hashed secrets"
```

### Task 3: Jobs + audit JSONL

**Files:**
- Create: `internal/fleet/jobs.go` (state machine queued→running→succeeded/failed, idempotency key, timeout, append `fleet_jobs.jsonl`, retensi hari/entri)
- Create: `internal/fleet/audit.go` (append `fleet_audit.jsonl`: actor, target, waktu, hasil, correlation ID; sanitasi secret; rotasi)
- Test: `internal/fleet/jobs_test.go`, `internal/fleet/audit_test.go` (transisi ilegal ditolak, duplikat idempotency = job sama, retensi memangkas lama, secret tak bocor)

**Interfaces:**
- Consumes: `fleet.Registry` (Task 2).
- Produces: `fleet.Jobs` (Submit, Get, Complete), `fleet.Audit` (Log) dipakai Task 5.

- [ ] **Step 1: Write failing tests** (state ilegal + idempotency + sanitasi, kode eksplisit per kasus)
- [ ] **Step 2: Run** `go test ./internal/fleet -v` → FAIL
- [ ] **Step 3: Implement** (append-only file, lock per file, rotasi by count+age)
- [ ] **Step 4: Run** → PASS
- [ ] **Step 5: Commit** `feat(fleet): jobs state machine and audit JSONL with retention`

### Task 4: Agent transport + HMAC + revokasi

**Files:**
- Create: `internal/fleet/auth.go` (HMAC-SHA256 atas arah+op+body_sha256+timestamp+message_id; cache message_id anti-replay window expiry; cek revokasi)
- Create: `internal/fleet/agent.go` (handshake hello, heartbeat loop, dispatch job, backoff+jitter, kirim accepted/running/succeeded/failed)
- Create: `cmd/fleet-agent/main.go` (biner: enroll via token, TLS outbound, Docker socket)
- Create: `internal/fleet/docker.go` (interface DockerClient: List/Inspect/Start/Stop/Restart/Logs + FakeDocker untuk test)
- Test: `internal/fleet/auth_test.go` (replay ditolak, lintas-endpoint ditolak, expired ditolak, revoked connect ditolak + revoked mid-stream tak terima job)

**Interfaces:**
- Consumes: `fleet.Registry`, `fleet.Jobs`.
- Produces: channel agent↔kontrol dipakai Task 5.

- [ ] **Step 1: Write failing tests** (5 kasus di atas, satu fungsi test per kasus)
- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** → PASS
- [ ] **Step 5: Commit** `feat(fleet): agent transport with HMAC replay protection and two-point revocation`

### Task 5: Fleet server (mux terpisah) + wiring

**Files:**
- Create: `internal/fleet/server.go` (mux sendiri, middleware recover + limit body, route `GET /api/v1/fleet/status`, `GET/POST enrollments`, `GET/DELETE servers`, `GET containers`, `GET detail`, `GET logs`, `POST start/stop/restart` → 202+job_id, `GET jobs/{id}`, `GET audit-events`; tiap route cek `authz.Authorizer` resource `fleet_*`)
- Modify: `cmd/server/main.go` (buka listener Fleet bila enabled; tutup total bila disabled)
- Modify: `internal/web/web.go` (tambah field `fleetEnabled` ke `GET /api/v1/info`)
- Test: `internal/fleet/server_test.go` (tabel authz viewer/operator/admin × tiap route; FR-001: disabled → semua route 404/403; audit tertulis tiap mutasi)

**Interfaces:**
- Consumes: Task 1–4.
- Produces: endpoint hidup + flag info dipakai Task 6.

- [ ] **Step 1: Write failing tests** (tabel authz + FR-001 + audit)
- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** `go test -count=1 ./internal/fleet ./cmd/server ./internal/web` → PASS
- [ ] **Step 5: Commit** `feat(fleet): isolated fleet server with shared-authz and FR-001 gate`

### Task 6: UI zona Fleet

**Files:**
- Modify: `web/dist/index.html` (sakelar zona topbar Kafka|Fleet; zona Fleet: Servers/Containers/Jobs/Audit/Settings; baca flag dari `/api/v1/info`; disabled = sakelar hilang + `#/fleet/...` = view disabled tanpa API call; dialog konfirmasi ketik nama untuk stop/restart/revoke; status teks+ikon SVG)
- Test: manual checklist (sakelar pindah zona + hash update; disabled = zona hilang; keyboard penuh; reduced-motion hentikan polling)

**Interfaces:**
- Consumes: flag info Task 5, endpoint Task 5.

- [ ] **Step 1: Implementasi zona + flag** (tak ada unit test SPA; verifikasi via browser/`curl`)
- [ ] **Step 2: Verifikasi** broker lokal: enabled → zona muncul; disabled → zona hilang + `curl /api/v1/fleet/servers` → 404/403
- [ ] **Step 3: Commit** `feat(fleet): zone switcher and Fleet pages in embedded UI`

### Task 7: Hardening + packaging + docs

**Files:**
- Modify: `internal/fleet/*.go` (rate limit enroll + operasi mahal, batas job/agent/koneksi, sanitasi error envelope `code/message/request_id`)
- Create: `cmd/fleet-agent` systemd unit contoh + `FLEET_RUNBOOK.md` (install, TLS, backup/restore metadata, upgrade/rollback, troubleshooting)
- Modify: `ROADMAP.md` (tandai Fleet masuk pre-v1.0)
- Test: `go test ./...`, `go test -race ./internal/fleet`, `INTEROP_DOCKER=1 test/interop/run.sh` (broker tak regresi), review P0 PRD §13

- [ ] **Step 1: Rate limit + batas resource + envelope error**
- [ ] **Step 2: Runbook + systemd unit**
- [ ] **Step 3: Full gate** (`go test ./...`, race fleet, interop native)
- [ ] **Step 4: Commit** `feat(fleet): hardening, packaging and runbook`
