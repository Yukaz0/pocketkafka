# PocketKafka Fleet — Design (disetujui 2026-10-26)

Basis: PRD `POCKETKAFKA_FLEET_PRD.md` + audit repo. Keputusan Q1–Q6 terkunci di bawah.
Pendekatan: **horizontal penuh** — satu desain utuh, dibangun sekaligus, integrasi di akhir.
(Fase F1–F4 di §5 adalah administratif, bukan slicing vertikal.)

## §1 Arsitektur — hibrida bersyarat (opsi C)

- `internal/fleet/`: coordinator, registry, jobs, audit. Kontrol in-process.
- **Mux Fleet terpisah** dari mux dashboard 8080. `fleet.enabled=false` = listener/route
  tak dibuka sama sekali (FR-001).
- Tiap handler Fleet: `recover()` + batas ukuran body + batas baris/byte log.
  Satu panic Fleet tidak boleh mematikan broker.
- `cmd/fleet-agent/`: biner sendiri, Linux + WSL, akses Docker socket standar.
- State terpisah dari `__acls.json` (rincian §3).

## §2 UI — sakelar zona Kafka | Fleet

- Sakelar zona di topbar, bukan item nav campur. Ganti zona = ganti nav + konten +
  hash (`#/fleet/servers`, `#/topics`). Breadcrumb tampilkan zona aktif.
- Zona Kafka (tak berubah): Topics, Groups, Data Browser, Schemas, Topology.
- Zona Fleet: Servers, Containers, Jobs, Audit, Settings-Fleet.
- Bahasa per zona beda (broker vs host); aksen sekunder beda, tetap flat tanpa gradient.
- Status server: teks + ikon SVG, bukan warna saja. Kontras 4.5:1, keyboard penuh,
  `prefers-reduced-motion` hentikan animasi polling.
- **FR-001**: status Fleet dari field publik `/api/v1/info` (tanpa cookie).
  Disabled = sakelar tak dirender; akses `#/fleet/...` langsung = view
  "Fleet disabled" tanpa panggil API. DoD: `curl /api/v1/fleet/*` saat disabled
  → 404/403, bukan 500.

## §3 Data & API

- Prefix `/api/v1/fleet` (skema PRD §10). Mutasi → `202 Accepted` + `job_id`;
  operator pantau status job (queued→running→succeeded/failed). Alasan: agent
  remote bisa lambat/offline.
- RBAC: role `viewer < operator < admin` adalah alias konfigurasi di atas
  `authz.Authorizer` yang sama dengan broker (satu sumber kebenaran, bukan store
  ganda). Resource baru: `fleet_server`, `fleet_container`, `fleet_job`,
  `fleet_audit`. Operasi: `describe` (viewer), `control` (operator: start/stop/
  restart/logs), `admin` (revoke, enroll, kelola). Fleet admin bukan otomatis
  cluster admin dan sebaliknya — grant eksplisit per resource.
- Storage 3 kelas (bukan satu file monolitik):
  1. **Registry** `__fleet_agents.json` — agent, kredensial (hash, bukan plaintext),
     enrollment. Jarang berubah, pola atomicfile seperti ACL.
  2. **Inventaris + heartbeat** — memori saja; persist opsional snapshot terakhir/agent.
  3. **Audit + job** `fleet_audit.jsonl`, `fleet_jobs.jsonl` — append-only + rotasi.
     Tiap entri audit: actor, target (server/container), waktu, hasil, correlation
     ID (FR-011). Log container dibatasi baris/byte + sanitasi secret metadata (FR-008).
- Retensi konfigurabel, default 30 hari / 10 ribu entri (mana dulu tercapai).

## §3b Konfigurasi listener Fleet

- Key `fleet.*`: `enabled` (default false), `listen` (default `127.0.0.1:18090`),
  `tls` (cert/key, wajib bila listen non-loopback), `heartbeat_timeout`,
  `retention_days`/`retention_entries`.
- Validasi di `config.Load` seperti field lain: listen publik tanpa TLS+auth =
  startup gagal. Aturan sama dengan `allow_insecure_public` broker.

## §4 Agent & protokol

- Agent: koneksi keluar TLS ke kontrol, heartbeat + reconnect backoff + jitter.
- Pesan bawa `protocol_version`, `message_id`, `issued_at`, expiry, correlation/job ID.
- Auth: token enrollment sekali-pakai → tukar kredensial HMAC; field identitas
  abstrak agar siap upgrade mTLS tanpa rombak protokol.
- **HMAC dihitung atas**: arah + op + SHA-256 body + timestamp + message_id.
  Agent cache message_id dalam window expiry → tolak replay/replay lintas endpoint.
- **Revokasi 2 titik**: saat agent connect (tolak stream baru) DAN sebelum kontrol
  kirim job ke stream terbuka (FR-012).
- Tanpa shell arbitrer. Tanpa `exec.Command` dengan string bebas.

## §5 Milestone & DoD (administratif, bangun sekaligus)

- F1: registry + enrollment + heartbeat 1 agent (read-only).
- F2: job start/stop/restart + gate authz minimal + audit append P0 bareng mutasi pertama.
- F3: UI zona + editor RBAC + ekspor/retensi audit.
- F4: packaging agent + beta tertutup.
- DoD tiap fase: `go test ./...`, `go test -race ./...`, interop native hijau,
  FR-001 disabled=mati total, review P0 PRD §13 sebelum beta.

## Keputusan terkunci

| # | Topik | Pilihan |
|---|-------|---------|
| Q1 | Penempatan | C hibrida-bersyarat (mux terpisah, recover+limit, state sendiri) |
| Q2 | Storage | Split 3 kelas (registry JSON, inventaris memori, audit/job JSONL) |
| Q3 | Auth agent | C (token→HMAC, siap mTLS) |
| Q4 | Platform | Linux + Docker Engine, WSL didukung |
| Q5 | Lisensi kode baru | C (community MIT, premium modul terpisah proprietary) |
| Q6 | Retensi | B (konfigurabel, default 30 hari / 10 ribu) |
