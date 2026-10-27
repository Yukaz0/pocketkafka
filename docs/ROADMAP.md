# PocketKafka Roadmap

Sumber kebenaran status: kode + `test/interop/compatibility.yaml`.
Dokumen `docs/` di-ignore git (tidak ikut rilis); peta ini self-contained.

## Kini: v0.1.1 (ter-tag)

Broker single-node siap: wire protocol, UI, registry subset,
REST/MQTT gateway, compaction, S3 tiering (off default), SASL/SCRAM+TLS,
Prometheus `/metrics`, `/healthz`+`/livez`, provenance rilis.
v0.1.1 update: Modular Web UI (ES modules), MQTT Studio zone mandiri,
Fleet server & token lifecycle management, flat core navigation, dan dynamic asset cache-buster.
## v0.2 — External interop

- `test/interop/run.sh` Docker matrix hijau: kcat/librdkafka, KafkaJS, franz-go.
- `compatibility.yaml`: `planned` → `supported` + hapus label "planned" di README.
- Done: `INTEROP_DOCKER=1 test/interop/run.sh` exit 0; `go test ./...` + `-race` tetap hijau.

## v0.3 — Operasional

- Replika / failover terdokumentasi (atau dinyatakan non-goal eksplisit).
- Backup-restore `data_dir` terdokumentasi + teruji.
- Dashboard Grafana selaras metrik `pocketkafka_*`.
- Done: runbook restore teruji dari backup nyata.

## v1.0 — Janji stabil

- API wire + REST yang diiklankan dibekukan; perubahan breaking naik major.
- Transaksi: implementasi penuh atau tetap fail-closed permanen (diputuskan eksplisit).
- Done: tidak ada `planned` tersisa di README; compatibility matrix penuh hijau.

## Fleet (Pre-v1.0 Experimental)

Subkomponen manajemen host dan container jarak jauh (`fleet-agent` + UI zona Fleet).
- **Status saat ini**: Pre-v1.0 / experimental preview (disabled secara default via `fleet.enabled: false`).
- **Cakupan fitur**:
  - Registry & enrollment agen aman via token sekali-pakai + pertukaran kredensial HMAC bertanda-tangan.
  - Lifecycle kontrol container Docker (start/stop/restart) berbasis antrean job async idempotensial.
  - Audit log append-only dengan rotasi dan correlation ID.
  - Rate limiting, batasan koneksi/job/agen, dan isolasi kegagalan (panic recovery di tiap handler).
  - UI zona Fleet di SPA terintegrasi dan sistem packaging systemd + GitHub Release artifact.
- **Kriteria stabil (v1.0)**: evaluasi ketahanan multi-host pada rilis v0.2–v0.3 sebelum promosi ke fitur stabil di v1.0.
