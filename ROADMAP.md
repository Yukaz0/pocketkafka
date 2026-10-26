# PocketKafka Roadmap

Sumber kebenaran status: kode + `test/interop/compatibility.yaml`.
Dokumen `docs/` di-ignore git (tidak ikut rilis); peta ini self-contained.

## Kini: v0.1.0 (ter-tag)

Broker single-node siap: wire protocol, UI, registry subset,
REST/MQTT gateway, compaction, S3 tiering (off default), SASL/SCRAM+TLS,
Prometheus `/metrics`, `/healthz`+`/livez`, provenance rilis.

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
