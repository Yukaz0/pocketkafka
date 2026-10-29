# MCP Server — PocketKafka

Server MCP (Model Context Protocol) bikin broker bisa dipakai agen AI
(Claude, Copilot, Cursor, dsb.) untuk observasi dan operasi Kafka
pakai bahasa natural. Zero-dep tetap: stdlib saja
(`encoding/json` + `bufio` + `net/http`).

## Arsitektur

- Biner baru: `cmd/mcp-server` (nama biner: `pocketkafka-mcp`).
- Transport: **stdio** (JSON-RPC 2.0, newline-delimited). Satu-satunya
  transport Fase 1.
- Peran: **client** ke broker, bukan bagian broker. Pakai `pkg/client`
  (wire protocol) + HTTP ke Schema Registry. Lifecycle pisah — broker
  tetap ramping, MCP crash tidak ganggu broker.
- Protocol version: `2025-06-18`.
- Method wajib: `initialize`, `tools/list`, `tools/call`, `ping`.
  Tambahan full-AI: `resources/list`, `resources/read`, `prompts/list`,
  `prompts/get`.
- Satu file implementasi: `cmd/mcp-server/main.go` + `main_test.go`
  (golden `initialize` + dispatch tiap tool + error envelope).

```
agen AI --stdio--> pocketkafka-mcp --wire 9092--> broker
                              \--http 8081--> schema registry
```

`ponytail:` Streamable HTTP belakangan untuk agen remote —
reuse `internal/gateway/rest.go`. Cap 30 koneksi.

## Tools — full support AI

Semua operasi yang `kctl` dan REST gateway bisa, agen juga bisa.
Default **read-only**. Write di belakang `--allow-write`.

### Observasi (read-only, selalu aktif)

| Tool | Sumber existing | Input | Output |
|---|---|---|---|
| `list_topics` | `ListTopics` + `Partitions` | `{}` | `[{name, partitions}]` |
| `describe_topic` | `Partitions` + `QueryOffset(-2/-1)` | `{topic}` | partisi + earliest/LEO per partisi |
| `fetch_messages` | `FetchRequest` via `RoundTrip` mentah (tanpa join group, tanpa commit) | `{topic, partition?, offset?, limit?}` | `[{offset, key, value, headers}]` |
| `list_consumer_groups` | `ListGroups` + `DescribeGroup` | `{}` | `[{group_id, state, members}]` |
| `describe_consumer_group` | `CommittedOffset` + `QueryOffset` (logika `kctl/groups.go`) | `{group_id}` | member + lag per topic-partisi |
| `list_schemas` | `GET /subjects` | `{}` | `[subject]` |
| `get_schema` | `GET /subjects/{s}/versions/{v}`, `GET /schemas/ids/{id}` | `{subject?, version?, id?}` | skema JSON |
| `broker_health` | Metadata + `ListOffsets` (hitung topik/partisi/pesan) | `{}` | ringkasan cluster (topik, partisi, pesan, uptime) |

Skema input kunci:

```json
// fetch_messages — paling sering dipakai agen
{"topic": "string (wajib)", "partition": "int, default 0",
 "offset": "int, opsional; kosong = max(LEO-limit, earliest)",
 "limit": "int, default 20, max 100"}
// describe_topic
{"topic": "string (wajib)"}
// describe_consumer_group
{"group_id": "string (wajib)"}
// get_schema — isi salah satu
{"subject": "string", "version": "string|int", "id": "int"}
```

Cap anti-ledakan-konteks: `limit` max 100, value truncate 4 KB/pesan
(ditandai `"truncated": true`), total respons max ±100 KB.

### Operasi (gated `--allow-write`)

| Tool | Sumber existing | Catatan |
|---|---|---|
| `produce_message` | `Producer.SendSync` (`{topic, value, key?, partition?, headers?}`) | value string/JSON |
| `create_topic` / `delete_topic` | `CreateTopic` / `DeleteTopic` | delete = destruktif, log audit |
| `register_schema` | `POST /subjects/{s}/versions` (`{subject, schema, schema_type}`) | AVRO/JSON saja |
| `reset_consumer_offset` | `ResetOffset` | paling berbahaya — wajib konfirmasi eksplisit tiap panggilan |
| `delete_consumer_group` | `DeleteGroup` | destruktif, log audit |

### Resources (konteks pasif untuk agen)

| URI | Isi |
|---|---|
| `kafka://topics` | daftar topik (refresh tiap read) |
| `kafka://topics/{topic}` | metadata + LEO/earliest per partisi |
| `kafka://schemas` | daftar subject |
| `kafka://schemas/{subject}` | versi terbaru skema |
| `kafka://health` | ringkasan cluster |

### Prompts (alur siap pakai)

| Prompt | Isi |
|---|---|
| `investigate-lag` | langkah diagnosa lag: `describe_consumer_group` → `describe_topic` → `fetch_messages` ekor |
| `inspect-topic` | `describe_topic` → `get_schema` (jika subject cocok) → `fetch_messages` sampel |
| `publish-event` | panduan `produce_message` + cek `fetch_messages` (perlu `--allow-write`) |

## Error envelope

- `-32700` parse error, `-32600` request invalid, `-32601` unknown
  method/tool, `-32602` param salah (sebut nama param + harapan).
- `-32000` broker error — bungkus `KafkaError` + pesan asli,
  jangan telan kode wire-nya (agen butuh kode untuk diagnosa).
- Tool sukses tapi hasil kosong → kembalikan `[]`, bukan error
  (bedakan "tidak ada data" vs "gagal").

## Config & registrasi

Flag/env:

```
-b localhost:9092                  (default; lawan broker lokal)
--registry http://localhost:8081   POCKETKAFKA_REGISTRY
--allow-write                      aktifkan tools write
--auth-token ...                   POCKETKAFKA_MCP_TOKEN
--timeout 10s                      POCKETKAFKA_MCP_TIMEOUT
```

Token dikirim agen di `initialize` params: `{"token": "..."}`.
Tanpa token cocok → `-32001 unauthorized`. Tanpa `--allow-write`,
tools write tolak `-32002`.

## Coba langsung

```bash
go build -o /tmp/pocketkafka-mcp ./cmd/mcp-server
echo '{"jsonrpc":"2.0","id":1,"method":"initialize"}' | /tmp/pocketkafka-mcp -b localhost:9092
echo '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"broker_health","arguments":{}}}' \
  | /tmp/pocketkafka-mcp -b localhost:9092
```

Bind localhost default. Registrasi client:

```json
// .mcp.json (repo) atau claude_desktop_config.json
{"mcpServers": {"pocketkafka": {
  "command": "pocketkafka-mcp",
  "args": ["-b", "localhost:9092"],
  "env": {"POCKETKAFKA_REGISTRY": "http://localhost:8081"}
}}}
```

## Keamanan

MCP = buka broker ke agen AI. Tanpa auth = lubang.

1. Token bearer sejak hari-1 (`--auth-token`, env). Tolak request tanpa
   token yang cocok.
2. Read-only default. `--allow-write` eksplisit, dan `delete_topic` /
   `reset_consumer_offset` tetap minta konfirmasi per panggilan
   (mekanisme konfirmasi milik host MCP).
3. Bind `127.0.0.1` default. Listen publik wajib + token + TLS.
4. Timeout 10 s per panggilan broker. Jangan retry write otomatis
   (risiko duplikasi produce) — kembalikan error, biar agen yang putuskan.
5. Audit log: setiap tool write catat (waktu, tool, argumen ringkas)
   ke stderr terstruktur atau file `--audit-log`.

## Testing

- `main_test.go`: golden `initialize` handshake, dispatch tiap tool
  (mock transport atau broker ephemeral), error envelope `-32602` /
  `-32000`, cap truncate 4 KB, `--allow-write` tolak write saat off.
- e2e: `internal/e2e/mcp_test.go` — spawn server + broker ephemeral,
  `list_topics` → `produce_message` → `fetch_messages` roundtrip.
- `go vet` + `gofmt` bersih (CI sudah cek).

## Urutan bangun

1. `cmd/mcp-server/main.go`: stdio loop + `initialize`/`tools/list`/`tools/call`.
2. Tools read-only (tabel observasi) + cap konteks.
3. Resources + prompts.
4. Tools write di belakang `--allow-write` + audit log.
5. `docs/mcp.md` ini + `.mcp.json` contoh + e2e.

`skipped: Streamable HTTP transport, sampling LLM, base64 value —
tambah saat agen butuh.`
