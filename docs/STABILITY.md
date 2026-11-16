# Stability contract (v1.0)

From v1.0, the surfaces below follow semantic versioning. A change that breaks
a client using only what is listed here requires a new major version. Anything
not listed is not covered and may change in a minor release.

## Frozen surfaces

### 1. Kafka wire protocol

The advertised API keys and their maximum versions, as in
`test/interop/compatibility.yaml` (`api_versions`):

Produce 3 (min 3), Fetch 5 (min 4), ListOffsets 5, Metadata 8, OffsetCommit 7,
OffsetFetch 5, FindCoordinator 2, JoinGroup 5, Heartbeat 3, LeaveGroup 3,
SyncGroup 3, DescribeGroups 3, ListGroups 2, SaslHandshake 1, ApiVersions 2,
CreateTopics 4, DeleteTopics 3, InitProducerID 1, SaslAuthenticate 1,
DeleteGroups 1.

- Allowed in a minor release: advertise a higher max version, add a new key.
- Breaking (major): remove a key, lower a max version, raise a min version,
  change the behavior of an advertised version.
- Enforced by `TestWireSurfaceFrozen` and `TestCompatibilityManifestMatchesWire`
  (`pkg/protocol`): the broker, the table in the test and the manifest must agree.
- Client support claims are the `supported` entries in the manifest (pocketkafka
  SDK, sarama, franz-go, kcat/librdkafka modern, kafkajs), each backed by an
  executable check.

### 2. REST proxy (`:8082`)

| Route | Contract |
|---|---|
| `POST /topics/{topic}/messages` | body `{"key","value","headers"}`; `201 {"topic","partition","offset"}`; `400` on bad JSON or topic name |
| `GET /topics/{topic}/messages?offset&limit&partition` | `200` array of `{"topic","partition","offset","key","value","headers"}`; `limit` 1-500 (default 50); `404` unknown topic/partition |

Enforced by `TestRESTProxyContractFrozen`. New optional fields and query
parameters may be added; existing ones are not renamed or removed.

### 3. Schema Registry subset (`:8081`)

`GET /subjects`, `POST /subjects/{subject}/versions`,
`GET /subjects/{subject}/versions`, `GET /subjects/{subject}/versions/{version}`,
`GET /schemas/ids/{id}`. Subset semantics (no compatibility checking, PROTOBUF
rejected) are documented in the README. Adding compatibility checking later is
additive only if it is opt-in.

## Explicitly not frozen

The web dashboard and its internal HTTP API, the MQTT bridge, Fleet (experimental,
off by default), `pkctl` output format, the `config.yaml` schema beyond keys
documented in the README, the on-disk layout (see `OPERATIONS.md` for the
supported backup procedure; format changes ship with a migration note), and
metric names (renames are called out in release notes).

## Transactions: permanently fail-closed

`AddPartitionsToTxn`, `AddOffsetsToTxn` and `EndTxn` are not advertised and
answer `UNSUPPORTED_VERSION`; the SDK returns `ErrTransactionsUnsupported`.
`InitProducerID` is advertised for idempotent producers only. Exactly-once
semantics across partitions are not provided. This is the v1.0 contract, not a
gap to be filled later: implementing transactions would be an additive minor
release and the manifest and `TestWireSurfaceFrozen` must be updated with it.

## Replication

Non-goal. See `OPERATIONS.md`.
