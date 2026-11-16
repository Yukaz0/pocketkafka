# Operations

Scope: single-node PocketKafka. For the Fleet agent see `FLEET_RUNBOOK.md`.

## Replication and failover: non-goal

PocketKafka is a single broker. There is no replication, no leader election and
no automatic failover, and none is planned. `acks=-1` means the
record is on this node's disk (with `storage.sync_on_acks_all=true`), not on a
second node.

Availability model: one process, restarted by a supervisor (systemd
`Restart=on-failure`, Docker `restart: unless-stopped`). On restart the broker
recovers the log and consumer offsets from `data_dir`
(`TestBrokerRestartPreservesLogAndOffsets`). For protection against losing the
disk or the host, use the backup procedure below. If you need multi-node
durability, run Apache Kafka or Redpanda.

## What lives in `data_dir`

| Path | Content |
|---|---|
| `<topic>/<partition>/` | log segments and indexes |
| `__coordinator/` | committed consumer-group offsets (`offsets.gob` + `offsets.wal`) |
| `__acls.json` | ACLs |
| `__schemas.json` | Schema Registry subjects and IDs |
| `__secrets.key` | key that seals stored cluster credentials (when not supplied via `KAFKA_SECRETS_KEY`) |

A backup is a copy of this whole directory. It contains `__secrets.key` next to
the credentials it seals: treat the backup like a secret (restrictive
permissions, encrypted storage).

## Backup (cold, supported)

1. Stop the broker and wait for the process to exit (`SIGTERM`).
2. Copy the directory, preserving modes:

   ```bash
   tar -C "$(dirname "$DATA_DIR")" -czf pocketkafka-$(date +%F).tgz "$(basename "$DATA_DIR")"
   ```

3. Start the broker again.

Downtime is the length of the copy. Hot copies of a running broker are not a
supported or tested procedure: segments and the offset WAL are written
concurrently, so a plain `cp`/`tar` can capture them at different points. If you
cannot stop the broker, take an atomic filesystem snapshot (LVM, ZFS, btrfs) and
back up from the snapshot; recovery from it behaves like recovery from a crash
at that instant, so records acknowledged only under `acks=1` shortly before the
snapshot may be missing.

## Restore

1. Stop the broker. Move the damaged `data_dir` aside (do not delete it until
   the restore is verified).
2. Extract the backup into an empty `data_dir`:

   ```bash
   mkdir -p "$DATA_DIR" && tar -C "$(dirname "$DATA_DIR")" -xzf pocketkafka-YYYY-MM-DD.tgz
   ```

3. Start the broker with the same `storage.data_dir`. If you used
   `KAFKA_SECRETS_KEY` instead of `__secrets.key`, set the same value, or stored
   cluster credentials cannot be unsealed.
4. Verify:

   ```bash
   pkctl topics                              # topics present, log_end_offset as at backup time
   pkctl offset get --group G --topic T      # group offsets as at backup time
   pkctl produce T probe                     # next offset == previous log_end_offset
   ```

Data written after the backup is lost; clients resume from the restored
committed offsets and will re-read anything committed after the backup.

## What is tested

- `internal/e2e` `TestBackupRestoreFromColdCopy`: produce, commit a group
  offset, stop, copy `data_dir`, delete the original, restore into a fresh
  directory, start. Log end offset and committed offset match, and the next
  write lands at the expected offset.
- Manual drill with the release binary (this procedure verbatim, tar archive,
  original directory removed): all 5 records readable, `log_end_offset` 5, next
  produce got offset 5.
- Not covered: ACL, Schema Registry and secrets-key restore (they are plain
  files in the same directory but have no restore test), tiered (S3) storage,
  and hot copies.

## Monitoring

`deploy/grafana/dashboard.json` (import via the Grafana UI or HTTP API) charts
the `pocketkafka_*` series from `/metrics`. `TestDashboardMetricsExist` fails if
a panel queries a metric the broker no longer emits. Watch
`pocketkafka_disk_usage_ratio`: a full `data_dir` disk is the most likely cause
of an unplanned outage on a single node.
