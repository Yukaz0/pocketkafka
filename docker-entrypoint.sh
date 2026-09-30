#!/bin/sh
# Run the broker as an unprivileged user. The image starts as root only to take
# ownership of the data directory (a volume from an earlier image is root-owned),
# then drops privileges with su-exec. Started with --user, none of this runs.
set -e

DATA_DIR="${KAFKA_DATA_DIR:-/var/lib/pocketkafka/data}"
BROKER_USER="${POCKETKAFKA_USER:-pocketkafka}"

if [ "$(id -u)" != "0" ]; then
	exec /usr/local/bin/pocketkafka "$@"
fi

if [ ! -d "$DATA_DIR" ]; then
	mkdir -p "$DATA_DIR"
fi

# Chown only when the directory is not already ours: stat the directory instead
# of walking a possibly large volume on every start.
uid="$(id -u "$BROKER_USER")"
if [ "$(stat -c %u "$DATA_DIR")" != "$uid" ]; then
	echo "entrypoint: taking ownership of $DATA_DIR for $BROKER_USER (uid $uid)"
	chown -R "$BROKER_USER:$BROKER_USER" "$DATA_DIR"
fi

exec su-exec "$BROKER_USER" /usr/local/bin/pocketkafka "$@"
