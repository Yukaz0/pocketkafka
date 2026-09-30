#!/bin/sh
# Start the broker as an unprivileged user.
#
# The image starts as root for one reason only: to take ownership of the data
# directory. A volume created by an earlier image is owned by root, so switching
# the user without this step would leave the broker unable to write its segments
# and indexes. After that the privileges are dropped with su-exec, and every
# process that serves traffic runs as the broker user. Start the container with
# --user and none of this runs: nothing here needs root.
set -e

DATA_DIR="${KAFKA_DATA_DIR:-/var/lib/pocketkafka/data}"
BROKER_USER="${POCKETKAFKA_USER:-pocketkafka}"

if [ "$(id -u)" != "0" ]; then
	exec /usr/local/bin/pocketkafka "$@"
fi

if [ ! -d "$DATA_DIR" ]; then
	mkdir -p "$DATA_DIR"
fi

# Chown recursively only when it is needed: the directory itself is stat-ed
# instead of walking a possibly large volume on every container start.
uid="$(id -u "$BROKER_USER")"
if [ "$(stat -c %u "$DATA_DIR")" != "$uid" ]; then
	echo "entrypoint: taking ownership of $DATA_DIR for $BROKER_USER (uid $uid)"
	chown -R "$BROKER_USER:$BROKER_USER" "$DATA_DIR"
fi

exec su-exec "$BROKER_USER" /usr/local/bin/pocketkafka "$@"
