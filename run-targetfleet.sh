#!/usr/bin/env bash
# run-targetfleet.sh — onboard satu host target ke PocketKafka Fleet Controller.
#
# Alur: login web API -> baca agent_id state remote + registry controller ->
#   tentukan enroll dari KECOCOKAN keduanya -> terbitkan token bila perlu ->
#   pasang biner (unduh GitHub Release di target) -> enroll satu-kali via SSH ->
#   pasang systemd service -> verifikasi dari controller.
#
# Token HANYA diterbitkan bila enroll benar-benar dibutuhkan; state stale
# di-backup lalu di-reset sebelum enroll ulang (tak pernah dipakai diam-diam).
#
# Contoh:
#   export PK_ADMIN_PASSWORD="$(cat secrets/admin_password.txt)"
#   ./run-targetfleet.sh --target root@<target-host> --server-id <id> \
#     --controller-url http://<controller-host>:8080
set -euo pipefail

TARGET=""
SERVER_ID=""
WEB_URL="http://127.0.0.1:8080"
CONTROLLER_URL=""
SSH_PORT="22"
ARCH="auto"
TOKEN_TTL="15m"
RELEASE="latest"
REPO="Yukaz0/pocketkafka"
BINARY=""
FORCE_REENROLL=0
SUDO=""
DRY_RUN=0
SELF_TEST=0
PASSWORD_ENV="PK_ADMIN_PASSWORD"
PASSWORD_FILE=""
REMOTE_STATE="/var/lib/fleet-agent/agent-state.json"

usage() {
  cat <<'EOF'
usage: run-targetfleet.sh --target <user@host> --server-id <id> [opsi]

wajib:
  --target <user@host>     SSH target tempat fleet-agent dipasang
  --server-id <id>         ID server (huruf/angka/-/_. ; dipakai sbg prefix agent_id)

opsi:
  --web <url>              web API controller (default http://127.0.0.1:8080)
  --controller-url <url>   URL controller dari sudut pandang target
                           (default = --web dgn host 127.0.0.1/localhost diganti IP LAN)
  --arch amd64|arm64|auto  arsitektur biner target (default auto via uname -m)
  --release <tag>|latest|local
                           sumber biner: tag GitHub Release, "latest" (via API GitHub),
                           atau "local" = go build dari repo ini (default latest)
  --repo <slug>            repo GitHub rilis (default Yukaz0/pocketkafka)
  --binary <path>          kirim biner lokal via scp, lewati unduhan release
                           (fallback offline)
  --force-reenroll         backup+reset state remote walau sudah cocok, enroll ulang
  --token-ttl <dur>        masa berlaku token, mis. 15m, 30m (default 15m)
  --ssh-port <port>        port SSH target (default 22)
  --sudo                   awali perintah remote dgn sudo (bila user target bukan root)
  --password-file <path>   berkas berisi password admin web (alternatif env)
  --dry-run                cetak rencana saja, tanpa eksekusi
  --self-test              uji unit logika decide_enroll, tanpa jaringan/SSH
  -h, --help               bantuan ini

env:
  PK_ADMIN_PASSWORD        password admin web API (wajib bila tanpa --password-file)
EOF
}

# decide_enroll <remote_agent_id> <registry_agent_id> <force 0|1>
# Menentukan apakah enroll ulang dibutuhkan dari KECOCOKAN keduanya.
# Output: "<aksi> <alasan>" dgn aksi = skip|enroll.
decide_enroll() {
  local remote="${1:-}" reg="${2:-}" force="${3:-0}"
  if [ "$force" = 1 ]; then echo "enroll forced"; return 0; fi
  if [ -n "$remote" ] && [ "$remote" = "$reg" ]; then echo "skip consistent"; return 0; fi
  if [ -z "$remote" ] && [ -z "$reg" ]; then echo "enroll fresh"; return 0; fi
  if [ -n "$remote" ] && [ -z "$reg" ]; then echo "enroll orphan-state"; return 0; fi
  if [ -z "$remote" ] && [ -n "$reg" ]; then echo "enroll orphan-registry"; return 0; fi
  echo "enroll stale"
}

self_test() {
  local fail=0
  chk() { # $1 desc $2 got $3 want
    if [ "$2" = "$3" ]; then echo "ok: $1";
    else echo "FAIL: $1 (dapat '$2', mau '$3')"; fail=1; fi
  }
  chk "konsisten -> skip" "$(decide_enroll agent-x-1 agent-x-1 0)" "skip consistent"
  chk "keduanya kosong -> enroll fresh" "$(decide_enroll "" "" 0)" "enroll fresh"
  chk "state ada, registry tak kenal -> enroll + backup" "$(decide_enroll agent-x-9 "" 0)" "enroll orphan-state"
  chk "registry ada, state hilang -> enroll (token wajib terbit)" "$(decide_enroll "" agent-x-1 0)" "enroll orphan-registry"
  chk "id beda -> enroll stale" "$(decide_enroll agent-x-1 agent-x-2 0)" "enroll stale"
  chk "force walau cocok -> enroll" "$(decide_enroll agent-x-1 agent-x-1 1)" "enroll forced"
  return $fail
}

while [ $# -gt 0 ]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --server-id) SERVER_ID="$2"; shift 2 ;;
    --web) WEB_URL="$2"; shift 2 ;;
    --controller-url) CONTROLLER_URL="$2"; shift 2 ;;
    --arch) ARCH="$2"; shift 2 ;;
    --token-ttl) TOKEN_TTL="$2"; shift 2 ;;
    --release) RELEASE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --binary) BINARY="$2"; shift 2 ;;
    --force-reenroll) FORCE_REENROLL=1; shift ;;
    --ssh-port) SSH_PORT="$2"; shift 2 ;;
    --sudo) SUDO="sudo"; shift ;;
    --password-file) PASSWORD_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "opsi tak dikenal: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ "$SELF_TEST" = 1 ] && { self_test; exit $?; }

[ -n "$TARGET" ] || { echo "--target wajib diisi" >&2; exit 2; }
[ -n "$SERVER_ID" ] || { echo "--server-id wajib diisi" >&2; exit 2; }
case "$SERVER_ID" in
  *[!A-Za-z0-9._-]*|"") echo "server-id hanya boleh huruf/angka/-/_/." >&2; exit 2 ;;
esac

if [ -n "$PASSWORD_FILE" ]; then
  ADMIN_PASSWORD="$(cat "$PASSWORD_FILE")"
elif [ -n "${!PASSWORD_ENV:-}" ]; then
  ADMIN_PASSWORD="${!PASSWORD_ENV}"
else
  echo "isi $PASSWORD_ENV atau --password-file dulu" >&2; exit 2
fi

# URL controller versi target: ganti loopback dgn IP LAN host ini.
if [ -z "$CONTROLLER_URL" ]; then
  LAN_IP="$(hostname -I 2>/dev/null | tr ' ' '\n' | grep -v '^127\.' | head -1 || true)"
  CONTROLLER_URL="$(echo "$WEB_URL" | sed -E "s#://(127\.0\.0\.1|localhost)(:[0-9]+)?#://${LAN_IP:-127.0.0.1}\2#")"
fi
case "$CONTROLLER_URL" in
  *://127.0.0.1*|*://localhost*) echo "controller-url menunjuk loopback ($CONTROLLER_URL); target tak bisa enroll ke sana. Beri --controller-url http://<IP-LAN-controller>:8080" >&2; exit 2 ;;
esac

for cmd in curl jq ssh; do
  command -v "$cmd" >/dev/null || { echo "butuh $cmd di PATH" >&2; exit 1; }
done
if [ -n "$BINARY" ] || [ "$RELEASE" = local ]; then
  command -v scp >/dev/null || { echo "butuh scp di PATH untuk --binary/--release local" >&2; exit 1; }
fi
if [ -z "$BINARY" ] && [ "$RELEASE" = local ]; then
  command -v go >/dev/null || { echo "butuh go di PATH untuk --release local" >&2; exit 1; }
fi
[ -f cmd/fleet-agent/fleet-agent.service ] || { echo "jalankan dari root repo go-kafka-neu" >&2; exit 2; }

SSH="ssh -p $SSH_PORT $TARGET"
JAR="$(mktemp -t pk-fleet-cookies.XXXXXX)"
BIN_TMP=""
cleanup() { rm -f "$JAR" ${BIN_TMP:+$BIN_TMP}; }
trap cleanup EXIT

API_H=(-sS -m 15 -H 'Content-Type: application/json' -H 'X-PocketKafka-Request: 1')
api() { # $1 method $2 path [$3 body]
  if [ "${3:-}" = "" ]; then
    curl "${API_H[@]}" -b "$JAR" -c "$JAR" -X "$1" "$WEB_URL$2"
  else
    curl "${API_H[@]}" -b "$JAR" -c "$JAR" -X "$1" "$WEB_URL$2" -d "$3"
  fi
}

log() { echo "[run-targetfleet] $*"; }

if [ "$DRY_RUN" = 1 ]; then
  cat <<EOF
[rencana]
  login admin      : $WEB_URL/api/v1/auth/login
  cocokkan         : agent_id state $TARGET:$REMOTE_STATE vs registry server_id=$SERVER_ID
  enroll bila      : tak cocok / state hilang / registry hilang / --force-reenroll
  token            : diterbitkan HANYA bila enroll (ttl=$TOKEN_TTL)
  biner            : ${BINARY:+scp lokal ($BINARY)}${BINARY:-unduh di target dari github.com/$REPO releases ($RELEASE), verifikasi SHA256SUMS}
  service          : via SSH stdin -> /etc/systemd/system/fleet-agent.service
EOF
  exit 0
fi

log "login admin ke $WEB_URL"
LOGIN_OUT="$(api POST /api/v1/auth/login "{\"username\":\"admin\",\"password\":\"$ADMIN_PASSWORD\"}")"
echo "$LOGIN_OUT" | jq -e '.status == "ok"' >/dev/null \
  || { echo "login gagal: $LOGIN_OUT" >&2; exit 1; }

log "baca registry + state remote"
REG_AGENT="$(api GET /api/v1/fleet/servers | jq -r --arg id "$SERVER_ID" '.servers[]? | select(.id == $id) | .agent_id // empty')"
REMOTE_JSON="$($SSH "cat $REMOTE_STATE 2>/dev/null" || true)"
REMOTE_AGENT="$(printf '%s' "$REMOTE_JSON" | jq -r '.agent_id // empty' 2>/dev/null || true)"
log "registry agent_id=[${REG_AGENT:-kosong}] remote agent_id=[${REMOTE_AGENT:-kosong}]"

read -r DECISION REASON <<<"$(decide_enroll "$REMOTE_AGENT" "$REG_AGENT" "$FORCE_REENROLL")"
log "keputusan: $DECISION ($REASON)"

TOKEN=""
if [ "$DECISION" = enroll ]; then
  log "menerbitkan token untuk server_id=$SERVER_ID (ttl=$TOKEN_TTL)"
  ENROLL_OUT="$(api POST /api/v1/fleet/enrollments "{\"server_id\":\"$SERVER_ID\",\"duration\":\"$TOKEN_TTL\"}")"
  TOKEN="$(echo "$ENROLL_OUT" | jq -r '.token // empty')"
  [ -n "$TOKEN" ] || { echo "gagal terbitkan token: $ENROLL_OUT" >&2; exit 1; }
  log "token terbit, segera dipakai sebelum kedaluwarsa ($TOKEN_TTL)"
else
  log "enrollment dilewati; kredensial remote sudah cocok dgn registry"
fi

# Arsitektur target.
if [ "$ARCH" = auto ]; then
  MACHINE="$($SSH "uname -m")"
  case "$MACHINE" in
    x86_64) ARCH=amd64 ;;
    aarch64|arm64) ARCH=arm64 ;;
    *) echo "arsitektur tak dikenal: $MACHINE (pakai --arch)" >&2; exit 1 ;;
  esac
  log "arsitektur target: $MACHINE -> $ARCH"
fi

# Siapkan sumber biner: unduhan release di target (default), atau scp lokal.
USE_SCP=0
if [ -n "$BINARY" ]; then
  [ -f "$BINARY" ] || { echo "biner tak ditemukan: $BINARY" >&2; exit 1; }
  log "kirim biner lokal ke $TARGET:/tmp/fleet-agent-new"
  scp -P "$SSH_PORT" "$BINARY" "$TARGET:/tmp/fleet-agent-new"
  USE_SCP=1
else
  case "$RELEASE" in
    latest)
      log "cari tag rilis terbaru $REPO"
      RELEASE="$(curl -sS -m 15 "https://api.github.com/repos/$REPO/releases/latest" | jq -r '.tag_name // empty')"
      [ -n "$RELEASE" ] || { echo "gagal baca rilis terbaru; beri --release <tag> atau --binary" >&2; exit 1; }
      log "rilis terbaru: $RELEASE"
      ;;
    local)
      BIN_TMP="$(mktemp -t fleet-agent-bin.XXXXXX)"
      log "build biner linux/$ARCH dari repo ini"
      GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0 go build -o "$BIN_TMP" ./cmd/fleet-agent
      log "kirim biner lokal ke $TARGET:/tmp/fleet-agent-new"
      scp -P "$SSH_PORT" "$BIN_TMP" "$TARGET:/tmp/fleet-agent-new"
      USE_SCP=1
      ;;
  esac
fi

log "install biner, enroll (bila perlu), pasang service"
if [ -n "$SUDO" ]; then REMOTE_SH="sudo bash -s"; else REMOTE_SH="bash -s"; fi
$SSH $REMOTE_SH <<EOF
set -e
STATE=$REMOTE_STATE
NEED='$([ "$DECISION" = enroll ] && echo 1 || echo 0)'
TOKEN='$TOKEN'
CTRL='$CONTROLLER_URL'
REPO='$REPO'
TAG='$RELEASE'
ARCH='$ARCH'
USE_SCP='$USE_SCP'
command -v curl >/dev/null || { echo 'butuh curl di target' >&2; exit 1; }
if [ "\$USE_SCP" = 1 ]; then
  install -m 0755 /tmp/fleet-agent-new /usr/local/bin/fleet-agent
else
  ASSET="fleet-agent-linux-\$ARCH"
  BASE="https://github.com/\$REPO/releases/download/\$TAG"
  cd /tmp
  curl -fsSL -o "fleet-agent-new" "\$BASE/\$ASSET"
  curl -fsSL -o SHA256SUMS "\$BASE/SHA256SUMS.txt"
  # Entri SHA256SUMS berbentuk "dist/<asset>" (hasil `sha256sum dist/*`);
  # cocokkan basename lalu verifikasi TERHADAP berkas unduhan.
  EXPECTED="\$(awk -v a=\"\$ASSET\" '{ sub(/.*\//, \"\", \$2); if (\$2 == a) print \$1 }' SHA256SUMS | head -1)"
  [ -n "\$EXPECTED" ] || { echo "checksum untuk \$ASSET tak ada di SHA256SUMS" >&2; exit 1; }
  echo "\$EXPECTED  fleet-agent-new" | sha256sum -c -
  install -m 0755 /tmp/fleet-agent-new /usr/local/bin/fleet-agent
fi
mkdir -p /var/lib/fleet-agent /etc/fleet-agent
if [ "\$NEED" = 1 ]; then
  # Hentikan daemon dulu: kredensial lama yg masih jalan akan terus dipakai
  # bila service hanya di-enable tanpa restart.
  systemctl stop fleet-agent 2>/dev/null || true
  if [ -f "\$STATE" ]; then
    STALE="\$STATE.stale-\$(date +%Y%m%d%H%M%S)"
    mv "\$STATE" "\$STALE"
    echo "state stale di-backup ke \$STALE"
  fi
  timeout 25 /usr/local/bin/fleet-agent --token "\$TOKEN" --server "\$CTRL" --state "\$STATE" > /tmp/fleet-enroll.log 2>&1 || true
  grep -q 'fleet-agent active: agent_id=' /tmp/fleet-enroll.log || { echo 'ENROLL GAGAL:'; cat /tmp/fleet-enroll.log; exit 1; }
  grep 'fleet-agent active' /tmp/fleet-enroll.log
else
  echo "state cocok, enroll dilewati"
fi
cat > /etc/default/fleet-agent <<EOF2
FLEET_SERVER_URL="$CONTROLLER_URL"
FLEET_STATE_PATH="$REMOTE_STATE"
FLEET_DOCKER_SOCKET="/var/run/docker.sock"
EOF2
chmod 0600 /etc/default/fleet-agent
cat > /etc/systemd/system/fleet-agent.service <<'UNITEOF'
$(cat cmd/fleet-agent/fleet-agent.service)
UNITEOF
systemctl daemon-reload
if [ "\$NEED" = 1 ]; then
  systemctl enable fleet-agent
  systemctl restart fleet-agent
else
  systemctl enable --now fleet-agent
fi
systemctl is-active --quiet fleet-agent && echo 'service: active'
EOF

log "verifikasi dari controller (maks ~60 dtk)"
for _ in $(seq 1 12); do
  ROW="$(api GET /api/v1/fleet/servers | jq -c --arg id "$SERVER_ID" '.servers[]? | select(.id == $id)')"
  if [ -n "$ROW" ]; then
    echo "$ROW" | jq '{id, status, agent_id, updated_at}'
    STATUS="$(echo "$ROW" | jq -r '.status')"
    [ "$STATUS" = "online" ] && { log "SELESAI: target $TARGET terdaftar sebagai $SERVER_ID"; exit 0; }
  fi
  sleep 5
done
echo "server $SERVER_ID belum online setelah 60 dtk; cek 'journalctl -u fleet-agent' di target" >&2
exit 1
