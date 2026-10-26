# PocketKafka Fleet Runbook

Panduan operasional PocketKafka Fleet: instalasi agen, konfigurasi TLS, backup/restore metadata, upgrade/rollback, dan troubleshooting.

---

## 1. Arsitektur Singkat

- **PocketKafka Fleet Controller**: berjalan in-process di broker PocketKafka (port default `18090`), terisolasi di balik flag `fleet.enabled`.
- **Fleet Agent (`fleet-agent`)**: daemon ringan yang berjalan di setiap node target, membaca Docker socket lokal (`/var/run/docker.sock`), dan berkomunikasi keluar (outbound-only) ke Controller via HTTPS/TLS dengan autentikasi HMAC bertanda-tangan.

---

## 2. Instalasi & Setup Host Agent

### Persyaratan Node
- Linux (x86_64 / arm64) atau WSL2.
- Docker Engine aktif (`systemctl status docker`).
- Akses jaringan keluar ke endpoint controller Fleet (`https://<controller-host>:18090`).

### Langkah Instalasi
1. **Unduh Biner**:
   Unduh biner rilis `fleet-agent-linux-amd64` (atau `arm64`) dari GitHub Release dan simpan ke `/usr/local/bin/fleet-agent`:
   ```bash
   sudo install -m 0755 fleet-agent-linux-amd64 /usr/local/bin/fleet-agent
   fleet-agent -version
   ```

2. **Dapatkan Token Enrollment**:
   Di dashboard PocketKafka, buka zona **Fleet** -> **Servers** -> Klik **+ Enroll Server** (atau buat via API/CLI). Token 64-karakter hex berlaku selama 15 menit dan bersifat sekali pakai (*single-use*).

3. **Buat Direktori State & Konfigurasi**:
   ```bash
   sudo mkdir -p /var/lib/fleet-agent /etc/fleet-agent
   sudo chmod 0700 /var/lib/fleet-agent
   ```

4. **Konfigurasi Environment**:
   Salin berkas contoh ke `/etc/fleet-agent/agent.env`:
   ```bash
   sudo tee /etc/fleet-agent/agent.env << 'EOF'
   FLEET_SERVER_URL="https://controller.internal:18090"
   FLEET_ENROLL_TOKEN="<TOKEN_ENROLLMENT_DARI_DASHBOARD>"
   FLEET_STATE_PATH="/var/lib/fleet-agent/agent-state.json"
   FLEET_DOCKER_SOCKET="/var/run/docker.sock"
   # FLEET_CA_CERT="/etc/fleet-agent/ca.pem"
   EOF
   sudo chmod 0600 /etc/fleet-agent/agent.env
   ```

5. **Pasang Systemd Unit**:
   Salin `cmd/fleet-agent/fleet-agent.service` ke `/etc/systemd/system/fleet-agent.service`:
   ```bash
   sudo cp cmd/fleet-agent/fleet-agent.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now fleet-agent
   ```

6. **Verifikasi Status**:
   ```bash
   sudo systemctl status fleet-agent
   sudo journalctl -u fleet-agent -f
   ```
   Setelah agen berhasil enroll, berkas `/var/lib/fleet-agent/agent-state.json` akan terbuat secara atomik. Variabel `FLEET_ENROLL_TOKEN` di `/etc/fleet-agent/agent.env` boleh dikosongkan.

---

## 3. Konfigurasi TLS

Jika Controller Fleet mendengarkan pada antarmuka non-loopback (mis. `0.0.0.0:18090`), TLS wajib diaktifkan.

### Controller (`config.yaml`)
```yaml
fleet:
  enabled: true
  listen: "0.0.0.0:18090"
  secret_key: "${KAFKA_FLEET_SECRET_KEY}" # min 16 karakter untuk enkripsi registry AES-GCM
  tls:
    cert_file: "/etc/pocketkafka/certs/fleet.crt"
    key_file: "/etc/pocketkafka/certs/fleet.key"
    ca_file: "/etc/pocketkafka/certs/ca.crt" # opsional jika self-signed
```

### Validasi Agen ke Controller
Jika Controller menggunakan sertifikat self-signed atau private CA, pasang berkas CA root ke node agen:
```bash
# Di node agent
sudo cp ca.crt /etc/fleet-agent/ca.pem
```
Tambahkan di `/etc/fleet-agent/agent.env`:
```bash
FLEET_CA_CERT="/etc/fleet-agent/ca.pem"
```
Restart agen: `sudo systemctl restart fleet-agent`.

---

## 4. Backup & Restore Metadata

Metadata Fleet disimpan di dalam direktori `data_dir` PocketKafka (terpisah dari ACL broker):
1. `__fleet_agents.json` — Registry agen, host, status token, dan sealed HMAC secret key (AES-256-GCM).
2. `fleet_jobs.jsonl` — Log append-only riwayat eksekusi pekerjaan (start/stop/restart container).
3. `fleet_audit.jsonl` — Append-only audit trail operasional (siapa melakukan apa, target, hasil, correlation ID).

### Prosedur Backup
1. Jalankan snapshot atau salin berkas saat broker aktif (ketiganya aman dibaca secara atomik):
   ```bash
   mkdir -p /backup/fleet-$(date +%F)
   cp /var/lib/pocketkafka/data/__fleet_agents.json /backup/fleet-$(date +%F)/
   cp /var/lib/pocketkafka/data/fleet_jobs.jsonl /backup/fleet-$(date +%F)/
   cp /var/lib/pocketkafka/data/fleet_audit.jsonl /backup/fleet-$(date +%F)/
   ```
2. Pastikan menyimpan nilai `fleet.secret_key` atau `KAFKA_FLEET_SECRET_KEY` di tempat aman (kunci enkripsi AES-GCM).

### Prosedur Restore
1. Hentikan broker PocketKafka:
   ```bash
   sudo systemctl stop pocketkafka
   ```
2. Salin kembali berkas backup ke direktori data PocketKafka:
   ```bash
   cp /backup/fleet-YYYY-MM-DD/__fleet_agents.json /var/lib/pocketkafka/data/
   cp /backup/fleet-YYYY-MM-DD/fleet_jobs.jsonl /var/lib/pocketkafka/data/
   cp /backup/fleet-YYYY-MM-DD/fleet_audit.jsonl /var/lib/pocketkafka/data/
   sudo chown -R pocketkafka:pocketkafka /var/lib/pocketkafka/data/
   ```
3. Pastikan `fleet.secret_key` sama dengan yang digunakan saat backup.
4. Nyalakan kembali broker PocketKafka:
   ```bash
   sudo systemctl start pocketkafka
   ```
5. Agen yang sudah terdaftar akan otomatis tersambung kembali menggunakan kunci HMAC tersimpan.

---

## 5. Upgrade & Rollback

### Upgrade Agen
1. Unduh biner `fleet-agent` versi baru.
2. Hentikan agen sementara:
   ```bash
   sudo systemctl stop fleet-agent
   ```
3. Ganti biner:
   ```bash
   sudo install -m 0755 fleet-agent-linux-amd64 /usr/local/bin/fleet-agent
   ```
4. Nyalakan agen:
   ```bash
   sudo systemctl start fleet-agent
   ```
5. Berkas state `/var/lib/fleet-agent/agent-state.json` tetap utuh; agen melanjutkan koneksi tanpa perlu token baru.

### Rollback Agen
1. Ganti kembali biner `/usr/local/bin/fleet-agent` ke versi sebelumnya.
2. `sudo systemctl restart fleet-agent`.

---

## 6. Troubleshooting

### 1. `HMAC signature mismatch` atau `timestamp skew exceeded`
- **Penyebab**: Jam sistem pada node agen dan broker Controller berselisih lebih dari 5 menit (skema pencegahan replay attack).
- **Solusi**: Sinkronisasi NTP di kedua sisi:
  ```bash
  sudo systemctl restart systemd-timesyncd # atau chrony/ntp
  timedatectl status
  ```

### 2. `failed to connect to docker socket`
- **Penyebab**: Docker daemon mati atau izin socket tidak mencukupi.
- **Solusi**:
  ```bash
  sudo systemctl status docker
  # Uji akses docker socket:
  sudo docker ps
  ```

### 3. `rate limit exceeded for enroll` (HTTP 429)
- **Penyebab**: Melebihi kuota percobaan pendaftaran token dari IP yang sama (default 30 request/menit).
- **Solusi**: Tunggu 1 menit sebelum mencoba lagi atau gunakan token yang valid.

### 4. `active job limit reached` (HTTP 429)
- **Penyebab**: Terlalu banyak pekerjaan (jobs) yang berada dalam status *queued* atau *running* (batas default: 1000).
- **Solusi**: Periksa status agen target; jika agen offline, pekerjaan lama akan timeout setelah durasi timeout habis atau selesaikan antrian agen yang macet.

### 5. `server not found` atau `unknown server_id`
- **Penyebab**: Host target belum terdaftar atau telah di-revoke oleh administrator.
- **Solusi**: Periksa daftar server di menu Fleet -> Servers atau daftarkan ulang host melalui proses enrollment baru.
