# Kebijakan Keamanan

## Versi yang didukung

PocketKafka belum punya rilis stabil ber-versi. Hanya **commit terbaru di branch
`master`** yang diperbaiki; tag lama tidak menerima backport.

| Versi | Didukung |
|-------|----------|
| `master` (HEAD) | ✅ |
| Tag/rilis lama | ❌ |

## Melaporkan kerentanan

**Jangan** buka issue publik untuk kerentanan.

1. Utamakan **GitHub Private Vulnerability Reporting**:
   tab *Security* → *Report a vulnerability* pada repo `Yukaz0/pocketkafka`.
2. Alternatif surel: `security@example.com` (placeholder — ganti dengan alamat
   nyata sebelum repo dipublikasikan).

Sertakan bila ada:

- versi/commit yang terpengaruh,
- langkah reproduksi atau bukti konsep,
- dampak yang diperkirakan (RCE, auth bypass, DoS, kebocoran data),
- saran perbaikan (opsional).

Jangan sertakan data pihak ketiga atau kredensial produksi. Uji hanya pada
instance milik sendiri.

## Ekspektasi respons

| Tahap | Target |
|-------|--------|
| Konfirmasi penerimaan laporan | 3 hari kerja |
| Penilaian awal + severity | 7 hari kerja |
| Perbaikan untuk isu High/Critical | 30 hari |
| Perbaikan untuk isu Medium/Low | rilis berikutnya (tanpa tenggat keras) |

Kami akan memberi tahu saat perbaikan dirilis dan memberi kredit kecuali kamu
minta anonim. Mohon beri waktu perbaikan sebelum pengungkapan publik
(*coordinated disclosure*).

## Cakupan

**Termasuk:** kode di repo ini (broker Kafka, dashboard web, Schema Registry,
REST/MQTT gateway, storage), Dockerfile, workflow CI, dan konfigurasi default.

**Tidak termasuk:**

- kerentanan di dependensi pihak ketiga (laporkan ke upstream; repo ini nol
  dependensi Go) — kecuali cara pakainya di sini memperbesar dampak,
- salah konfigurasi oleh pengguna (mis. mengekspos dashboard tanpa auth),
- deployment yang sengaja dijalankan tanpa TLS/ACL,
- social engineering dan serangan fisik.

Catatan: repositori ini tidak memuat catatan tinjauan keamanan internal. Temuan
yang sudah ditangani di kode (lihat riwayat commit) tidak perlu dilaporkan ulang;
laporan hanya berguna untuk masalah yang masih terbuka di `master`.
