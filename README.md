# Pipeline ETL Citi Bike Jersey City

Tugas Big Data Minggu 10, Kelompok X: Herpratama Rafi Putranto (101032300076) dan Fandy Rifky Melani Saputra (101032300181).

Pipeline **ETL** (Extract → Transform → Load) yang berjalan otomatis di GitHub Actions. Data diambil dari tiga sumber, dibersihkan dan diolah di Python, lalu **hanya data yang sudah bersih** dimuat ke Supabase (PostgreSQL). Tidak ada input manual.

```
Trip data S3 (bulanan) ┐
GBFS station_status    ├─► GitHub Actions: extract.py ─► transform.py (R1–R7) ─► load.py ─► Supabase ETL
GBFS station_info      │                                                                 (fact_station_30min,
Open-Meteo (cuaca)     ┘                                                                  dim_station, weather_hourly)
```

## Struktur

| File | Isi |
|---|---|
| `etl/config.py` | URL sumber, zona waktu, batas aturan transformasi |
| `etl/extract.py` | Mengambil GBFS, file trip bulanan dari S3, cuaca Open-Meteo |
| `etl/transform.py` | Tujuh aturan transformasi (tanpa akses jaringan/database, bisa diuji) |
| `etl/load.py` | Upsert ke Supabase, log, penanda file yang sudah dimuat |
| `etl/quality.py` | Uji kualitas data setelah load |
| `etl/run_etl.py` | Integrasi E → T → L per job + pencatatan log |
| `etl/loop.py` | Loop tiap 10 menit di runner GitHub Actions |
| `tests/test_transform.py` | Uji unit setiap aturan transformasi |
| `sql/001_etl_schema.sql` | Skema tabel target |
| `notebooks/backfill_streaming_colab.ipynb` | Memuat snapshot lama dari Drive (sekali jalan) |

## Aturan transformasi

| Kode | Aturan | Fungsi |
|---|---|---|
| R1 | Hanya stasiun JC/HB yang aktif (`is_installed = 1`, kapasitas > 0) | `transform_station_info`, `transform_status_snapshot` |
| R2 | ID live (`short_name`) dipetakan ke ID historical; cadangan lewat nama | `transform_station_info`, `petakan_stasiun_trip` |
| R3 | Waktu ke America/New_York, dibulatkan ke slot 30 menit | `utc_ke_lokal`, `ke_slot` |
| R4 | Buang duplikat `ride_id` dan pasangan `snapshot_time` + `station_id` | `bersihkan_trips`, `transform_status_snapshot` |
| R5 | Buang perjalanan < 1 menit atau > 24 jam | `bersihkan_trips` |
| R6 | Agregasi per stasiun per 30 menit (trip, net flow, ketersediaan, kosong/penuh) | `agregasi_trips`, `aggregate_snapshots`, `gabung_status` |
| R7 | Gabung cuaca per jam | `transform_weather`, `gabung_cuaca` |

## Jadwal otomatis

| Workflow | Jadwal | Isi |
|---|---|---|
| `etl-loop.yml` (utama) | Memicu dirinya sendiri tiap ±5,7 jam; cron 6 jam sebagai cadangan | Stream tiap 10 menit + job harian sekali sehari setelah 06.00 UTC (`etl/loop.py`) |
| `etl-daily.yml` | Setiap kode ETL berubah; cron harian cadangan | Uji unit, cuaca 10 hari terakhir, file trip bulan baru (inkremental), uji kualitas |
| `etl-stream.yml` | Manual | Satu snapshot GBFS (uji/demo) |
| `tests.yml` | Setiap push | Uji unit |

Cron GitHub `*/10` tidak berjalan di repo ini, jadi jeda 10 menit diatur di dalam runner dan run berikutnya dipicu lewat `workflow_dispatch` (diizinkan untuk `GITHUB_TOKEN`).

Semua job aman dijalankan ulang (upsert + snapshot yang sudah diproses dilewati). Setiap job tercatat di tabel `etl_log_run`; uji kualitas di `etl_quality_check`. Jika job gagal, GitHub mengirim email.

## Menjalankan

```bash
pip install -r requirements.txt
python -m pytest -v                         # uji unit, tanpa database
export DATABASE_URL="postgresql://..."      # Supabase > Connect > Session pooler
python -m etl.run_etl stream                # atau: daily | weather | trips | quality
```

Di GitHub, `DATABASE_URL` disimpan sebagai *repository secret* (Settings → Secrets and variables → Actions).
