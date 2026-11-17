"""Konfigurasi pipeline ETL Citi Bike Jersey City.

Semua nilai yang bisa berubah dikumpulkan di sini supaya modul lain tidak
menyimpan angka atau URL sendiri.
"""
import os

# Koneksi target (Supabase ETL). Diisi lewat GitHub Secret / variabel lingkungan,
# tidak pernah ditulis di kode.
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Zona waktu seluruh data (Jersey City)
TZ = "America/New_York"
SLOT = "30min"

# Rule 1: hanya stasiun Jersey City dan Hoboken
PREFIX_WILAYAH = {"JC": "Jersey City", "HB": "Hoboken"}

# Sumber batch: trip data bulanan Citi Bike Jersey City
S3_BASE = "https://s3.amazonaws.com/tripdata"
BULAN_AWAL = "202607"  # data historical proyek dimulai Juli 2026

# Sumber streaming: GBFS Citi Bike
GBFS_DISCOVERY = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
HTTP_HEADERS = {"User-Agent": "bigdata-course-project/etl (Telkom University)"}

# Sumber cuaca: Open-Meteo, titik pusat Jersey City
LAT, LON = 40.7178, -74.0431
WEATHER_VARS = "temperature_2m,precipitation,wind_speed_10m"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_LAG_DAYS = 7  # arsip Open-Meteo baru lengkap beberapa hari kemudian
CUACA_ULANG_HARI = 10  # job harian memperbarui cuaca 10 hari terakhir

# Rule 5: batas durasi perjalanan yang dianggap wajar
DURASI_MIN_DETIK = 60
DURASI_MAX_DETIK = 24 * 3600

# Uji kualitas: data streaming dianggap segar jika snapshot terakhir < 40 menit
BATAS_SEGAR_MENIT = 40

GITHUB_RUN_ID = os.environ.get("GITHUB_RUN_ID")
