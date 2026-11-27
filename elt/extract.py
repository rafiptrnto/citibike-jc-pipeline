"""EXTRACT (ELT): mengambil data dari sumber tanpa mengubah isi.

Berbeda dengan etl/extract.py, semua nilai dipertahankan sebagai teks persis seperti
sumbernya (tidak ada konversi tipe, tidak ada penyaringan). Alasannya: pada ELT yang
disimpan ke storage adalah data mentah; konversi tipe dan pembersihan baru dilakukan
setelah load, di dalam database.
"""
import io
import zipfile

import pandas as pd
import requests

from etl import config
from etl.extract import get_json  # pengambilan JSON + percobaan ulang yang sama

KOLOM_TRIPS = ["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name",
               "start_station_id", "end_station_name", "end_station_id", "start_lat", "start_lng",
               "end_lat", "end_lng", "member_casual"]


def extract_trips_raw(bulan):
    """Trip data satu bulan sebagai DataFrame teks. None jika file belum dipublikasikan."""
    csv_name = f"JC-{bulan}-citibike-tripdata.csv"
    for zip_name in (f"{csv_name}.zip", f"JC-{bulan}-citibike-tripdata.zip"):
        r = requests.get(f"{config.S3_BASE}/{zip_name}", headers=config.HTTP_HEADERS, timeout=300)
        if r.status_code == 200:
            break
    else:
        return None, None
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        anggota = [m for m in z.namelist() if m.endswith(".csv") and not m.startswith("__MACOSX")]
        frames = [pd.read_csv(z.open(m), dtype=str, keep_default_na=False, na_filter=False) for m in anggota]
    df = pd.concat(frames, ignore_index=True)
    return df, f"{config.S3_BASE}/{zip_name}"


def extract_gbfs_raw():
    """Satu snapshot GBFS: daftar objek stasiun asli (dict) + waktu update feed (epoch, teks)."""
    discovery = get_json(config.GBFS_DISCOVERY)
    feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
    info_js = get_json(feeds["station_information"])
    status_js = get_json(feeds["station_status"])
    return (info_js["data"]["stations"], status_js["data"]["stations"],
            str(int(status_js["last_updated"])), feeds)


def _hourly_raw(js, sumber):
    h = js["hourly"]
    df = pd.DataFrame({"hour_ts": h["time"],
                       "temperature_2m": h["temperature_2m"],
                       "precipitation": h["precipitation"],
                       "wind_speed_10m": h["wind_speed_10m"]})
    df = df.astype(object).where(df.notna(), None)
    for c in df.columns:
        df[c] = [None if v is None else str(v) for v in df[c]]
    df["sumber_api"] = sumber
    return df


def extract_weather_raw(start, end, today=None):
    """Cuaca per jam [start, end] persis seperti respons API. Jika rentang mencakup arsip dan
    forecast, keduanya diambil dan dimuat apa adanya (tumpang tindih diselesaikan saat Transform)."""
    from datetime import date, timedelta
    today = today or date.today()
    archive_end = min(end, today - timedelta(days=config.ARCHIVE_LAG_DAYS))
    parts = []
    if start <= archive_end:
        parts.append(_hourly_raw(get_json(config.ARCHIVE_URL, params={
            "latitude": config.LAT, "longitude": config.LON,
            "start_date": str(start), "end_date": str(archive_end),
            "hourly": config.WEATHER_VARS, "timezone": config.TZ}), "archive"))
    if end > archive_end:
        past_days = min(92, max(1, (today - max(start, archive_end)).days + 1))
        parts.append(_hourly_raw(get_json(config.FORECAST_URL, params={
            "latitude": config.LAT, "longitude": config.LON,
            "past_days": past_days, "forecast_days": 2,
            "hourly": config.WEATHER_VARS, "timezone": config.TZ}), "forecast"))
    return pd.concat(parts, ignore_index=True)
