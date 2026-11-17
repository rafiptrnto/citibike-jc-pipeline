"""EXTRACT: mengambil data dari tiga sumber tanpa mengubah isinya.

- GBFS (streaming): station_information + station_status, kondisi saat ini.
- Trip data S3 (batch bulanan): file ZIP berisi CSV perjalanan.
- Open-Meteo (batch harian): cuaca per jam.
"""
import io
import time
import zipfile
from datetime import date, timedelta

import pandas as pd
import requests

from . import config


def get_json(url, params=None, timeout=90, percobaan=4):
    """GET JSON dengan percobaan ulang (jeda 5, 15, 45 detik) untuk galat jaringan sementara."""
    for i in range(percobaan):
        try:
            r = requests.get(url, params=params, headers=config.HTTP_HEADERS, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            sementara = not isinstance(e, requests.HTTPError) or (
                e.response is not None and e.response.status_code in (429, 500, 502, 503, 504))
            if not sementara or i == percobaan - 1:
                raise
            time.sleep(5 * 3 ** i)


# ---------------------------------------------------------------- GBFS (streaming)
def extract_gbfs():
    """Satu snapshot seluruh sistem. Mengembalikan (info, status, waktu_snapshot_utc)."""
    discovery = get_json(config.GBFS_DISCOVERY)
    feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}

    info_js = get_json(feeds["station_information"])
    status_js = get_json(feeds["station_status"])

    info = pd.DataFrame(info_js["data"]["stations"])
    status = pd.DataFrame(status_js["data"]["stations"])
    snapshot_utc = pd.Timestamp(status_js["last_updated"], unit="s", tz="UTC")
    status["snapshot_time"] = int(status_js["last_updated"])
    return info, status, snapshot_utc


# ---------------------------------------------------------------- Trip data (batch)
def nama_file_trip(bulan):
    return f"JC-{bulan}-citibike-tripdata.csv"


def daftar_bulan(dari, sampai):
    """['202607', '202608', ...] dari bulan `dari` sampai `sampai` (inklusif)."""
    p = pd.Period(f"{dari[:4]}-{dari[4:]}", freq="M")
    q = pd.Period(f"{sampai[:4]}-{sampai[4:]}", freq="M")
    out = []
    while p <= q:
        out.append(p.strftime("%Y%m"))
        p += 1
    return out


def extract_trip_month(bulan):
    """Unduh trip data satu bulan. None jika file belum dipublikasikan di S3."""
    csv_name = nama_file_trip(bulan)
    # Nama zip di bucket tidak selalu konsisten (.csv.zip atau .zip)
    for zip_name in (f"{csv_name}.zip", f"JC-{bulan}-citibike-tripdata.zip"):
        r = requests.get(f"{config.S3_BASE}/{zip_name}", headers=config.HTTP_HEADERS, timeout=300)
        if r.status_code == 200:
            break
    else:
        return None
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        members = [m for m in z.namelist() if m.endswith(".csv") and not m.startswith("__MACOSX")]
        frames = [pd.read_csv(z.open(m), dtype={"start_station_id": "string", "end_station_id": "string"})
                  for m in members]
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------- Cuaca (batch)
def _to_df(js, sumber):
    w = pd.DataFrame(js["hourly"]).rename(columns={"time": "hour_ts"})
    w["hour_ts"] = pd.to_datetime(w["hour_ts"])
    w["sumber_api"] = sumber
    return w


def extract_weather(start, end, today=None):
    """Cuaca per jam [start, end] (tanggal lokal). Arsip untuk tanggal lama,
    forecast (dengan past_days) untuk beberapa hari terakhir."""
    today = today or date.today()
    archive_end = min(end, today - timedelta(days=config.ARCHIVE_LAG_DAYS))
    parts = []
    if start <= archive_end:
        parts.append(_to_df(get_json(config.ARCHIVE_URL, params={
            "latitude": config.LAT, "longitude": config.LON,
            "start_date": str(start), "end_date": str(archive_end),
            "hourly": config.WEATHER_VARS, "timezone": config.TZ}), "archive"))
    if end > archive_end:
        past_days = min(92, max(1, (today - max(start, archive_end)).days + 1))
        parts.append(_to_df(get_json(config.FORECAST_URL, params={
            "latitude": config.LAT, "longitude": config.LON,
            "past_days": past_days, "forecast_days": 2,
            "hourly": config.WEATHER_VARS, "timezone": config.TZ}), "forecast"))
    return pd.concat(parts, ignore_index=True)
