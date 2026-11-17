"""LOAD: memuat data yang SUDAH bersih ke Supabase (PostgreSQL).

Semua penulisan memakai upsert (INSERT ... ON CONFLICT DO UPDATE) sehingga
job aman dijalankan ulang tanpa membuat data dobel (idempotent).
Loader trip data dan loader streaming memperbarui kolom yang berbeda pada
baris stasiun x slot yang sama, sehingga keduanya tidak saling menimpa.
"""
import datetime as dt

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

from . import config

for _t in (np.int8, np.int16, np.int32, np.int64):
    register_adapter(_t, AsIs)
for _t in (np.float32, np.float64):
    register_adapter(_t, lambda v: AsIs("NULL") if np.isnan(v) else AsIs(repr(float(v))))
register_adapter(np.bool_, lambda v: AsIs("TRUE" if v else "FALSE"))

KOLOM_KALENDER = ["tanggal", "jam", "hari", "akhir_pekan"]
KOLOM_CUACA = ["suhu_c", "hujan_mm", "angin_kmh"]
KOLOM_STATUS = ["sepeda_awal", "sepeda_tersedia", "ebike_tersedia", "dok_tersedia", "sepeda_rusak",
                "dok_rusak", "sepeda_rata2", "dok_rata2", "net_flow_status", "kosong", "penuh",
                "n_snapshot", "snapshot_terakhir"]
KOLOM_TRIP = ["trips_keluar", "trips_masuk", "net_flow_trip"]
KOLOM_DIM = ["station_id", "live_station_id", "station_name", "wilayah", "capacity", "lat", "lng",
             "in_live_feed", "metode_pemetaan"]


def connect():
    if not config.DATABASE_URL:
        raise RuntimeError("DATABASE_URL belum diisi (GitHub Secret / variabel lingkungan).")
    return psycopg2.connect(config.DATABASE_URL, connect_timeout=30)


def _rows(df, cols):
    d = df[cols].astype(object).where(df[cols].notna(), None)
    return [tuple(r) for r in d.itertuples(index=False, name=None)]


def query_df(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def upsert(conn, table, df, kunci, kolom_update, page_size=5000):
    """Upsert df ke table. Hanya kolom_update yang diperbarui jika kunci sudah ada."""
    if df is None or len(df) == 0:
        return 0
    cols = list(kunci) + [c for c in kolom_update if c not in kunci]
    set_ = ", ".join(f"{c} = excluded.{c}" for c in kolom_update) + ", loaded_at = now()" \
        if table in ("fact_station_30min", "weather_hourly") else \
        ", ".join(f"{c} = excluded.{c}" for c in kolom_update)
    aksi = f"do update set {set_}" if kolom_update else "do nothing"
    sql = (f"insert into public.{table} ({', '.join(cols)}) values %s "
           f"on conflict ({', '.join(kunci)}) {aksi}")
    with conn.cursor() as cur:
        execute_values(cur, sql, _rows(df, cols), page_size=page_size)
    return len(df)


# ---------------------------------------------------------------- dim_station
def load_dim_live(conn, dim):
    """Stasiun dari feed live: info terbaru selalu menimpa."""
    d = dim.copy()
    n = upsert(conn, "dim_station", d, ["station_id"], KOLOM_DIM[1:])
    with conn.cursor() as cur:
        cur.execute("update public.dim_station set updated_at = now() where station_id = any(%s)",
                    (list(d["station_id"]),))
    return n


def load_dim_historical(conn, dim_baru):
    """Stasiun yang hanya ada di trip data: ditambahkan jika belum ada."""
    return upsert(conn, "dim_station", dim_baru, ["station_id"], [])


def baca_dim(conn):
    return query_df(conn, "select * from public.dim_station")


# ---------------------------------------------------------------- fakta
def load_fact_status(conn, df):
    return upsert(conn, "fact_station_30min", df, ["station_id", "waktu"],
                  KOLOM_KALENDER + KOLOM_CUACA + KOLOM_STATUS)


def load_fact_trips(conn, df):
    return upsert(conn, "fact_station_30min", df, ["station_id", "waktu"],
                  KOLOM_KALENDER + KOLOM_CUACA + KOLOM_TRIP)


def baca_slot(conn, waktu_list):
    """Isi slot yang sudah tersimpan (untuk digabung dengan snapshot baru)."""
    return query_df(conn, """
        select station_id, waktu, sepeda_awal, sepeda_tersedia, n_snapshot, sepeda_rata2,
               dok_rata2, snapshot_terakhir
        from public.fact_station_30min
        where waktu = any(%s) and n_snapshot is not null""",
                    ([pd.Timestamp(w).to_pydatetime() for w in waktu_list],))


# ---------------------------------------------------------------- cuaca
def load_weather(conn, w):
    return upsert(conn, "weather_hourly", w, ["hour_ts"], ["suhu_c", "hujan_mm", "angin_kmh", "sumber_api"])


def perbarui_cuaca_fakta(conn, w):
    """Tulis ulang kolom cuaca pada baris fakta untuk jam-jam di w (nilai dihitung di Python)."""
    if len(w) == 0:
        return 0
    sql = """update public.fact_station_30min f
             set suhu_c = v.suhu_c, hujan_mm = v.hujan_mm, angin_kmh = v.angin_kmh
             from (values %s) as v(hour_ts, suhu_c, hujan_mm, angin_kmh)
             where f.waktu >= v.hour_ts::timestamp and f.waktu < v.hour_ts::timestamp + interval '1 hour'"""
    with conn.cursor() as cur:
        execute_values(cur, sql, _rows(w, ["hour_ts", "suhu_c", "hujan_mm", "angin_kmh"]),
                       template="(%s::timestamp, %s::real, %s::real, %s::real)", page_size=1000)
        return cur.rowcount


def baca_cuaca(conn, dari, sampai):
    return query_df(conn, "select hour_ts, suhu_c, hujan_mm, angin_kmh from public.weather_hourly "
                          "where hour_ts >= %s and hour_ts < %s", (dari, sampai))


# ---------------------------------------------------------------- batch & log
def bulan_sudah_dimuat(conn):
    return set(query_df(conn, "select bulan from public.etl_batch_loaded")["bulan"])


def catat_batch(conn, file_name, bulan, ringkasan, baris_load):
    with conn.cursor() as cur:
        cur.execute("""insert into public.etl_batch_loaded
                       (file_name, bulan, baris_mentah, baris_bersih, baris_load) values (%s,%s,%s,%s,%s)
                       on conflict (file_name) do update set baris_mentah = excluded.baris_mentah,
                       baris_bersih = excluded.baris_bersih, baris_load = excluded.baris_load, loaded_at = now()""",
                    (file_name, bulan, ringkasan["baris_mentah"], ringkasan["baris_bersih"], baris_load))


def catat_log(conn, job, mulai, status, extract=None, transform=None, load=None, pesan=None):
    with conn.cursor() as cur:
        cur.execute("""insert into public.etl_log_run
                       (job, mulai, selesai, status, baris_extract, baris_transform, baris_load, pesan, github_run_id)
                       values (%s, %s, now(), %s, %s, %s, %s, %s, %s)""",
                    (job, mulai, status, extract, transform, load, (pesan or "")[:2000], config.GITHUB_RUN_ID))


def sekarang():
    return dt.datetime.now(dt.timezone.utc)
