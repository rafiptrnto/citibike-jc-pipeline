"""LOAD (ELT): memuat data MENTAH ke schema raw_elt tanpa cleaning/transformasi.

Setiap pemuatan dicatat di raw_elt.load_batch (asal data, jumlah baris diekstraksi, jumlah
baris tersimpan). Tabel raw bersifat append-only: transformasi tidak pernah mengubahnya.
"""
import csv
import io
import json

import psycopg2

from etl import config
from etl.load import connect, query_df  # noqa: F401  (koneksi sama dengan pipeline ETL)

NULL = r"\N"


def _csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    for r in rows:
        w.writerow([NULL if (v is None or v == "") else v for v in r])
    buf.seek(0)
    return buf


def buat_batch(conn, sumber, source_file, bulan, baris_extract):
    with conn.cursor() as cur:
        cur.execute("""insert into raw_elt.load_batch (sumber, source_file, bulan, baris_extract, github_run_id)
                       values (%s,%s,%s,%s,%s) returning batch_id""",
                    (sumber, source_file, bulan, int(baris_extract), config.GITHUB_RUN_ID))
        return cur.fetchone()[0]


def _copy(conn, tabel, kolom, rows):
    with conn.cursor() as cur:
        cur.copy_expert(f"copy raw_elt.{tabel} ({', '.join(kolom)}) from stdin with (format csv, null '{NULL}')",
                        _csv(rows))


def _selesai(conn, batch_id, tabel):
    """Baca kembali jumlah baris yang benar-benar tersimpan untuk batch ini."""
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from raw_elt.{tabel} where batch_id = %s", (batch_id,))
        n = cur.fetchone()[0]
        cur.execute("update raw_elt.load_batch set baris_load = %s where batch_id = %s", (n, batch_id))
    return n


def batch_selesai(conn, sumber, bulan):
    """Batch trip yang sudah termuat lengkap untuk bulan ini (batch_id, baris) atau None."""
    d = query_df(conn, """select batch_id, baris_load from raw_elt.load_batch
                          where sumber = %s and bulan = %s and baris_load = baris_extract
                          order by batch_id desc limit 1""", (sumber, bulan))
    return None if d.empty else (int(d.iloc[0]["batch_id"]), int(d.iloc[0]["baris_load"]))


def load_trips_raw(conn, df, bulan, source_file):
    """Muat trip data mentah. File bulanan bersifat tetap, jadi jika bulan ini sudah termuat
    lengkap, tidak dimuat ulang (eksekusi ulang aman, raw tidak berlipat)."""
    ada = batch_selesai(conn, "trips", bulan)
    if ada:
        return ada[0], ada[1], True
    batch_id = buat_batch(conn, "trips", source_file, bulan, len(df))
    from .extract import KOLOM_TRIPS
    rows = ((batch_id, i + 1, *r) for i, r in enumerate(df[KOLOM_TRIPS].itertuples(index=False, name=None)))
    _copy(conn, "trips", ["batch_id", "row_no"] + KOLOM_TRIPS, rows)
    return batch_id, _selesai(conn, batch_id, "trips"), False


def load_gbfs_raw(conn, info_records, status_records, snapshot_time, feeds):
    """Muat satu snapshot GBFS: setiap stasiun disimpan sebagai objek JSON aslinya (jsonb).
    info_records=None melewati station_information (data stasiun jarang berubah)."""
    b_info = n_info = None
    if info_records is not None:
        b_info = buat_batch(conn, "gbfs_station_information", feeds.get("station_information"), None, len(info_records))
        _copy(conn, "gbfs_station_information", ["batch_id", "row_no", "payload"],
              ((b_info, i + 1, json.dumps(s, ensure_ascii=False)) for i, s in enumerate(info_records)))
        n_info = _selesai(conn, b_info, "gbfs_station_information")
    b_st = buat_batch(conn, "gbfs_station_status", feeds.get("station_status"), None, len(status_records))
    _copy(conn, "gbfs_station_status", ["batch_id", "row_no", "snapshot_time", "payload"],
          ((b_st, i + 1, snapshot_time, json.dumps(s, ensure_ascii=False)) for i, s in enumerate(status_records)))
    n_st = _selesai(conn, b_st, "gbfs_station_status")
    return (b_info, n_info), (b_st, n_st)


def info_sudah_dimuat(conn, jam=24):
    """True jika station_information lengkap sudah dimuat dalam `jam` jam terakhir."""
    with conn.cursor() as cur:
        cur.execute("""select exists (select 1 from raw_elt.load_batch
                       where sumber = 'gbfs_station_information' and baris_load = baris_extract
                         and loaded_at > now() - make_interval(hours => %s))""", (jam,))
        return cur.fetchone()[0]


def load_weather_raw(conn, df, sumber_url):
    batch_id = buat_batch(conn, "weather", sumber_url, None, len(df))
    kol = ["hour_ts", "temperature_2m", "precipitation", "wind_speed_10m", "sumber_api"]
    rows = ((batch_id, i + 1, *r) for i, r in enumerate(df[kol].itertuples(index=False, name=None)))
    _copy(conn, "weather", ["batch_id", "row_no"] + kol, rows)
    return batch_id, _selesai(conn, batch_id, "weather")


def purge_gbfs_raw(conn, keep_days):
    """Retensi paket gratis Supabase (500 MB): snapshot GBFS mentah yang lebih lama dari keep_days
    dihapus HANYA jika sudah ditransformasi ke silver (elt.gbfs_status_clean)."""
    with conn.cursor() as cur:
        cur.execute("""
            with lama as (
              select batch_id from raw_elt.load_batch
              where sumber = 'gbfs_station_status' and loaded_at < now() - make_interval(days => %s)
                and exists (select 1 from elt.gbfs_status_clean))
            delete from raw_elt.gbfs_station_status where batch_id in (select batch_id from lama)""", (keep_days,))
        return cur.rowcount
