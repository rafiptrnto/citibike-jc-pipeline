"""Integrasi pipeline ELT: Extract -> Load (raw) -> Transform (SQL di database) dalam satu perintah.

    python -m elt.run_elt batch 202610        # trip bulan itu + cuaca + satu snapshot GBFS
    python -m elt.run_elt stream [KEEP_DAYS]  # satu snapshot GBFS (+ retensi raw opsional)

Urutan dijaga: Transform hanya berjalan jika raw sudah tersimpan lengkap dan jumlahnya cocok
dengan hasil ekstraksi. Raw tidak diubah oleh Transform. Setiap eksekusi dicatat di elt.run_log.
"""
import sys
import traceback
from datetime import datetime, timezone

import pandas as pd

from etl import config
from . import extract, load, transform


def _say(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def _hash_raw(conn, batch_id):
    """Sidik jari isi raw trips satu batch (untuk membuktikan raw tidak berubah setelah Transform)."""
    with conn.cursor() as cur:
        cur.execute("""select count(*), md5(string_agg(concat_ws('|', row_no, ride_id, started_at, ended_at,
                              start_station_id, end_station_id), ',' order by row_no))
                       from raw_elt.trips where batch_id = %s""", (batch_id,))
        return cur.fetchone()


def _catat(conn, job, mulai, status, ext=None, ld=None, tr=None, pesan=None):
    with conn.cursor() as cur:
        cur.execute("""insert into elt.run_log (job, mulai, selesai, status, baris_extract, baris_load,
                       baris_transform, pesan, github_run_id) values (%s,%s,now(),%s,%s,%s,%s,%s,%s)""",
                    (job, mulai, status, ext, ld, tr, pesan, config.GITHUB_RUN_ID))


def elt_batch(conn, bulan, say=_say):
    """ELT lengkap untuk satu bulan trip data. Mengembalikan ringkasan jumlah baris tiap tahap."""
    mulai = datetime.now(timezone.utc)
    r = {"bulan": bulan}
    try:
        # ---------------- EXTRACT
        say(f"Proses Extract dimulai ({bulan})")
        trips, sumber_trips = extract.extract_trips_raw(bulan)
        if trips is None:
            _catat(conn, "elt_batch", mulai, "dilewati", pesan=f"file {bulan} belum tersedia")
            conn.commit()
            say(f"File trip {bulan} belum tersedia; dilewati")
            return {"status": "dilewati"}
        awal = pd.Timestamp(f"{bulan[:4]}-{bulan[4:]}-01").date()
        akhir = (pd.Timestamp(awal) + pd.offsets.MonthEnd(0)).date()
        cuaca = extract.extract_weather_raw(awal, akhir)
        info, status, snap, feeds = extract.extract_gbfs_raw()
        r.update(extract_trips=len(trips), extract_cuaca=len(cuaca), extract_gbfs_info=len(info),
                 extract_gbfs_status=len(status))
        say(f"Data berhasil diekstraksi: {len(trips):,} perjalanan, {len(cuaca):,} baris cuaca, "
            f"{len(info):,} stasiun GBFS (tanpa cleaning)")

        # ---------------- LOAD (raw)
        say("Proses Load dimulai: memuat data mentah ke schema raw_elt")
        b_trips, n_trips, dilewati = load.load_trips_raw(conn, trips, bulan, sumber_trips)
        b_w, n_w = load.load_weather_raw(conn, cuaca, config.ARCHIVE_URL)
        (b_i, n_i), (b_s, n_s) = load.load_gbfs_raw(conn, info, status, snap, feeds)
        conn.commit()
        for nama, a, b in (("trips", len(trips), n_trips), ("cuaca", len(cuaca), n_w),
                           ("gbfs_information", len(info), n_i), ("gbfs_status", len(status), n_s)):
            if a != b:
                raise RuntimeError(f"Jumlah raw {nama} tidak cocok: ekstraksi {a} vs tersimpan {b}")
        r.update(load_trips=n_trips, load_cuaca=n_w, load_gbfs_info=n_i, load_gbfs_status=n_s)
        say("Raw data berhasil dimuat ke storage" + (" (trip bulan ini sudah ada, tidak dimuat ulang)" if dilewati else "")
            + f": trips {n_trips:,}, cuaca {n_w:,}, gbfs {n_i:,}+{n_s:,} baris")
        sebelum = _hash_raw(conn, b_trips)

        # ---------------- TRANSFORM (SQL di dalam database)
        say("Proses Transform dimulai: SQL dijalankan di PostgreSQL terhadap raw yang tersimpan")
        r["trips"] = transform.transform_trips(conn, bulan, b_trips, say)
        r["gbfs"] = transform.transform_gbfs(conn, say)
        conn.commit()
        say(f"Data berhasil ditransformasi: {r['trips']['bersih']:,} perjalanan bersih -> "
            f"{r['trips']['fact_trips']:,} baris fakta")
        sesudah = _hash_raw(conn, b_trips)
        r["raw_tidak_berubah"] = sebelum == sesudah
        if not r["raw_tidak_berubah"]:
            raise RuntimeError("Raw data berubah setelah Transform!")
        say("Data hasil transformasi berhasil disimpan ke schema elt; raw data tetap utuh")
        r.update(batch_trips=b_trips)
        _catat(conn, "elt_batch", mulai, "sukses", len(trips), n_trips, r["trips"]["fact_trips"], f"bulan {bulan}")
        conn.commit()
        say("ELT Pipeline selesai dijalankan")
        return r
    except Exception as e:
        conn.rollback()
        _catat(conn, "elt_batch", mulai, "gagal", pesan=f"{type(e).__name__}: {e}\n{traceback.format_exc()}"[:4000])
        conn.commit()
        raise


def elt_stream(conn, keep_days=None, say=_say):
    mulai = datetime.now(timezone.utc)
    try:
        say("Proses Extract dimulai (GBFS)")
        info, status, snap, feeds = extract.extract_gbfs_raw()
        say(f"Data berhasil diekstraksi: {len(info):,} stasiun, {len(status):,} status")
        say("Proses Load dimulai")
        (b_i, n_i), (b_s, n_s) = load.load_gbfs_raw(conn, info, status, snap, feeds)
        if (n_i, n_s) != (len(info), len(status)):
            raise RuntimeError("Jumlah raw GBFS tidak cocok dengan hasil ekstraksi")
        conn.commit()
        say(f"Raw data berhasil dimuat ke storage: {n_i:,} + {n_s:,} baris")
        say("Proses Transform dimulai")
        h = transform.transform_gbfs(conn, say)
        if keep_days:
            say(f"Retensi: {load.purge_gbfs_raw(conn, keep_days):,} baris raw status lama dihapus")
        conn.commit()
        say("Data hasil transformasi berhasil disimpan")
        _catat(conn, "elt_stream", mulai, "sukses", len(status), n_s, h["fact_status"], f"snapshot {snap}")
        conn.commit()
        say("ELT Pipeline selesai dijalankan")
        return h
    except Exception as e:
        conn.rollback()
        _catat(conn, "elt_stream", mulai, "gagal", pesan=f"{type(e).__name__}: {e}"[:4000])
        conn.commit()
        raise


if __name__ == "__main__":
    perintah = sys.argv[1] if len(sys.argv) > 1 else "batch"
    conn = load.connect()
    try:
        if perintah == "batch":
            elt_batch(conn, sys.argv[2] if len(sys.argv) > 2 else "202610")
        elif perintah == "stream":
            elt_stream(conn, int(sys.argv[2]) if len(sys.argv) > 2 else None)
        else:
            sys.exit(f"Perintah tidak dikenal: {perintah}")
    finally:
        conn.close()
