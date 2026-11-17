"""Integrasi pipeline ETL: Extract -> Transform -> Load dalam satu perintah.

    python -m etl.run_etl stream            # tiap 10 menit (GitHub Actions)
    python -m etl.run_etl daily             # harian: cuaca + trip bulanan + uji kualitas
    python -m etl.run_etl trips|weather|quality
    python -m etl.run_etl backfill-stream FILE.csv   # sekali: snapshot lama dari Drive/Colab

Setiap job mencatat waktu, jumlah baris per tahap, dan status ke tabel etl_log_run.
"""
import sys
import traceback
from datetime import date, timedelta

import pandas as pd

from . import config, extract, load, quality, transform as T


def _jalankan(conn, job, fungsi):
    mulai = load.sekarang()
    try:
        hasil = fungsi(conn)
        status = hasil.pop("status", "sukses")
        load.catat_log(conn, job, mulai, status, hasil.get("extract"), hasil.get("transform"),
                       hasil.get("load"), hasil.get("pesan"))
        conn.commit()
        print(f"[{job}] {status}: {hasil}")
        return hasil
    except Exception as e:
        conn.rollback()
        load.catat_log(conn, job, mulai, "gagal", pesan=f"{type(e).__name__}: {e}\n{traceback.format_exc()}")
        conn.commit()
        raise


def _cuaca_untuk(waktu_min, waktu_maks):
    hari_ini = pd.Timestamp.now(tz=config.TZ).date()
    raw = extract.extract_weather(pd.Timestamp(waktu_min).date(), pd.Timestamp(waktu_maks).date(), today=hari_ini)
    return T.transform_weather(raw)


def _pastikan_dim(conn):
    dim = load.baca_dim(conn)
    if dim["in_live_feed"].fillna(False).astype(bool).any():
        return dim
    info, _, _ = extract.extract_gbfs()
    load.load_dim_live(conn, T.transform_station_info(info))
    conn.commit()
    return load.baca_dim(conn)


# ================================================================ job: streaming
def job_stream(conn):
    info, status, snap_utc = extract.extract_gbfs()                       # E
    dim_live = T.transform_station_info(info)                             # T
    snaps = T.transform_status_snapshot(status, dim_live)
    agg = T.aggregate_snapshots(snaps)
    slot = sorted(agg["waktu"].unique())
    tersimpan = load.baca_slot(conn, slot + [pd.Timestamp(s) - T.SLOT for s in slot])
    tersimpan["waktu"] = pd.to_datetime(tersimpan["waktu"])
    lama = tersimpan[tersimpan["waktu"].isin(slot)]
    sebelumnya = tersimpan[~tersimpan["waktu"].isin(slot)]
    baris = T.gabung_status(agg, lama, sebelumnya)
    if len(baris) == 0:
        return {"status": "dilewati", "extract": len(status), "transform": 0, "load": 0,
                "pesan": f"snapshot {snap_utc} sudah pernah diproses"}
    cuaca = _cuaca_untuk(min(slot), max(slot))
    baris = T.tambah_kalender(T.gabung_cuaca(baris, cuaca))
    load.load_dim_live(conn, dim_live)                                    # L
    n = load.load_fact_status(conn, baris)
    return {"extract": len(status), "transform": len(baris), "load": n,
            "pesan": f"snapshot {snap_utc}, {len(dim_live)} stasiun JC/HB"}


# ================================================================ job: trip data bulanan
def job_trips(conn):
    dim = _pastikan_dim(conn)
    sudah = load.bulan_sudah_dimuat(conn)
    bulan_lalu = (pd.Timestamp.now(tz=config.TZ).to_period("M") - 1).strftime("%Y%m")
    target = [b for b in extract.daftar_bulan(config.BULAN_AWAL, bulan_lalu) if b not in sudah]
    tot_e = tot_t = tot_l = 0
    dimuat, belum = [], []
    for bulan in target:
        raw = extract.extract_trip_month(bulan)                           # E
        if raw is None:
            belum.append(bulan)
            continue
        bersih, ringkas = T.bersihkan_trips(raw, bulan)                   # T
        bersih, dim_baru = T.petakan_stasiun_trip(bersih, dim)
        fakta = T.agregasi_trips(bersih, bulan)
        awal = pd.Timestamp(f"{bulan[:4]}-{bulan[4:]}-01")
        cuaca = _cuaca_untuk(awal, awal + pd.offsets.MonthEnd(0))
        fakta = T.tambah_kalender(T.gabung_cuaca(fakta, cuaca))
        load.load_dim_historical(conn, dim_baru)                          # L
        load.load_weather(conn, cuaca)
        n = load.load_fact_trips(conn, fakta)
        load.catat_batch(conn, extract.nama_file_trip(bulan), bulan, ringkas, n)
        conn.commit()
        dim = load.baca_dim(conn)
        tot_e, tot_t, tot_l = tot_e + len(raw), tot_t + len(fakta), tot_l + n
        dimuat.append(f"{bulan} ({ringkas})")
    status = "sukses" if dimuat else "dilewati"
    return {"status": status, "extract": tot_e, "transform": tot_t, "load": tot_l,
            "pesan": f"dimuat: {dimuat or '-'}; belum tersedia di S3: {belum or '-'}"}


# ================================================================ job: cuaca
def job_weather(conn):
    ada = load.query_df(conn, "select count(*) as n from public.weather_hourly")["n"].iloc[0]
    hari_ini = pd.Timestamp.now(tz=config.TZ).date()
    mulai = hari_ini - timedelta(days=config.CUACA_ULANG_HARI) if ada else \
        date(int(config.BULAN_AWAL[:4]), int(config.BULAN_AWAL[4:]), 1)
    raw = extract.extract_weather(mulai, hari_ini, today=hari_ini)       # E
    w = T.transform_weather(raw)                                          # T
    n = load.load_weather(conn, w)                                        # L
    sampai_kini = w[w["hour_ts"] <= pd.Timestamp.now(tz=config.TZ).tz_localize(None)]
    diperbarui = load.perbarui_cuaca_fakta(conn, sampai_kini)
    return {"extract": len(raw), "transform": len(w), "load": n,
            "pesan": f"{mulai} s.d. {hari_ini}; kolom cuaca fakta diperbarui untuk {len(sampai_kini)} jam"}


# ================================================================ job: uji kualitas
def job_quality(conn):
    hasil, gagal = quality.jalankan(conn)
    lolos = sum(1 for h in hasil if h[1])
    if gagal:
        raise RuntimeError(f"Uji kualitas kritis gagal: {gagal}")
    return {"load": lolos, "pesan": f"{lolos}/{len(hasil)} cek lolos"}


# ================================================================ backfill sekali jalan
def job_backfill_stream(conn, path, chunk_hari=7):
    """Muat snapshot lama (CSV dari polling Apps Script/notebook) lewat transform yang sama."""
    dim = _pastikan_dim(conn)
    dim_live = dim[dim["in_live_feed"].astype(bool)]
    raw = pd.read_csv(path, dtype={"station_id": "string"})
    snaps = T.transform_status_snapshot(raw, dim_live)
    agg = T.aggregate_snapshots(snaps)
    # slot yang sudah diisi job live tidak ditimpa (snapshot yang lebih baru menang)
    tersimpan = load.query_df(conn, """
        select station_id, waktu, sepeda_awal, n_snapshot, sepeda_rata2, dok_rata2, snapshot_terakhir
        from public.fact_station_30min where n_snapshot is not null and waktu between %s and %s""",
                              (agg["waktu"].min().to_pydatetime(), agg["waktu"].max().to_pydatetime()))
    hasil = T.gabung_status(agg, tersimpan)
    cuaca = load.baca_cuaca(conn, hasil["waktu"].min(), hasil["waktu"].max() + pd.Timedelta(hours=1))
    if len(cuaca) == 0:
        cuaca = _cuaca_untuk(hasil["waktu"].min(), hasil["waktu"].max())
    hasil = T.tambah_kalender(T.gabung_cuaca(hasil, cuaca))
    n = 0
    for _, potong in hasil.groupby(pd.to_datetime(hasil["waktu"]).dt.floor(f"{chunk_hari}D")):
        n += load.load_fact_status(conn, potong)
        conn.commit()
    return {"extract": len(raw), "transform": len(hasil), "load": n,
            "pesan": f"backfill {path}: {snaps['snapshot_dt'].min()} s.d. {snaps['snapshot_dt'].max()}"}


JOBS = {"stream": job_stream, "trips": job_trips, "weather": job_weather, "quality": job_quality}


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    perintah = argv[0]
    conn = load.connect()
    try:
        if perintah == "daily":
            gagal = []
            for nama in ("weather", "trips", "quality"):   # satu job gagal tidak menghentikan yang lain
                try:
                    _jalankan(conn, nama, JOBS[nama])
                except Exception as e:
                    gagal.append(f"{nama}: {e}")
            if gagal:
                raise RuntimeError("; ".join(gagal))
        elif perintah == "backfill-stream":
            _jalankan(conn, "backfill_stream", lambda c: job_backfill_stream(c, argv[1]))
        else:
            _jalankan(conn, perintah, JOBS[perintah])
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
