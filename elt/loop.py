"""Penjadwal ELT di dalam runner GitHub Actions (pola sama dengan etl/loop.py).

Satu run workflow `elt-loop.yml` menjalankan ELT streaming (snapshot GBFS) tiap 10 menit
selama ~5,7 jam, menjalankan job harian (cuaca, bulan trip baru) sekali sehari setelah
06.00 UTC, lalu memicu run berikutnya.

    python -m elt.loop --menit 340
"""
import argparse
import datetime as dt
import time

from . import load, run_elt

INTERVAL_DETIK = 600


def perlu_harian(conn, sekarang):
    batas = sekarang.replace(hour=6, minute=0, second=0, microsecond=0)
    if sekarang < batas:
        batas -= dt.timedelta(days=1)
    n = load.query_df(conn, "select count(*) as n from elt.run_log "
                            "where job = 'elt_cuaca' and mulai >= %s", (batas,))["n"].iloc[0]
    return n == 0


def satu_putaran():
    conn = load.connect()
    try:
        try:
            run_elt.elt_stream(conn)
        except Exception as e:
            print(f"[elt-loop] stream gagal (tercatat di elt.run_log): {type(e).__name__}")
        if perlu_harian(conn, dt.datetime.now(dt.timezone.utc)):
            for fn in (run_elt.elt_cuaca, run_elt.elt_trip_bulan_baru):
                try:
                    fn(conn)
                except Exception as e:
                    print(f"[elt-loop] {fn.__name__} gagal: {type(e).__name__}")
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--menit", type=int, default=340)
    akhir = time.time() + p.parse_args().menit * 60
    while True:
        try:
            satu_putaran()
        except Exception as e:
            print(f"[elt-loop] putaran gagal: {type(e).__name__}: {e}")
        berikut = (time.time() // INTERVAL_DETIK + 1) * INTERVAL_DETIK
        if berikut > akhir:
            break
        time.sleep(max(0, berikut - time.time()))


if __name__ == "__main__":
    main()
