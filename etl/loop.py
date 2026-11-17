"""Penjadwal di dalam runner GitHub Actions.

Jadwal cron GitHub untuk interval pendek sering terlambat atau tidak berjalan
sama sekali di repo baru. Karena itu satu run workflow `etl-loop.yml` menjalankan
job streaming setiap 10 menit selama hampir 6 jam (batas satu job GitHub), lalu
memicu run berikutnya. Job harian (cuaca, trip bulanan, uji kualitas) ikut
dijalankan sekali sehari setelah 06.00 UTC.

    python -m etl.loop --menit 340
"""
import argparse
import datetime as dt
import time

from . import load, run_etl

INTERVAL_DETIK = 600


def perlu_harian(conn, sekarang):
    """True jika job harian belum berjalan sejak batas 06.00 UTC terakhir."""
    batas = sekarang.replace(hour=6, minute=0, second=0, microsecond=0)
    if sekarang < batas:
        batas -= dt.timedelta(days=1)
    n = load.query_df(conn, "select count(*) as n from public.etl_log_run "
                            "where job = 'weather' and mulai >= %s", (batas,))["n"].iloc[0]
    return n == 0


def satu_putaran():
    conn = load.connect()
    try:
        try:
            run_etl._jalankan(conn, "stream", run_etl.job_stream)
        except Exception as e:
            print(f"[loop] stream gagal (tercatat di etl_log_run): {type(e).__name__}")
        if perlu_harian(conn, dt.datetime.now(dt.timezone.utc)):
            for nama in ("weather", "trips", "quality"):
                try:
                    run_etl._jalankan(conn, nama, run_etl.JOBS[nama])
                except Exception as e:
                    print(f"[loop] {nama} gagal (tercatat di etl_log_run): {type(e).__name__}")
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--menit", type=int, default=340, help="lama loop sebelum run berikutnya dipicu")
    akhir = time.time() + p.parse_args().menit * 60
    while True:
        try:
            satu_putaran()
        except Exception as e:  # mis. database sementara tidak bisa dihubungi: coba lagi 10 menit kemudian
            print(f"[loop] putaran gagal: {type(e).__name__}: {e}")
        # tunggu sampai kelipatan 10 menit berikutnya
        berikut = (time.time() // INTERVAL_DETIK + 1) * INTERVAL_DETIK
        if berikut > akhir:
            break
        time.sleep(max(0, berikut - time.time()))


if __name__ == "__main__":
    main()
