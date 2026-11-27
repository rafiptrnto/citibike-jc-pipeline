"""Pipeline ELT (Extract -> Load -> Transform) Citi Bike Jersey City.

Perbedaan dengan paket `etl/` (Minggu 10): data mentah dimuat ke schema `raw_elt`
tanpa cleaning, lalu seluruh transformasi dijalankan sebagai SQL di dalam PostgreSQL
terhadap data yang sudah tersimpan, dengan hasil di schema `elt`.
"""
