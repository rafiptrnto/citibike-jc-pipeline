"""Data Inspection (ELT): pemeriksaan kondisi dan masalah pada RAW DATA yang sudah tersimpan,
dijalankan sebagai SQL di dalam database (bukan di pandas)."""
import pandas as pd

from etl.load import query_df

KOLOM_TRIPS = ["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
               "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]
RE_WAKTU = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$"


def struktur_trips(conn, batch_id):
    """Jumlah baris, tipe tersimpan, dan nilai kosong per atribut raw trip data."""
    nulls = ", ".join(f"count(*) filter (where {c} is null) as {c}" for c in KOLOM_TRIPS)
    d = query_df(conn, f"select count(*) as baris, {nulls} from raw_elt.trips where batch_id = %s", (batch_id,))
    baris = int(d.loc[0, "baris"])
    tipe = query_df(conn, """select column_name as atribut, data_type as tipe_tersimpan
                             from information_schema.columns
                             where table_schema = 'raw_elt' and table_name = 'trips'
                               and column_name = any(%s) order by ordinal_position""", (KOLOM_TRIPS,))
    tipe["nilai_kosong"] = [int(d.loc[0, a]) for a in tipe["atribut"]]
    tipe["baris"] = baris
    return tipe


def masalah_trips(conn, batch_id, bulan):
    """Identifikasi masalah pada raw trip data: format waktu, duplikat, durasi, di luar bulan, stasiun kosong."""
    q = f"""
    with t as (select *, case when started_at ~ '{RE_WAKTU}' then started_at::timestamp end st,
                         case when ended_at ~ '{RE_WAKTU}' then ended_at::timestamp end en
               from raw_elt.trips where batch_id = %(b)s),
    u as (select *, extract(epoch from en - st) as dur from t)
    select 'Baris mentah' as pemeriksaan, count(*)::bigint as jumlah from u
    union all select 'Waktu tidak dapat dikonversi', count(*) from u where st is null or en is null
    union all select 'ride_id duplikat (baris berlebih)', count(*) - count(distinct ride_id) from u
    union all select 'Durasi < 1 menit', count(*) from u where dur < 60
    union all select 'Durasi > 24 jam', count(*) from u where dur > 86400
    union all select 'Mulai di luar bulan file', count(*) from u
        where st < to_date(%(m)s || '01','YYYYMMDD') or st >= to_date(%(m)s || '01','YYYYMMDD') + interval '1 month'
    union all select 'end_station_id kosong', count(*) from u where end_station_id is null
    union all select 'start_station_id kosong', count(*) from u where start_station_id is null
    union all select 'Atribut tidak relevan (rideable_type, member_casual) - dibuang saat agregasi', 2
    """
    return query_df(conn, q, {"b": batch_id, "m": bulan})


def masalah_gbfs(conn):
    """Identifikasi masalah pada raw GBFS information (snapshot terbaru) dan status."""
    q = """
    with b as (select max(batch_id) b from raw_elt.load_batch where sumber = 'gbfs_station_information' and baris_load = baris_extract),
    i as (select payload from raw_elt.gbfs_station_information, b where batch_id = b.b)
    select 'Stasiun pada feed (seluruh sistem)' as pemeriksaan, count(*)::bigint as jumlah from i
    union all select 'Bukan Jersey City / Hoboken (short_name bukan JC/HB)', count(*) from i
        where upper(payload->>'short_name') !~ '^(JC|HB)'
    union all select 'JC/HB dengan kapasitas 0', count(*) from i
        where upper(payload->>'short_name') ~ '^(JC|HB)' and coalesce((payload->>'capacity')::numeric, 0) = 0
    union all select 'short_name JC/HB ganda (ambigu)', count(*) from (
        select upper(payload->>'short_name') s from i where upper(payload->>'short_name') ~ '^(JC|HB)'
        group by 1 having count(*) > 1) x
    """
    return query_df(conn, q)


def ringkasan_status_gbfs(conn):
    q = """
    with b as (select max(batch_id) b from raw_elt.load_batch where sumber = 'gbfs_station_status' and baris_load = baris_extract)
    select count(*) as baris, count(*) filter (where payload->>'is_installed' not in ('1','true')) as tidak_terpasang,
           count(*) - count(distinct payload->>'station_id') as duplikat_station_id
    from raw_elt.gbfs_station_status s, b where s.batch_id = b.b"""
    return query_df(conn, q)
