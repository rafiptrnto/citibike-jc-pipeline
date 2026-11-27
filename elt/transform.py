"""TRANSFORM (ELT): seluruh pembersihan dan transformasi dijalankan sebagai SQL DI DALAM
PostgreSQL terhadap data mentah yang sudah tersimpan di schema raw_elt. Python hanya
mengirim perintah SQL dan mencatat jumlah baris tiap langkah; tidak ada data yang ditarik
ke pandas untuk diolah.

Aturan sama dengan pipeline ETL (R1-R7) supaya hasil kedua pendekatan bisa dibandingkan:
  R1 saring stasiun JC/HB aktif | R2 petakan ID live -> ID historical | R3 waktu lokal + slot 30 menit
  R4 buang duplikat | R5 buang durasi < 1 menit / > 24 jam | R6 agregasi per stasiun x slot | R7 gabung cuaca

Lapisan hasil: silver = elt.trips_clean, elt.gbfs_status_clean; gold = elt.dim_station,
elt.weather_hourly, elt.fact_station_30min. Tabel raw_elt tidak pernah diubah.
"""

RE_WAKTU = r"'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$'"
RE_ANGKA = r"'^-?\d+(\.\d+)?$'"
RE_JAM = r"'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?$'"
ORIGIN = "timestamp '2000-01-01'"

# ---------------------------------------------------------------- dimensi stasiun (R1, R2)
SQL_DIM_LIVE = """
with terbaru as (select max(batch_id) b from raw_elt.load_batch
                 where sumber = 'gbfs_station_information' and baris_load = baris_extract),
info as (
  select i.payload->>'station_id' as live_id, trim(i.payload->>'name') as nm,
         upper(trim(i.payload->>'short_name')) as sid,
         nullif(i.payload->>'capacity','')::numeric as cap,
         nullif(i.payload->>'lat','')::float8 as lat, nullif(i.payload->>'lon','')::float8 as lng
  from raw_elt.gbfs_station_information i join terbaru t on i.batch_id = t.b),
jchb as (                                       -- R1: hanya JC/HB berkapasitas > 0
  select *, case when sid like 'JC%' then 'Jersey City' when sid like 'HB%' then 'Hoboken' end as wil
  from info where coalesce(cap, 0) > 0),
tunggal as (select *, count(*) over (partition by sid) as n from jchb where wil is not null)
insert into elt.dim_station (station_id, live_station_id, station_name, wilayah, capacity, lat, lng,
                             in_live_feed, metode_pemetaan)
select sid, live_id, nm, wil, cap::int, lat, lng, true, 'short_name'
from tunggal where n = 1                        -- short_name ganda bersifat ambigu -> dibuang
on conflict (station_id) do update set live_station_id = excluded.live_station_id,
  station_name = excluded.station_name, wilayah = excluded.wilayah, capacity = excluded.capacity,
  lat = excluded.lat, lng = excluded.lng, in_live_feed = true, metode_pemetaan = 'short_name',
  updated_at = now()
"""

# ---------------------------------------------------------------- cuaca (R7: bersihkan + satu baris per jam)
SQL_CUACA = """
insert into elt.weather_hourly (hour_ts, suhu_c, hujan_mm, angin_kmh, sumber_api)
select distinct on (h) h, t, p, w, sumber_api
from (select case when hour_ts ~ """ + RE_JAM + """ then hour_ts::timestamp end as h,
             case when temperature_2m ~ """ + RE_ANGKA + """ then temperature_2m::real end as t,
             case when precipitation ~ """ + RE_ANGKA + """ then precipitation::real end as p,
             case when wind_speed_10m ~ """ + RE_ANGKA + """ then wind_speed_10m::real end as w,
             sumber_api, batch_id
      from raw_elt.weather) x
where h is not null and not (t is null and p is null and w is null)
order by h, (sumber_api <> 'archive'), batch_id desc          -- arsip diutamakan di atas forecast
on conflict (hour_ts) do update set suhu_c = excluded.suhu_c, hujan_mm = excluded.hujan_mm,
  angin_kmh = excluded.angin_kmh, sumber_api = excluded.sumber_api, loaded_at = now()
"""

# ---------------------------------------------------------------- trip data (R1-R5)
SQL_T0 = f"""
create temp table _t0 on commit drop as
select row_no, ride_id,
  case when started_at ~ {RE_WAKTU} then started_at::timestamp end as st,
  case when ended_at ~ {RE_WAKTU} then ended_at::timestamp end as en,
  upper(trim(start_station_id)) as s_id, nullif(trim(start_station_name), '') as s_nm,
  upper(trim(end_station_id)) as e_id, nullif(trim(end_station_name), '') as e_nm,
  case when start_lat ~ {RE_ANGKA} then start_lat::float8 end as s_lat,
  case when start_lng ~ {RE_ANGKA} then start_lng::float8 end as s_lng,
  case when end_lat ~ {RE_ANGKA} then end_lat::float8 end as e_lat,
  case when end_lng ~ {RE_ANGKA} then end_lng::float8 end as e_lng
from raw_elt.trips where batch_id = %(batch)s
"""
SQL_T1 = "create temp table _t1 on commit drop as select * from _t0 where st is not null and en is not null"
SQL_T2 = """create temp table _t2 on commit drop as
            select distinct on (ride_id) * from _t1 order by ride_id, row_no"""            # R4
SQL_T3 = """create temp table _t3 on commit drop as
            select * from _t2 where extract(epoch from en - st) between %(dmin)s and %(dmax)s"""   # R5
SQL_T4 = """create temp table _t4 on commit drop as
            select * from _t3 where st >= to_date(%(bulan)s || '01', 'YYYYMMDD')
              and st < to_date(%(bulan)s || '01', 'YYYYMMDD') + interval '1 month'"""

# R1 + R2: cocokkan ke dim_station lewat ID, cadangan lewat nama unik, cadangan terakhir ID JC/HB sendiri
SQL_MAP = """
create temp table _nama_unik on commit drop as
  select station_name, min(station_id) as station_id from elt.dim_station
  group by station_name having count(*) = 1;
create temp table _map on commit drop as
select t.*,
  coalesce(ds.station_id, nu.station_id,
           case when t.s_id like 'JC%%' or t.s_id like 'HB%%' then t.s_id end) as start_sid,
  coalesce(de.station_id, ne.station_id,
           case when t.e_id like 'JC%%' or t.e_id like 'HB%%' then t.e_id end) as end_sid
from _t4 t
left join elt.dim_station ds on ds.station_id = t.s_id left join _nama_unik nu on nu.station_name = t.s_nm
left join elt.dim_station de on de.station_id = t.e_id left join _nama_unik ne on ne.station_name = t.e_nm
"""

# stasiun JC/HB yang hanya ada di trip data (tidak ada di feed live) ditambahkan ke dimensi
SQL_DIM_HIST = """
insert into elt.dim_station (station_id, live_station_id, station_name, wilayah, capacity, lat, lng,
                             in_live_feed, metode_pemetaan)
select sid, null, coalesce((array_agg(nm order by sisi, row_no) filter (where nm is not null))[1], sid),
       case when sid like 'JC%' then 'Jersey City' else 'Hoboken' end, null,
       percentile_cont(0.5) within group (order by lat), percentile_cont(0.5) within group (order by lng),
       false, 'historical_saja'
from (select start_sid as sid, s_nm as nm, s_lat as lat, s_lng as lng, 0 as sisi, row_no from _map
      union all
      select end_sid, e_nm, e_lat, e_lng, 1, row_no from _map) u
where sid is not null and not exists (select 1 from elt.dim_station d where d.station_id = u.sid)
group by sid
on conflict (station_id) do nothing
"""

SQL_TRIPS_CLEAN = """
delete from elt.trips_clean where bulan = %(bulan)s;
insert into elt.trips_clean (bulan, ride_id, started_at, ended_at, start_sid, end_sid, source_batch_id)
select %(bulan)s, ride_id, st, en, start_sid, end_sid, %(batch)s from _map
"""

# ---------------------------------------------------------------- fakta trip (R3, R6, R7)
SQL_FACT_TRIPS = """
insert into elt.fact_station_30min (station_id, waktu, tanggal, jam, hari, akhir_pekan, suhu_c, hujan_mm,
                                    angin_kmh, trips_keluar, trips_masuk, net_flow_trip)
with awal as (select to_date(%(bulan)s || '01', 'YYYYMMDD')::timestamp as t),
stasiun as (select start_sid as sid from elt.trips_clean where bulan = %(bulan)s and start_sid is not null
            union select end_sid from elt.trips_clean where bulan = %(bulan)s and end_sid is not null),
slot as (select generate_series(a.t, a.t + interval '1 month' - interval '30 minutes', interval '30 minutes') as w
         from awal a),
grid as (select s.sid, sl.w from stasiun s cross join slot sl),
k as (select start_sid as sid, date_bin('30 minutes', started_at, ORIGIN) as w, count(*) as n
      from elt.trips_clean where bulan = %(bulan)s and start_sid is not null group by 1, 2),
i as (select end_sid as sid, date_bin('30 minutes', ended_at, ORIGIN) as w, count(*) as n
      from elt.trips_clean where bulan = %(bulan)s and end_sid is not null group by 1, 2)
select g.sid, g.w, g.w::date, extract(hour from g.w)::int, (extract(isodow from g.w) - 1)::int,
       extract(isodow from g.w) >= 6, wh.suhu_c, wh.hujan_mm, wh.angin_kmh,
       coalesce(k.n, 0), coalesce(i.n, 0), coalesce(i.n, 0) - coalesce(k.n, 0)
from grid g
left join k on k.sid = g.sid and k.w = g.w
left join i on i.sid = g.sid and i.w = g.w
left join elt.weather_hourly wh on wh.hour_ts = date_trunc('hour', g.w)
on conflict (station_id, waktu) do update set tanggal = excluded.tanggal, jam = excluded.jam,
  hari = excluded.hari, akhir_pekan = excluded.akhir_pekan, suhu_c = excluded.suhu_c,
  hujan_mm = excluded.hujan_mm, angin_kmh = excluded.angin_kmh, trips_keluar = excluded.trips_keluar,
  trips_masuk = excluded.trips_masuk, net_flow_trip = excluded.net_flow_trip, loaded_at = now()
""".replace("ORIGIN", ORIGIN)

# ---------------------------------------------------------------- status GBFS (R1-R4, R6)
SQL_GBFS_CLEAN = """
insert into elt.gbfs_status_clean (station_id, snapshot_terakhir, snapshot_dt, waktu, num_bikes_available,
    num_ebikes_available, num_docks_available, num_bikes_disabled, num_docks_disabled)
select d.station_id, to_timestamp(s.snapshot_time::bigint),
       to_timestamp(s.snapshot_time::bigint) at time zone 'America/New_York',
       date_bin('30 minutes', to_timestamp(s.snapshot_time::bigint) at time zone 'America/New_York', ORIGIN),
       nullif(s.payload->>'num_bikes_available', '')::numeric::int,
       nullif(s.payload->>'num_ebikes_available', '')::numeric::int,
       nullif(s.payload->>'num_docks_available', '')::numeric::int,
       nullif(s.payload->>'num_bikes_disabled', '')::numeric::int,
       nullif(s.payload->>'num_docks_disabled', '')::numeric::int
from (select distinct on (snapshot_time, payload->>'station_id') *            -- R4
      from raw_elt.gbfs_station_status order by snapshot_time, payload->>'station_id', batch_id) s
join elt.dim_station d on d.live_station_id = s.payload->>'station_id'          -- R1 + R2
where s.payload->>'is_installed' in ('1', 'true')
on conflict (station_id, snapshot_terakhir) do nothing
""".replace("ORIGIN", ORIGIN)

SQL_FACT_STATUS = """
with agg as (
  select station_id, waktu,
    (array_agg(num_bikes_available order by snapshot_dt))[1] as sepeda_awal,
    (array_agg(num_bikes_available order by snapshot_dt desc))[1] as sepeda_tersedia,
    (array_agg(num_ebikes_available order by snapshot_dt desc))[1] as ebike_tersedia,
    (array_agg(num_docks_available order by snapshot_dt desc))[1] as dok_tersedia,
    (array_agg(num_bikes_disabled order by snapshot_dt desc))[1] as sepeda_rusak,
    (array_agg(num_docks_disabled order by snapshot_dt desc))[1] as dok_rusak,
    avg(num_bikes_available) as sepeda_rata2, avg(num_docks_available) as dok_rata2,
    count(distinct snapshot_dt) as n, max(snapshot_terakhir) as terakhir
  from elt.gbfs_status_clean group by station_id, waktu),
f as (select a.*, p.sepeda_tersedia as prev from agg a
      left join agg p on p.station_id = a.station_id and p.waktu = a.waktu - interval '30 minutes')
insert into elt.fact_station_30min (station_id, waktu, tanggal, jam, hari, akhir_pekan, suhu_c, hujan_mm,
    angin_kmh, sepeda_awal, sepeda_tersedia, ebike_tersedia, dok_tersedia, sepeda_rusak, dok_rusak,
    sepeda_rata2, dok_rata2, net_flow_status, kosong, penuh, n_snapshot, snapshot_terakhir)
select f.station_id, f.waktu, f.waktu::date, extract(hour from f.waktu)::int,
       (extract(isodow from f.waktu) - 1)::int, extract(isodow from f.waktu) >= 6,
       wh.suhu_c, wh.hujan_mm, wh.angin_kmh, f.sepeda_awal, f.sepeda_tersedia, f.ebike_tersedia,
       f.dok_tersedia, f.sepeda_rusak, f.dok_rusak, f.sepeda_rata2, f.dok_rata2,
       coalesce(f.sepeda_tersedia - f.prev, f.sepeda_tersedia - f.sepeda_awal),
       f.sepeda_tersedia = 0, f.dok_tersedia = 0, f.n, f.terakhir
from f left join elt.weather_hourly wh on wh.hour_ts = date_trunc('hour', f.waktu)
on conflict (station_id, waktu) do update set tanggal = excluded.tanggal, jam = excluded.jam,
  hari = excluded.hari, akhir_pekan = excluded.akhir_pekan,
  suhu_c = coalesce(excluded.suhu_c, elt.fact_station_30min.suhu_c),
  hujan_mm = coalesce(excluded.hujan_mm, elt.fact_station_30min.hujan_mm),
  angin_kmh = coalesce(excluded.angin_kmh, elt.fact_station_30min.angin_kmh),
  sepeda_awal = excluded.sepeda_awal, sepeda_tersedia = excluded.sepeda_tersedia,
  ebike_tersedia = excluded.ebike_tersedia, dok_tersedia = excluded.dok_tersedia,
  sepeda_rusak = excluded.sepeda_rusak, dok_rusak = excluded.dok_rusak,
  sepeda_rata2 = excluded.sepeda_rata2, dok_rata2 = excluded.dok_rata2,
  net_flow_status = excluded.net_flow_status, kosong = excluded.kosong, penuh = excluded.penuh,
  n_snapshot = excluded.n_snapshot, snapshot_terakhir = excluded.snapshot_terakhir, loaded_at = now()
"""


# ================================================================ runner
def _log(cur, bulan, langkah, masuk, keluar, ket=None):
    cur.execute("""insert into elt.transform_log (bulan, langkah, baris_masuk, baris_keluar, baris_ditolak, keterangan)
                   values (%s,%s,%s,%s,%s,%s)""",
                (bulan, langkah, masuk, keluar, None if masuk is None or keluar is None else masuk - keluar, ket))


def _hitung(cur, tabel):
    cur.execute(f"select count(*) from {tabel}")
    return cur.fetchone()[0]


def transform_trips(conn, bulan, batch_id, say=print):
    """Transformasi trip data bulan `bulan` dari batch raw `batch_id`. Mengembalikan ringkasan jumlah baris."""
    from etl import config
    p = {"bulan": bulan, "batch": batch_id, "dmin": config.DURASI_MIN_DETIK, "dmax": config.DURASI_MAX_DETIK}
    ringkas = {}
    with conn.cursor() as cur:
        cur.execute("select baris_load, baris_extract from raw_elt.load_batch where batch_id = %s", (batch_id,))
        load, ext = cur.fetchone()
        if load is None or load != ext:    # gerbang ELT: transform hanya jika raw sudah tersimpan lengkap
            raise RuntimeError(f"Batch raw {batch_id} belum termuat lengkap ({load} dari {ext}); transform dibatalkan.")

        cur.execute(SQL_DIM_LIVE)
        ringkas["dim_live"] = cur.rowcount
        _log(cur, bulan, "R1/R2 dim_station dari GBFS information", None, cur.rowcount)

        cur.execute(SQL_CUACA)
        ringkas["cuaca"] = cur.rowcount
        _log(cur, bulan, "R7 cuaca per jam (arsip diutamakan)", None, cur.rowcount)

        cur.execute(SQL_T0, p); n0 = _hitung(cur, "_t0")
        cur.execute(SQL_T1); n1 = _hitung(cur, "_t1")
        cur.execute(SQL_T2); n2 = _hitung(cur, "_t2")
        cur.execute(SQL_T3, p); n3 = _hitung(cur, "_t3")
        cur.execute(SQL_T4, p); n4 = _hitung(cur, "_t4")
        _log(cur, bulan, "R3 konversi waktu (tidak valid dibuang)", n0, n1)
        _log(cur, bulan, "R4 duplikat ride_id", n1, n2)
        _log(cur, bulan, "R5 durasi < 1 menit atau > 24 jam", n2, n3)
        _log(cur, bulan, "filter bulan file", n3, n4)
        ringkas.update(mentah=n0, waktu_valid=n1, tanpa_duplikat=n2, durasi_wajar=n3, bersih=n4)

        cur.execute(SQL_MAP, p)
        cur.execute(SQL_DIM_HIST); ringkas["dim_historical_baru"] = cur.rowcount
        _log(cur, bulan, "R2 stasiun hanya di trip data -> dim_station", None, cur.rowcount)
        cur.execute(SQL_TRIPS_CLEAN, p)
        ringkas["trips_clean"] = n4
        cur.execute(SQL_FACT_TRIPS, p)
        ringkas["fact_trips"] = cur.rowcount
        _log(cur, bulan, "R3/R6/R7 agregasi stasiun x slot + cuaca -> fact", n4, cur.rowcount)
    return ringkas


def transform_gbfs(conn, say=print):
    """Transformasi snapshot GBFS mentah: silver (bersih) lalu agregasi slot ke fact."""
    ringkas = {}
    with conn.cursor() as cur:
        cur.execute(SQL_DIM_LIVE)
        cur.execute("select count(*) from raw_elt.gbfs_station_status")
        n_raw = cur.fetchone()[0]
        cur.execute(SQL_GBFS_CLEAN); ringkas["status_clean_baru"] = cur.rowcount
        _log(cur, None, "R1/R2/R4 status GBFS -> silver", n_raw, cur.rowcount)
        cur.execute(SQL_FACT_STATUS); ringkas["fact_status"] = cur.rowcount
        _log(cur, None, "R6 agregasi slot 30 menit -> fact", None, cur.rowcount)
    return ringkas
