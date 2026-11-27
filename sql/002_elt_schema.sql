-- Skema target storage pipeline ELT (Supabase project: citibike-etl, schema terpisah dari ETL).
-- Diterapkan sebagai migration "elt_schema_awal"; disimpan di sini sebagai dokumentasi.
--
--   raw_elt  = zona RAW (bronze): data mentah persis seperti sumber, semua kolom bertipe teks / jsonb.
--   elt      = zona hasil transformasi: silver (trips_clean, gbfs_status_clean) dan gold (dim, fact).
--   public   = target pipeline ETL Minggu 10 (tidak disentuh ELT; dipakai untuk pembanding).

create schema if not exists raw_elt;
create schema if not exists elt;

-- ============================================================ RAW (bronze)
create table raw_elt.load_batch (          -- satu baris per pemuatan raw (jejak asal data)
  batch_id        bigserial primary key,
  sumber          text not null check (sumber in ('trips','gbfs_station_information','gbfs_station_status','weather')),
  source_file     text,                      -- nama file / URL sumber
  bulan           text,                      -- YYYYMM untuk trip data
  baris_extract   integer not null,          -- jumlah baris hasil ekstraksi
  baris_load      integer,                   -- jumlah baris yang tersimpan (diisi setelah COPY)
  loaded_at       timestamptz not null default now(),
  github_run_id   text
);

create table raw_elt.trips (               -- 13 kolom file CSV, semua teks, tanpa cleaning
  batch_id            bigint  not null references raw_elt.load_batch(batch_id),
  row_no              integer not null,    -- urutan baris di file sumber
  ride_id             text, rideable_type text, started_at text, ended_at text,
  start_station_name  text, start_station_id text, end_station_name text, end_station_id text,
  start_lat           text, start_lng text, end_lat text, end_lng text, member_casual text,
  primary key (batch_id, row_no)
);

create table raw_elt.gbfs_station_information (   -- satu baris per stasiun, objek JSON asli
  batch_id bigint not null references raw_elt.load_batch(batch_id),
  row_no   integer not null,
  payload  jsonb not null,
  primary key (batch_id, row_no)
);

create table raw_elt.gbfs_station_status (
  batch_id      bigint not null references raw_elt.load_batch(batch_id),
  row_no        integer not null,
  snapshot_time text not null,               -- last_updated (detik epoch) dari feed, apa adanya
  payload       jsonb not null,
  primary key (batch_id, row_no)
);

create table raw_elt.weather (             -- respons Open-Meteo per jam, nilai sebagai teks
  batch_id bigint not null references raw_elt.load_batch(batch_id),
  row_no   integer not null,
  hour_ts text, temperature_2m text, precipitation text, wind_speed_10m text, sumber_api text,
  primary key (batch_id, row_no)
);

-- ============================================================ TRANSFORM (silver + gold)
create table elt.dim_station (
  station_id text primary key, live_station_id text, station_name text not null,
  wilayah text not null check (wilayah in ('Jersey City','Hoboken')),
  capacity integer check (capacity >= 0), lat double precision, lng double precision,
  in_live_feed boolean not null default false, metode_pemetaan text,
  updated_at timestamptz not null default now()
);

create table elt.weather_hourly (
  hour_ts timestamp primary key, suhu_c real, hujan_mm real, angin_kmh real, sumber_api text not null,
  loaded_at timestamptz not null default now()
);

create table elt.trips_clean (             -- silver: perjalanan bersih + ID stasiun terpetakan
  bulan text not null, ride_id text not null,
  started_at timestamp not null, ended_at timestamp not null,
  start_sid text, end_sid text,
  source_batch_id bigint not null references raw_elt.load_batch(batch_id),
  primary key (bulan, ride_id)
);

create table elt.gbfs_status_clean (       -- silver: snapshot status stasiun JC/HB aktif
  station_id text not null references elt.dim_station(station_id),
  snapshot_terakhir timestamptz not null, snapshot_dt timestamp not null, waktu timestamp not null,
  num_bikes_available integer, num_ebikes_available integer, num_docks_available integer,
  num_bikes_disabled integer, num_docks_disabled integer,
  primary key (station_id, snapshot_terakhir)
);

create table elt.fact_station_30min (      -- gold: kolom sama dengan fact_station_30min pipeline ETL
  station_id text not null references elt.dim_station(station_id),
  waktu timestamp not null, tanggal date not null,
  jam smallint not null check (jam between 0 and 23), hari smallint not null check (hari between 0 and 6),
  akhir_pekan boolean not null,
  suhu_c real, hujan_mm real, angin_kmh real,
  trips_keluar integer check (trips_keluar >= 0), trips_masuk integer check (trips_masuk >= 0), net_flow_trip integer,
  sepeda_awal integer, sepeda_tersedia integer check (sepeda_tersedia >= 0), ebike_tersedia integer check (ebike_tersedia >= 0),
  dok_tersedia integer check (dok_tersedia >= 0), sepeda_rusak integer check (sepeda_rusak >= 0), dok_rusak integer check (dok_rusak >= 0),
  sepeda_rata2 real, dok_rata2 real, net_flow_status integer, kosong boolean, penuh boolean,
  n_snapshot integer check (n_snapshot > 0), snapshot_terakhir timestamptz,
  sumber text generated always as (
    case when trips_keluar is not null and n_snapshot is not null then 'keduanya'
         when trips_keluar is not null then 'historical' else 'streaming' end) stored,
  loaded_at timestamptz not null default now(),
  primary key (station_id, waktu)
);
create index elt_fact_waktu_idx on elt.fact_station_30min (waktu);

create table elt.transform_log (           -- jumlah baris masuk/keluar/ditolak tiap langkah transformasi
  id bigserial primary key, run_ts timestamptz not null default now(),
  bulan text, langkah text not null, baris_masuk integer, baris_keluar integer, baris_ditolak integer, keterangan text
);

create table elt.run_log (                 -- satu baris per eksekusi pipeline ELT
  id bigserial primary key, job text not null, mulai timestamptz not null, selesai timestamptz,
  status text not null check (status in ('sukses','gagal','dilewati')),
  baris_extract integer, baris_load integer, baris_transform integer, pesan text, github_run_id text
);

-- Skema ini tidak diekspos ke API publik Supabase; RLS tetap diaktifkan sebagai lapisan tambahan.
do $$ declare t record; begin
  for t in select schemaname, tablename from pg_tables where schemaname in ('raw_elt','elt') loop
    execute format('alter table %I.%I enable row level security', t.schemaname, t.tablename);
  end loop; end $$;
