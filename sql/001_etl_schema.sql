-- Skema target storage pipeline ETL (Supabase project: citibike-etl).
-- Sudah diterapkan sebagai migration "etl_schema_awal"; disimpan di sini sebagai dokumentasi.
-- Target ETL hanya berisi data yang SUDAH bersih (transformasi terjadi di Python sebelum load).

create table public.dim_station (
  station_id       text primary key,          -- ID historical = short_name GBFS (mis. JC032, HB612)
  live_station_id  text,                      -- station_id GBFS (UUID)
  station_name     text not null,
  wilayah          text not null check (wilayah in ('Jersey City','Hoboken')),
  capacity         integer check (capacity >= 0),
  lat              double precision,
  lng              double precision,
  in_live_feed     boolean not null default false,
  metode_pemetaan  text,                      -- short_name / historical_saja
  updated_at       timestamptz not null default now()
);

create table public.weather_hourly (
  hour_ts     timestamp primary key,          -- waktu lokal America/New_York
  suhu_c      real,
  hujan_mm    real,
  angin_kmh   real,
  sumber_api  text not null,                  -- archive / forecast
  loaded_at   timestamptz not null default now()
);

create table public.fact_station_30min (
  station_id        text not null references public.dim_station(station_id),
  waktu             timestamp not null,       -- awal slot 30 menit, waktu lokal America/New_York
  tanggal           date not null,
  jam               smallint not null check (jam between 0 and 23),
  hari              smallint not null check (hari between 0 and 6),   -- 0 = Senin
  akhir_pekan       boolean not null,
  suhu_c            real,
  hujan_mm          real,
  angin_kmh         real,
  trips_keluar      integer check (trips_keluar >= 0),      -- dari trip data (batch)
  trips_masuk       integer check (trips_masuk >= 0),
  net_flow_trip     integer,
  sepeda_awal       integer,                                -- dari GBFS (streaming)
  sepeda_tersedia   integer check (sepeda_tersedia >= 0),
  ebike_tersedia    integer check (ebike_tersedia >= 0),
  dok_tersedia      integer check (dok_tersedia >= 0),
  sepeda_rusak      integer check (sepeda_rusak >= 0),
  dok_rusak         integer check (dok_rusak >= 0),
  sepeda_rata2      real,
  dok_rata2         real,
  net_flow_status   integer,
  kosong            boolean,
  penuh             boolean,
  n_snapshot        integer check (n_snapshot > 0),
  snapshot_terakhir timestamptz,
  sumber            text generated always as (
                      case when trips_keluar is not null and n_snapshot is not null then 'keduanya'
                           when trips_keluar is not null then 'historical'
                           else 'streaming' end) stored,
  loaded_at         timestamptz not null default now(),
  primary key (station_id, waktu)
);
create index fact_station_30min_waktu_idx on public.fact_station_30min (waktu);

create table public.etl_batch_loaded (       -- file trip bulanan yang sudah dimuat (extract inkremental)
  file_name    text primary key,
  bulan        text not null,
  baris_mentah integer,
  baris_bersih integer,
  baris_load   integer,
  loaded_at    timestamptz not null default now()
);

create table public.etl_log_run (            -- monitoring setiap job
  id               bigserial primary key,
  job              text not null,
  mulai            timestamptz not null,
  selesai          timestamptz,
  status           text not null check (status in ('sukses','gagal','dilewati')),
  baris_extract    integer,
  baris_transform  integer,
  baris_load       integer,
  pesan            text,
  github_run_id    text
);
create index etl_log_run_mulai_idx on public.etl_log_run (mulai desc);

create table public.etl_quality_check (      -- hasil uji kualitas data setelah load
  id          bigserial primary key,
  dicek_pada  timestamptz not null default now(),
  nama_cek    text not null,
  lolos       boolean not null,
  nilai       text,
  keterangan  text
);

-- RLS: pipeline memakai role postgres. Data publik boleh dibaca (anon) untuk dashboard web; log tidak.
alter table public.dim_station        enable row level security;
alter table public.weather_hourly     enable row level security;
alter table public.fact_station_30min enable row level security;
alter table public.etl_batch_loaded   enable row level security;
alter table public.etl_log_run        enable row level security;
alter table public.etl_quality_check  enable row level security;
create policy "baca publik" on public.dim_station        for select to anon using (true);
create policy "baca publik" on public.weather_hourly     for select to anon using (true);
create policy "baca publik" on public.fact_station_30min for select to anon using (true);
