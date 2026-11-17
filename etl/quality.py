"""Uji kualitas data SETELAH load. Hasil setiap cek disimpan ke tabel
etl_quality_check; job gagal (dan GitHub mengirim email) jika ada cek kritis
yang tidak lolos."""
from . import config, load

# (nama, query yang mengembalikan satu angka, syarat lolos, kritis?, keterangan)
CEK = [
    ("tidak_ada_duplikat_kunci",
     "select count(*) - count(distinct (station_id, waktu)) from public.fact_station_30min",
     lambda v: v == 0, True, "Satu baris per stasiun per slot"),
    ("slot_30_menit_valid",
     "select count(*) from public.fact_station_30min "
     "where extract(minute from waktu) not in (0, 30) or extract(second from waktu) <> 0",
     lambda v: v == 0, True, "Semua waktu tepat di awal slot 30 menit (R3)"),
    ("stasiun_hanya_jc_hb",
     "select count(*) from public.dim_station where station_id !~ '^(JC|HB)'",
     lambda v: v == 0, True, "Hanya stasiun Jersey City/Hoboken (R1)"),
    ("fakta_punya_stasiun",
     "select count(*) from public.fact_station_30min f left join public.dim_station d using (station_id) "
     "where d.station_id is null",
     lambda v: v == 0, True, "Setiap baris fakta terhubung ke dim_station (R2)"),
    ("net_flow_trip_konsisten",
     "select count(*) from public.fact_station_30min where net_flow_trip <> trips_masuk - trips_keluar",
     lambda v: v == 0, True, "net_flow_trip = trips_masuk - trips_keluar (R6)"),
    ("kosong_penuh_konsisten",
     "select count(*) from public.fact_station_30min "
     "where n_snapshot is not null and (kosong <> (sepeda_tersedia = 0) or penuh <> (dok_tersedia = 0))",
     lambda v: v == 0, True, "Penanda kosong/penuh sesuai jumlah sepeda/dok (R6)"),
    ("nilai_tidak_negatif",
     "select count(*) from public.fact_station_30min "
     "where sepeda_tersedia < 0 or dok_tersedia < 0 or trips_keluar < 0 or trips_masuk < 0",
     lambda v: v == 0, True, "Tidak ada jumlah negatif"),
    ("kalender_terisi",
     "select count(*) from public.fact_station_30min "
     "where tanggal is null or jam is null or hari is null or akhir_pekan is null",
     lambda v: v == 0, True, "Kolom tanggal, jam, hari, akhir_pekan terisi"),
    ("data_streaming_segar_menit",
     "select coalesce(extract(epoch from now() - max(snapshot_terakhir)) / 60, 99999)::int "
     "from public.fact_station_30min",
     lambda v: v <= config.BATAS_SEGAR_MENIT, False,
     f"Snapshot GBFS terakhir kurang dari {config.BATAS_SEGAR_MENIT} menit lalu"),
    ("cakupan_cuaca_persen",
     "select coalesce(round(100.0 * count(suhu_c) / nullif(count(*), 0)), 0)::int "
     "from public.fact_station_30min where waktu < now() at time zone 'America/New_York'",
     lambda v: v >= 95, False, "Persentase baris fakta yang sudah punya data cuaca (R7)"),
    ("bulan_trip_dimuat",
     "select count(*) from public.etl_batch_loaded",
     lambda v: v >= 1, False, "Jumlah file trip bulanan yang sudah dimuat"),
]


def jalankan(conn):
    gagal_kritis, hasil = [], []
    with conn.cursor() as cur:
        for nama, sql, syarat, kritis, ket in CEK:
            cur.execute(sql)
            nilai = cur.fetchone()[0]
            nilai = int(nilai) if nilai is not None else None
            lolos = bool(nilai is not None and syarat(nilai))
            cur.execute("insert into public.etl_quality_check (nama_cek, lolos, nilai, keterangan) "
                        "values (%s, %s, %s, %s)", (nama, lolos, str(nilai), ket))
            hasil.append((nama, lolos, nilai, kritis))
            if kritis and not lolos:
                gagal_kritis.append(f"{nama}={nilai}")
    conn.commit()
    return hasil, gagal_kritis


if __name__ == "__main__":
    c = load.connect()
    for row in jalankan(c)[0]:
        print(row)
