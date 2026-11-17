"""TRANSFORM: seluruh pembersihan dan pengolahan dilakukan di Python,
SEBELUM data dimuat ke target. Modul ini tidak membaca/menulis jaringan atau
database, sehingga setiap aturan bisa diuji dengan data contoh (tests/).

Tujuh aturan transformasi (dokumen rancangan Minggu 9, bagian 4):
  R1  Saring stasiun Jersey City/Hoboken (ID berawalan JC/HB) dan stasiun aktif.
  R2  Petakan ID live (short_name) ke ID historical; cadangan lewat nama stasiun.
  R3  Ubah waktu ke America/New_York dan bulatkan ke slot 30 menit.
  R4  Buang duplikat (ride_id; pasangan snapshot_time + station_id).
  R5  Buang perjalanan berdurasi < 1 menit atau > 24 jam.
  R6  Agregasi per stasiun per 30 menit (trip keluar/masuk, net flow,
      ketersediaan sepeda/dok, penanda kosong/penuh).
  R7  Gabungkan dengan cuaca per jam.
"""
import numpy as np
import pandas as pd

from . import config

SLOT = pd.Timedelta(config.SLOT)
KUNCI = ["station_id", "waktu"]


# ================================================================ util
def wilayah_dari_id(ids):
    """R1: 'JC032' -> 'Jersey City', 'HB612' -> 'Hoboken', lainnya -> NaN."""
    s = pd.Series(ids, dtype="object").astype(str).str.upper()
    hasil = pd.Series(np.nan, index=s.index, dtype="object")
    for prefix, nama in config.PREFIX_WILAYAH.items():
        hasil[s.str.startswith(prefix)] = nama
    return hasil.values


def ke_slot(ts):
    """R3: bulatkan waktu lokal ke awal slot 30 menit."""
    return pd.to_datetime(ts).dt.floor(config.SLOT)


def utc_ke_lokal(ts_utc):
    """R3: waktu UTC -> waktu lokal New York tanpa info zona (sesuai kolom `waktu`)."""
    return pd.to_datetime(ts_utc, utc=True).dt.tz_convert(config.TZ).dt.tz_localize(None)


def tambah_kalender(df):
    w = pd.to_datetime(df["waktu"])
    df = df.copy()
    df["tanggal"] = w.dt.date
    df["jam"] = w.dt.hour.astype(int)
    df["hari"] = w.dt.dayofweek.astype(int)  # 0 = Senin
    df["akhir_pekan"] = df["hari"] >= 5
    return df


# ================================================================ cuaca
def transform_weather(raw):
    """Rapikan cuaca: satu baris per jam, arsip diutamakan di atas forecast."""
    w = raw.copy()
    w["urut"] = (w["sumber_api"] != "archive").astype(int)
    w = (w.sort_values(["hour_ts", "urut"])
          .drop_duplicates("hour_ts", keep="first")
          .rename(columns={"temperature_2m": "suhu_c", "precipitation": "hujan_mm",
                           "wind_speed_10m": "angin_kmh"}))
    w = w.dropna(subset=["suhu_c", "hujan_mm", "angin_kmh"], how="all")
    return w[["hour_ts", "suhu_c", "hujan_mm", "angin_kmh", "sumber_api"]].reset_index(drop=True)


def gabung_cuaca(df, weather):
    """R7: tempel cuaca jam yang sama ke setiap baris stasiun x slot."""
    out = df.copy()
    out["hour_ts"] = pd.to_datetime(out["waktu"]).dt.floor("h")
    cols = ["hour_ts", "suhu_c", "hujan_mm", "angin_kmh"]
    out = out.drop(columns=[c for c in cols[1:] if c in out.columns])
    out = out.merge(weather[cols], on="hour_ts", how="left")
    return out.drop(columns="hour_ts")


# ================================================================ stasiun (GBFS)
def transform_station_info(info):
    """station_information GBFS -> baris dim_station untuk stasiun JC/HB (R1, R2)."""
    d = info.copy()
    d["short_name"] = d["short_name"].astype(str).str.strip().str.upper()
    d["wilayah"] = wilayah_dari_id(d["short_name"])
    d = d[d["wilayah"].notna()]
    d = d[pd.to_numeric(d["capacity"], errors="coerce").fillna(0) > 0]
    # short_name ganda bersifat ambigu -> tidak dipakai (sama seperti notebook gathering)
    d = d.drop_duplicates("short_name", keep=False)
    return pd.DataFrame({
        "station_id": d["short_name"].values,
        "live_station_id": d["station_id"].astype(str).values,
        "station_name": d["name"].astype(str).str.strip().values,
        "wilayah": d["wilayah"].values,
        "capacity": pd.to_numeric(d["capacity"]).astype(int).values,
        "lat": d["lat"].astype(float).values,
        "lng": d["lon"].astype(float).values,
        "in_live_feed": True,
        "metode_pemetaan": "short_name",
    })


# ================================================================ status (GBFS)
STATUS_NUM = ["num_bikes_available", "num_ebikes_available", "num_docks_available",
              "num_bikes_disabled", "num_docks_disabled", "is_installed"]


def transform_status_snapshot(status, dim_live):
    """Snapshot station_status -> satu baris per stasiun JC/HB aktif.

    R2: station_id GBFS dipetakan ke ID historical lewat dim_station.
    R1: hanya stasiun terpetakan (JC/HB) dengan is_installed = 1.
    R3: waktu snapshot diubah ke lokal New York, lalu dibulatkan ke slot.
    R4: pasangan (snapshot_time, station_id) ganda dibuang.
    """
    s = status.copy()
    s["station_id"] = s["station_id"].astype(str)
    s = s.drop_duplicates(["snapshot_time", "station_id"])                       # R4
    peta = dim_live[["live_station_id", "station_id"]].rename(columns={"station_id": "sid"})
    s = s.merge(peta, left_on="station_id", right_on="live_station_id", how="inner")  # R1 + R2
    for c in STATUS_NUM:
        s[c] = pd.to_numeric(s.get(c), errors="coerce")
    s = s[s["is_installed"] == 1]                                              # R1
    s["snapshot_terakhir"] = pd.to_datetime(s["snapshot_time"], unit="s", utc=True)
    s["snapshot_dt"] = utc_ke_lokal(s["snapshot_terakhir"])                    # R3
    s["waktu"] = ke_slot(s["snapshot_dt"])                                     # R3
    out = s.drop(columns=["station_id", "live_station_id"]).rename(columns={"sid": "station_id"})
    return out[["station_id", "waktu", "snapshot_dt", "snapshot_terakhir"] + STATUS_NUM].reset_index(drop=True)


def aggregate_snapshots(snaps):
    """R6 (bagian streaming): banyak snapshot -> satu baris per stasiun per slot.
    Nilai ketersediaan memakai snapshot terakhir di slot itu; rata-rata ikut dihitung."""
    s = snaps.sort_values(["station_id", "snapshot_dt"])
    g = s.groupby(KUNCI, sort=True)
    agg = g.agg(sepeda_awal=("num_bikes_available", "first"),
                sepeda_tersedia=("num_bikes_available", "last"),
                ebike_tersedia=("num_ebikes_available", "last"),
                dok_tersedia=("num_docks_available", "last"),
                sepeda_rusak=("num_bikes_disabled", "last"),
                dok_rusak=("num_docks_disabled", "last"),
                sepeda_rata2=("num_bikes_available", "mean"),
                dok_rata2=("num_docks_available", "mean"),
                n_snapshot=("snapshot_dt", "nunique"),
                snapshot_terakhir=("snapshot_terakhir", "max")).reset_index()
    return agg


def gabung_status(baru, lama=None, slot_sebelumnya=None):
    """Gabungkan agregat baru dengan isi slot yang sudah ada di target.

    baru            : hasil aggregate_snapshots
    lama            : baris slot yang sama yang sudah tersimpan (station_id, waktu,
                      sepeda_awal, n_snapshot, sepeda_rata2, dok_rata2, snapshot_terakhir)
    slot_sebelumnya : sepeda_tersedia slot sebelumnya (station_id, waktu, sepeda_tersedia)

    Snapshot yang sudah pernah diproses dilewati, sehingga job aman dijalankan ulang.
    net_flow_status = perubahan sepeda dibanding slot sebelumnya; jika slot
    sebelumnya tidak ada, perubahan di dalam slot (sama seperti notebook gathering).
    """
    kolom_lama = ["station_id", "waktu", "sepeda_awal", "n_snapshot", "sepeda_rata2",
                  "dok_rata2", "snapshot_terakhir"]
    if lama is None or len(lama) == 0:
        lama = pd.DataFrame(columns=kolom_lama)
    lama = lama[kolom_lama].copy()
    lama["waktu"] = pd.to_datetime(lama["waktu"])
    lama["snapshot_terakhir"] = pd.to_datetime(lama["snapshot_terakhir"], utc=True)

    m = baru.merge(lama, on=KUNCI, how="left", suffixes=("", "_lama"))
    sudah = m["snapshot_terakhir_lama"].notna() & (m["snapshot_terakhir_lama"] >= m["snapshot_terakhir"])
    m = m[~sudah].copy()

    n_lama = pd.to_numeric(m["n_snapshot_lama"], errors="coerce").fillna(0)
    n_total = m["n_snapshot"] + n_lama
    for kol in ("sepeda_rata2", "dok_rata2"):
        lama_val = pd.to_numeric(m[f"{kol}_lama"], errors="coerce").fillna(0)
        m[kol] = (m[kol] * m["n_snapshot"] + lama_val * n_lama) / n_total
    m["n_snapshot"] = n_total.astype(int)
    awal_lama = pd.to_numeric(m["sepeda_awal_lama"], errors="coerce")
    m["sepeda_awal"] = awal_lama.where(awal_lama.notna(), m["sepeda_awal"]).round().astype(int)

    # nilai slot sebelumnya: dari target, ditimpa nilai di batch ini jika ada
    sumber_prev = [baru[["station_id", "waktu", "sepeda_tersedia"]]]
    if slot_sebelumnya is not None and len(slot_sebelumnya):
        sp = slot_sebelumnya[["station_id", "waktu", "sepeda_tersedia"]].copy()
        sp["waktu"] = pd.to_datetime(sp["waktu"])
        sumber_prev.insert(0, sp)
    prev = (pd.concat(sumber_prev, ignore_index=True)
              .drop_duplicates(KUNCI, keep="last")
              .rename(columns={"sepeda_tersedia": "sepeda_prev"}))
    prev["waktu"] = prev["waktu"] + SLOT
    m = m.merge(prev, on=KUNCI, how="left")

    m["net_flow_status"] = np.where(m["sepeda_prev"].notna(),
                                    m["sepeda_tersedia"] - m["sepeda_prev"],
                                    m["sepeda_tersedia"] - m["sepeda_awal"]).astype(int)
    m["kosong"] = m["sepeda_tersedia"] == 0
    m["penuh"] = m["dok_tersedia"] == 0
    kolom = ["station_id", "waktu", "sepeda_awal", "sepeda_tersedia", "ebike_tersedia",
             "dok_tersedia", "sepeda_rusak", "dok_rusak", "sepeda_rata2", "dok_rata2",
             "net_flow_status", "kosong", "penuh", "n_snapshot", "snapshot_terakhir"]
    return m[kolom].reset_index(drop=True)


# ================================================================ trip data (batch)
def bersihkan_trips(raw, bulan):
    """R3, R4, R5 untuk satu file bulanan. Mengembalikan (trips_bersih, ringkasan)."""
    t = raw.copy()
    n0 = len(t)
    t["started_at"] = pd.to_datetime(t["started_at"], errors="coerce", format="mixed")
    t["ended_at"] = pd.to_datetime(t["ended_at"], errors="coerce", format="mixed")
    t = t.dropna(subset=["started_at", "ended_at"])
    n_waktu = n0 - len(t)

    sebelum = len(t)
    t = t.drop_duplicates("ride_id")                                             # R4
    n_dup = sebelum - len(t)

    durasi = (t["ended_at"] - t["started_at"]).dt.total_seconds()
    ok = durasi.between(config.DURASI_MIN_DETIK, config.DURASI_MAX_DETIK)        # R5
    n_durasi = int((~ok).sum())
    t = t[ok]

    # hanya perjalanan yang mulai di bulan file ini (file Juli memuat sedikit data 30 Juni)
    awal = pd.Timestamp(f"{bulan[:4]}-{bulan[4:]}-01")
    akhir = awal + pd.offsets.MonthBegin(1)
    di_bulan = (t["started_at"] >= awal) & (t["started_at"] < akhir)
    n_luar = int((~di_bulan).sum())
    t = t[di_bulan]

    for c in ("start_station_id", "end_station_id"):
        t[c] = t[c].astype("string").str.strip().str.upper()
    ringkasan = {"baris_mentah": n0, "waktu_tidak_valid": n_waktu, "duplikat_ride_id": n_dup,
                 "durasi_tidak_wajar": n_durasi, "di_luar_bulan": n_luar, "baris_bersih": len(t)}
    return t.reset_index(drop=True), ringkasan


def petakan_stasiun_trip(trips, dim):
    """R1 + R2 untuk trip data.

    ID stasiun trip (mis. JC032) sama dengan short_name GBFS, jadi dicocokkan
    langsung ke dim_station. Jika tidak cocok, dicocokkan lewat nama stasiun.
    Stasiun JC/HB yang tidak ada di feed live tetap disimpan (historical_saja).
    Mengembalikan (trips dengan kolom start_sid/end_sid, baris dim baru).
    """
    ids = set(dim["station_id"])
    by_name = (dim.drop_duplicates("station_name", keep=False)
                  .set_index("station_name")["station_id"])
    t = trips.copy()
    for sisi in ("start", "end"):
        sid = t[f"{sisi}_station_id"]
        nama = t[f"{sisi}_station_name"].astype("string").str.strip()
        cocok_id = sid.where(sid.isin(ids))
        cocok_nama = nama.map(by_name)
        hasil = cocok_id.fillna(cocok_nama)
        # stasiun JC/HB yang tidak ada di dim -> tetap pakai ID aslinya
        jc_hb = pd.Series(wilayah_dari_id(sid.fillna("")), index=t.index).notna()
        hasil = hasil.where(hasil.notna(), sid.where(jc_hb))
        t[f"{sisi}_sid"] = hasil

    # stasiun baru (hanya ada di historical)
    baru = []
    for sisi, lat, lng in (("start", "start_lat", "start_lng"), ("end", "end_lat", "end_lng")):
        x = t[t[f"{sisi}_sid"].notna() & ~t[f"{sisi}_sid"].isin(ids)]
        if len(x):
            baru.append(pd.DataFrame({"station_id": x[f"{sisi}_sid"].astype(str),
                                      "station_name": x[f"{sisi}_station_name"].astype(str),
                                      "lat": x[lat], "lng": x[lng]}))
    if baru:
        b = pd.concat(baru, ignore_index=True)
        dim_baru = (b.groupby("station_id")
                      .agg(station_name=("station_name", "first"), lat=("lat", "median"), lng=("lng", "median"))
                      .reset_index())
        dim_baru["wilayah"] = wilayah_dari_id(dim_baru["station_id"])
        dim_baru["live_station_id"] = None
        dim_baru["capacity"] = None
        dim_baru["in_live_feed"] = False
        dim_baru["metode_pemetaan"] = "historical_saja"
    else:
        dim_baru = pd.DataFrame(columns=["station_id", "live_station_id", "station_name", "wilayah",
                                         "capacity", "lat", "lng", "in_live_feed", "metode_pemetaan"])
    return t, dim_baru


def agregasi_trips(trips, bulan):
    """R3 + R6: jumlah perjalanan keluar/masuk per stasiun per 30 menit.
    Dibuat sebagai grid lengkap (stasiun x semua slot di bulan itu), sehingga
    slot tanpa perjalanan tetap tercatat dengan nilai 0."""
    awal = pd.Timestamp(f"{bulan[:4]}-{bulan[4:]}-01")
    akhir = awal + pd.offsets.MonthBegin(1)
    slots = pd.date_range(awal, akhir, freq=config.SLOT, inclusive="left")

    stasiun = sorted(set(trips["start_sid"].dropna()) | set(trips["end_sid"].dropna()))
    keluar = (trips.dropna(subset=["start_sid"])
                   .groupby(["start_sid", ke_slot(trips["started_at"])]).size())
    keluar.index.names = KUNCI
    masuk = (trips.dropna(subset=["end_sid"])
                  .groupby(["end_sid", ke_slot(trips["ended_at"])]).size())
    masuk.index.names = KUNCI

    grid = pd.MultiIndex.from_product([stasiun, slots], names=KUNCI)
    out = pd.DataFrame({"trips_keluar": keluar.reindex(grid, fill_value=0),
                        "trips_masuk": masuk.reindex(grid, fill_value=0)}).reset_index()
    out["net_flow_trip"] = out["trips_masuk"] - out["trips_keluar"]
    return out
