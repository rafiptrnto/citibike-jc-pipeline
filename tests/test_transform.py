"""Uji unit untuk setiap aturan transformasi (R1-R7).

Dijalankan otomatis oleh GitHub Actions setiap ada perubahan kode:
    python -m pytest -v        (atau: python -m unittest -v)
Tidak butuh internet maupun database: semua memakai data contoh kecil.
"""
import unittest

import pandas as pd

from etl import transform as T


def info_contoh():
    return pd.DataFrame({
        "station_id": ["uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5", "uuid-6"],
        "short_name": ["JC001", "hb101", "5509.02", "JC002", "JC003", "JC003"],
        "name": ["Grove St", "Hoboken Terminal", "Times Sq", "Exchange Pl", "Dup A", "Dup B"],
        "lat": [40.71, 40.73, 40.75, 40.71, 40.7, 40.7],
        "lon": [-74.04, -74.02, -73.98, -74.03, -74.0, -74.0],
        "capacity": [20, 30, 50, 0, 10, 10],
    })


def status_contoh(epoch, bikes):
    return pd.DataFrame({
        "station_id": ["uuid-1", "uuid-2", "uuid-3"],
        "num_bikes_available": bikes,
        "num_ebikes_available": [1, 0, 2],
        "num_docks_available": [20 - bikes[0], 30 - bikes[1], 10],
        "num_bikes_disabled": [0, 1, 0],
        "num_docks_disabled": [0, 0, 0],
        "is_installed": [1, 1, 1],
        "snapshot_time": [epoch] * 3,
    })


class TestR1R2Stasiun(unittest.TestCase):
    def test_hanya_jc_hb_dengan_kapasitas(self):
        dim = T.transform_station_info(info_contoh())
        # 5509.02 = NYC (dibuang), JC002 kapasitas 0 (dibuang), JC003 ganda/ambigu (dibuang)
        self.assertEqual(sorted(dim["station_id"]), ["HB101", "JC001"])
        self.assertEqual(dict(zip(dim.station_id, dim.wilayah)),
                         {"JC001": "Jersey City", "HB101": "Hoboken"})

    def test_status_dipetakan_ke_id_historical(self):
        dim = T.transform_station_info(info_contoh())
        s = T.transform_status_snapshot(status_contoh(1_763_000_000, [5, 0, 7]), dim)
        self.assertEqual(sorted(s["station_id"]), ["HB101", "JC001"])  # uuid-3 (NYC) hilang

    def test_stasiun_tidak_terpasang_dibuang(self):
        dim = T.transform_station_info(info_contoh())
        st = status_contoh(1_763_000_000, [5, 0, 7])
        st.loc[0, "is_installed"] = 0
        s = T.transform_status_snapshot(st, dim)
        self.assertEqual(list(s["station_id"]), ["HB101"])

    def test_pemetaan_trip_cadangan_lewat_nama(self):
        dim = T.transform_station_info(info_contoh())
        trips = pd.DataFrame({
            "start_station_id": ["JC001", "JC999", "XX1", "HB500"],
            "start_station_name": ["Grove St", "Hoboken Terminal", "Somewhere", "Stasiun Lama"],
            "end_station_id": ["HB101"] * 4, "end_station_name": ["Hoboken Terminal"] * 4,
            "start_lat": [40.7] * 4, "start_lng": [-74.0] * 4, "end_lat": [40.7] * 4, "end_lng": [-74.0] * 4,
        })
        t, dim_baru = T.petakan_stasiun_trip(trips, dim)
        # JC001 cocok lewat ID, JC999 cocok lewat nama -> HB101, XX1 bukan JC/HB, HB500 stasiun lama
        self.assertEqual(list(t["start_sid"].astype(object).where(t["start_sid"].notna(), None)),
                         ["JC001", "HB101", None, "HB500"])
        self.assertEqual(list(dim_baru["station_id"]), ["HB500"])
        self.assertFalse(dim_baru["in_live_feed"].iloc[0])


class TestR3Waktu(unittest.TestCase):
    def test_utc_ke_new_york_dan_slot(self):
        # 2026-11-17 18:47 UTC = 13:47 waktu New York (EST, UTC-5) -> slot 13:30
        dim = T.transform_station_info(info_contoh())
        epoch = int(pd.Timestamp("2026-11-17 18:47:10", tz="UTC").timestamp())
        s = T.transform_status_snapshot(status_contoh(epoch, [5, 0, 7]), dim)
        self.assertEqual(s["snapshot_dt"].iloc[0], pd.Timestamp("2026-11-17 13:47:10"))
        self.assertEqual(s["waktu"].iloc[0], pd.Timestamp("2026-11-17 13:30"))

    def test_kalender(self):
        df = T.tambah_kalender(pd.DataFrame({"waktu": pd.to_datetime(["2026-11-15 08:30"])}))  # Minggu
        self.assertEqual((df.jam[0], df.hari[0], bool(df.akhir_pekan[0])), (8, 6, True))


def trips_contoh():
    return pd.DataFrame({
        "ride_id": ["a", "a", "b", "c", "d", "e", "f"],
        "started_at": ["2026-10-01 08:05:00", "2026-10-01 08:05:00", "2026-10-01 08:20:00",
                       "2026-10-01 08:40:00", "2026-10-01 09:00:00", "2026-09-30 23:50:00",
                       "2026-10-01 10:00:00"],
        "ended_at": ["2026-10-01 08:15:00", "2026-10-01 08:15:00", "2026-10-01 08:20:30",
                     "2026-10-01 08:55:00", "2026-10-03 09:00:00", "2026-10-01 00:05:00",
                     "2026-10-01 10:12:00"],
        "start_station_id": ["JC001", "JC001", "JC001", "JC001", "JC001", "JC001", "HB101"],
        "start_station_name": ["Grove St"] * 6 + ["Hoboken Terminal"],
        "end_station_id": ["HB101"] * 6 + ["JC001"],
        "end_station_name": ["Hoboken Terminal"] * 6 + ["Grove St"],
        "start_lat": [40.7] * 7, "start_lng": [-74.0] * 7, "end_lat": [40.7] * 7, "end_lng": [-74.0] * 7,
    })


class TestR4R5Trips(unittest.TestCase):
    def test_pembersihan(self):
        bersih, r = T.bersihkan_trips(trips_contoh(), "202610")
        self.assertEqual(r["duplikat_ride_id"], 1)       # R4: ride_id 'a' dua kali
        self.assertEqual(r["durasi_tidak_wajar"], 2)     # R5: 'b' 30 detik, 'd' 48 jam
        self.assertEqual(r["di_luar_bulan"], 1)          # 'e' mulai 30 Sep
        self.assertEqual(sorted(bersih["ride_id"]), ["a", "c", "f"])


class TestR6Agregasi(unittest.TestCase):
    def test_grid_trips(self):
        dim = T.transform_station_info(info_contoh())
        bersih, _ = T.bersihkan_trips(trips_contoh(), "202610")
        t, _ = T.petakan_stasiun_trip(bersih, dim)
        f = T.agregasi_trips(t, "202610").set_index(["station_id", "waktu"])
        self.assertEqual(len(f), 2 * 31 * 48)                       # grid lengkap 2 stasiun x 1.488 slot
        self.assertEqual(f.loc[("JC001", pd.Timestamp("2026-10-01 08:00")), "trips_keluar"], 1)
        self.assertEqual(f.loc[("JC001", pd.Timestamp("2026-10-01 08:30")), "trips_keluar"], 1)
        self.assertEqual(f.loc[("HB101", pd.Timestamp("2026-10-01 08:30")), "trips_masuk"], 1)
        self.assertEqual(f.loc[("JC001", pd.Timestamp("2026-10-01 03:00")), "trips_keluar"], 0)
        self.assertTrue((f["net_flow_trip"] == f["trips_masuk"] - f["trips_keluar"]).all())
        self.assertEqual(f["trips_keluar"].sum(), 3)

    def _snap(self, waktu_str, bikes, docks):
        t = pd.Timestamp(waktu_str)
        return pd.DataFrame({"station_id": ["JC001"], "waktu": [t.floor("30min")], "snapshot_dt": [t],
                             "snapshot_terakhir": [t.tz_localize("America/New_York").tz_convert("UTC")],
                             "num_bikes_available": [bikes], "num_ebikes_available": [0],
                             "num_docks_available": [docks], "num_bikes_disabled": [0],
                             "num_docks_disabled": [0], "is_installed": [1]})

    def test_agregasi_snapshot_dan_kosong_penuh(self):
        snaps = pd.concat([self._snap("2026-11-17 08:01", 4, 16), self._snap("2026-11-17 08:11", 2, 18),
                           self._snap("2026-11-17 08:21", 0, 20), self._snap("2026-11-17 08:31", 20, 0)])
        g = T.gabung_status(T.aggregate_snapshots(snaps)).set_index("waktu")
        a = g.loc[pd.Timestamp("2026-11-17 08:00")]
        self.assertEqual((a.sepeda_awal, a.sepeda_tersedia, a.n_snapshot), (4, 0, 3))
        self.assertAlmostEqual(a.sepeda_rata2, 2.0)
        self.assertTrue(a.kosong)
        self.assertEqual(a.net_flow_status, -4)          # slot pertama: perubahan di dalam slot
        b = g.loc[pd.Timestamp("2026-11-17 08:30")]
        self.assertTrue(b.penuh)
        self.assertEqual(b.net_flow_status, 20)          # dibanding slot sebelumnya (0 -> 20)

    def test_snapshot_baru_digabung_dengan_isi_lama_dan_idempotent(self):
        lama = T.gabung_status(T.aggregate_snapshots(pd.concat(
            [self._snap("2026-11-17 08:01", 4, 16), self._snap("2026-11-17 08:11", 2, 18)])))
        baru = T.aggregate_snapshots(self._snap("2026-11-17 08:21", 6, 14))
        g = T.gabung_status(baru, lama).iloc[0]
        self.assertEqual((g.n_snapshot, g.sepeda_awal, g.sepeda_tersedia), (3, 4, 6))
        self.assertAlmostEqual(g.sepeda_rata2, 4.0)
        # snapshot yang sama diproses ulang -> dilewati
        lama2 = pd.DataFrame([g]).assign(station_id="JC001", waktu=pd.Timestamp("2026-11-17 08:00"))
        self.assertEqual(len(T.gabung_status(baru, lama2)), 0)

    def test_net_flow_memakai_slot_sebelumnya_dari_target(self):
        prev = pd.DataFrame({"station_id": ["JC001"], "waktu": [pd.Timestamp("2026-11-17 07:30")],
                             "sepeda_tersedia": [10]})
        g = T.gabung_status(T.aggregate_snapshots(self._snap("2026-11-17 08:05", 7, 13)), None, prev)
        self.assertEqual(g.net_flow_status.iloc[0], -3)


class TestR7Cuaca(unittest.TestCase):
    def test_arsip_diutamakan_dan_digabung_per_jam(self):
        raw = pd.DataFrame({"hour_ts": pd.to_datetime(["2026-11-17 08:00", "2026-11-17 08:00", "2026-11-17 09:00"]),
                            "temperature_2m": [5.0, 6.0, 7.0], "precipitation": [0.0, 0.1, 0.0],
                            "wind_speed_10m": [10.0, 11.0, 12.0], "sumber_api": ["forecast", "archive", "forecast"]})
        w = T.transform_weather(raw)
        self.assertEqual(len(w), 2)
        self.assertEqual(w.loc[w.hour_ts == pd.Timestamp("2026-11-17 08:00"), "suhu_c"].iloc[0], 6.0)
        f = pd.DataFrame({"station_id": ["JC001", "JC001"],
                          "waktu": pd.to_datetime(["2026-11-17 08:30", "2026-11-17 09:00"])})
        g = T.gabung_cuaca(f, w)
        self.assertEqual(list(g["suhu_c"]), [6.0, 7.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
