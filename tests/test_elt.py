"""Uji unit ELT tanpa jaringan dan tanpa database."""
import sys
import types

import pandas as pd
import pytest

# psycopg2 tidak dibutuhkan untuk uji ini; dipalsukan hanya jika belum terpasang
try:
    import psycopg2  # noqa: F401
    import psycopg2.extras  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")
    sys.modules["psycopg2.extensions"] = types.SimpleNamespace(AsIs=object, register_adapter=lambda *a, **k: None)
    sys.modules["psycopg2.extras"] = types.SimpleNamespace(execute_values=None)

from elt import extract, load, transform  # noqa: E402


def test_raw_weather_tetap_teks_dan_apa_adanya():
    js = {"hourly": {"time": ["2026-10-01T00:00"], "temperature_2m": [12.5],
                     "precipitation": [0.0], "wind_speed_10m": [None]}}
    df = extract._hourly_raw(js, "archive")
    assert df.loc[0, "temperature_2m"] == "12.5" and df.loc[0, "precipitation"] == "0.0"
    assert df.loc[0, "wind_speed_10m"] is None and df.loc[0, "sumber_api"] == "archive"


def test_csv_load_memetakan_kosong_ke_null():
    isi = load._csv([(1, "", None, "abc")]).read()
    assert isi.strip() == r"1,\N,\N,abc"


def test_urutan_transform_trips_memakai_gerbang_raw_lengkap():
    """transform_trips menolak batch yang baris_load-nya belum sama dengan baris_extract."""
    class Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): self.sql = a[0]
        def fetchone(self): return (100, 111)   # tersimpan 100 dari 111
    class Conn:
        def cursor(self): return Cur()
    with pytest.raises(RuntimeError, match="belum termuat lengkap"):
        transform.transform_trips(Conn(), "202610", 1)


def test_semua_langkah_sql_memakai_schema_raw_dan_elt():
    for nama in ("SQL_DIM_LIVE", "SQL_CUACA", "SQL_T0", "SQL_GBFS_CLEAN"):
        assert "raw_elt." in getattr(transform, nama)
    for nama in ("SQL_FACT_TRIPS", "SQL_FACT_STATUS", "SQL_TRIPS_CLEAN"):
        sql = getattr(transform, nama)
        assert "elt." in sql and "update raw_elt" not in sql.lower() and "delete from raw_elt" not in sql.lower()


def test_transform_tidak_pernah_mengubah_raw():
    for nama in dir(transform):
        if nama.startswith("SQL_"):
            sql = getattr(transform, nama).lower()
            for kata in ("update raw_elt", "delete from raw_elt", "insert into raw_elt", "truncate raw_elt"):
                assert kata not in sql
