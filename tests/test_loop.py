"""Uji kapan loop otomatis menjalankan job harian (tanpa database)."""
import datetime as dt
import unittest
from unittest import mock

import pandas as pd

from etl import loop

UTC = dt.timezone.utc


class TestPerluHarian(unittest.TestCase):
    def _cek(self, sekarang, jumlah_weather):
        with mock.patch.object(loop.load, "query_df", return_value=pd.DataFrame({"n": [jumlah_weather]})) as q:
            hasil = loop.perlu_harian(None, sekarang)
            batas = q.call_args.args[2][0]
        return hasil, batas

    def test_setelah_06_utc_dan_belum_jalan(self):
        hasil, batas = self._cek(dt.datetime(2026, 11, 18, 7, 0, tzinfo=UTC), 0)
        self.assertTrue(hasil)
        self.assertEqual(batas, dt.datetime(2026, 11, 18, 6, 0, tzinfo=UTC))

    def test_sudah_jalan_hari_ini(self):
        hasil, _ = self._cek(dt.datetime(2026, 11, 18, 7, 0, tzinfo=UTC), 1)
        self.assertFalse(hasil)

    def test_sebelum_06_utc_memakai_batas_kemarin(self):
        _, batas = self._cek(dt.datetime(2026, 11, 18, 3, 0, tzinfo=UTC), 1)
        self.assertEqual(batas, dt.datetime(2026, 11, 17, 6, 0, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main(verbosity=2)
