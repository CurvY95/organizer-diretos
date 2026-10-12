import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from organizer import sessions_json


class SessionTests(unittest.TestCase):
    def test_saves_in_the_same_second_preserve_both_snapshots(self):
        orders = pd.DataFrame({"Cliente": ["Ana"], "Produto": ["REF 1"], "Quantidade": [1]})
        with tempfile.TemporaryDirectory(prefix="organizer-sessions-") as directory:
            with patch.object(sessions_json, "SESSIONS_DIR", directory), patch.object(
                sessions_json, "now_iso", return_value="2026-10-12T13:00:00+00:00"
            ):
                first_id, _ = sessions_json.save_session(label="Antes", orders_for_calc=orders,
                                                        price_overrides={}, meta={})
                orders.loc[0, "Quantidade"] = 3
                second_id, _ = sessions_json.save_session(label="Depois", orders_for_calc=orders,
                                                         price_overrides={}, meta={})
                self.assertNotEqual(first_id, second_id)
                snapshots = {row["id"]: sessions_json.load_session(row["path"])
                             for row in sessions_json.list_sessions()}
                self.assertEqual(len(snapshots), 2)
                self.assertEqual(snapshots[first_id]["orders"][0]["Quantidade"], 1)
                self.assertEqual(snapshots[second_id]["orders"][0]["Quantidade"], 3)
