import unittest

import pandas as pd

from organizer.core import build_message, build_summary, parse_inputs, stable_orders_fingerprint
from organizer.customers import legacy_names, with_customer_labels


class CustomerIdentityTests(unittest.TestCase):
    def test_homonyms_have_separate_totals_but_messages_use_the_original_name(self):
        orders = pd.DataFrame({"Cliente": ["Ana", "Ana"], "UserId": ["11111", "33333"],
                               "Produto": ["REF1", "REF1"], "Quantidade": [1, 2]})
        prices = pd.DataFrame({"Produto": ["REF1"], "Preco": [10]})
        parsed = parse_inputs(orders, prices)
        summary, details = build_summary(parsed.merged)
        self.assertEqual(summary["Total"].tolist(), [10, 20])
        self.assertEqual(len(details), 2)
        self.assertEqual(legacy_names(parsed.orders), {})
        message = build_message(summary.iloc[0]["Cliente"], details[summary.iloc[0]["Cliente"]],
                                10, "EUR", "", "")
        self.assertTrue(message.startswith("Ana:\n"))
        self.assertNotIn("11111", message)

    def test_rows_missing_ids_join_only_an_unambiguous_customer(self):
        orders = pd.DataFrame({"Cliente": ["Ana", "Ana", "Ana"], "UserId": ["11111", "", ""],
                               "ProfileId": ["ana.silva", "ana.silva", ""]})
        self.assertEqual(with_customer_labels(orders)["ClienteKey"].nunique(), 1)
        orders.loc[1, ["UserId", "ProfileId"]] = ["33333", "another.ana"]
        labeled = with_customer_labels(orders)
        self.assertEqual(labeled["ClienteKey"].nunique(), 3)
        self.assertEqual(labeled.iloc[2]["Cliente"], "Ana · sem ID")

    def test_real_names_cannot_collide_with_disambiguation_labels(self):
        orders = pd.DataFrame({"Cliente": ["Ana", "Ana", "Ana · ID 11111", "Ana · ID 11111 [user:11111]"],
                               "UserId": ["11111", "33333", "55555", "77777"]})
        labeled = with_customer_labels(orders)
        self.assertEqual(labeled["Cliente"].nunique(), 4)

    def test_directory_cache_fingerprint_distinguishes_customer_identity(self):
        orders = pd.DataFrame({"Cliente": ["Ana"], "UserId": ["11111"], "Produto": ["REF1"], "Quantidade": [1]})
        first = with_customer_labels(orders)
        orders["UserId"] = "33333"
        self.assertNotEqual(stable_orders_fingerprint(first), stable_orders_fingerprint(with_customer_labels(orders)))

    def test_missing_ids_do_not_merge_unrelated_customer_names(self):
        orders = pd.DataFrame({"Cliente": ["Ana", "Bruno"], "UserId": [None, pd.NA],
                               "ProfileId": [pd.NA, None], "Produto": ["REF1", "REF1"], "Quantidade": [1, 2]})
        parsed = parse_inputs(orders, pd.DataFrame({"Produto": ["REF1"], "Preco": [10]}))
        self.assertEqual(parsed.orders["ClienteKey"].nunique(), 2)
        self.assertEqual(build_summary(parsed.merged)[0]["Total"].tolist(), [10, 20])


if __name__ == "__main__":
    unittest.main()
