import unittest

import pandas as pd

from organizer.labels import aggregate_labels, build_labels_html, parse_hora_to_seconds


class LabelsTests(unittest.TestCase):
    def test_parse_hora_to_seconds(self):
        self.assertEqual(parse_hora_to_seconds("1:05"), 65)
        self.assertEqual(parse_hora_to_seconds("2:03:04"), 7384)
        self.assertEqual(parse_hora_to_seconds(" 10 : 02 "), 602)
        self.assertIsNone(parse_hora_to_seconds(""))
        self.assertIsNone(parse_hora_to_seconds("abc"))
        self.assertIsNone(parse_hora_to_seconds(pd.NA))
        self.assertIsNone(parse_hora_to_seconds("1::05"))
        self.assertIsNone(parse_hora_to_seconds("2:99"))

    def test_aggregate_labels_uses_earliest_numeric_time(self):
        base = pd.DataFrame({
            "Cliente": ["Ana", "Ana", "Bruno"],
            "Produto": ["REF1", "REF1", "REF1"],
            "Quantidade": [1, 2, 1],
            "Preco": [10, 10, 10],
            "Hora": ["10:00", "2:00", "5:00"],
        })

        labels = aggregate_labels(base)
        labels = labels.assign(seconds=labels["Hora"].map(parse_hora_to_seconds)).sort_values("seconds")

        self.assertEqual(labels["Cliente"].tolist(), ["Ana", "Bruno"])
        self.assertEqual(labels["Hora"].tolist(), ["2:00", "5:00"])
        self.assertEqual(labels["Quantidade"].tolist(), [3, 1])

    def test_aggregate_labels_distinguishes_same_named_client_ids(self):
        base = pd.DataFrame({
            "Cliente": ["Ana (1)", "Ana (1)", "Ana (2)"],
            "ClienteKey": ["user:1", "user:1", "user:2"],
            "ClienteNome": ["Ana", "Ana", "Ana"],
            "Produto": ["REF1", "REF1", "REF1"],
            "Quantidade": [1, 2, 4],
            "Preco": [10, 10, 20],
            "Hora": [None, "2:00", "1:00"],
        })

        labels = aggregate_labels(base).set_index("ClienteKey")

        self.assertEqual(labels.loc["user:1", "Quantidade"], 3)
        self.assertEqual(labels.loc["user:2", "Quantidade"], 4)
        self.assertEqual(labels.loc["user:1", "Hora"], "2:00")
        self.assertEqual(labels.loc["user:2", "Cliente"], "Ana (2)")
        self.assertEqual(labels["ClienteNome"].tolist(), ["Ana", "Ana"])

    def test_aggregate_labels_supports_no_time_and_empty_data(self):
        base = pd.DataFrame(columns=["Cliente", "Produto", "Quantidade", "Preco"])

        labels = aggregate_labels(base)

        self.assertTrue(labels.empty)
        self.assertEqual(labels.columns.tolist(), ["Cliente", "Produto", "Quantidade", "Preco"])

    def test_build_labels_html_escapes_user_content(self):
        html = build_labels_html(
            [
                {
                    "cliente": "<Ana>",
                    "referencia": 'REF "1"',
                    "quantidade": "2",
                    "preco_unit": "Preço: 7,00 €",
                }
            ]
        )

        self.assertIn("&lt;Ana&gt;", html)
        self.assertIn("REF &quot;1&quot;", html)
        self.assertNotIn("<Ana>", html)


if __name__ == "__main__":
    unittest.main()
