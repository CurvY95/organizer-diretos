import unittest

import pandas as pd

from organizer.core import (
    apply_price_overrides,
    build_message,
    build_summary,
    format_currency,
    parse_inputs,
    stable_orders_fingerprint,
)


class CoreTests(unittest.TestCase):
    def test_parse_inputs_accepts_aliases_and_decimal_commas(self):
        orders = pd.DataFrame(
            {
                "Nome": ["Ana", "Ana", "Bruno"],
                "Referência": [" REF 1 ", "REF 2", "ref 1"],
                "Qtd": ["2", "1,5", ""],
                "Mensagem": ["quero", "tambem", ""],
            }
        )
        prices = pd.DataFrame({"Referencia": ["ref 1", "ref 2"], "Preço": ["10,50", "2.5"]})

        parsed = parse_inputs(orders, prices, fill_missing_quantity_with=1)

        self.assertEqual(parsed.orders["Quantidade"].tolist(), [2.0, 1.5, 1.0])
        self.assertTrue(parsed.missing_price_keys.empty)
        totals = parsed.merged.groupby("Cliente")["TotalItem"].sum().to_dict()
        self.assertEqual(totals["Ana"], 24.75)
        self.assertEqual(totals["Bruno"], 10.5)

    def test_parse_inputs_reports_missing_prices(self):
        orders = pd.DataFrame({"Cliente": ["Ana"], "Produto": ["REF X"], "Quantidade": [1]})
        prices = pd.DataFrame({"Produto": ["REF 1"], "Preco": [2]})

        parsed = parse_inputs(orders, prices)

        self.assertEqual(parsed.missing_price_keys["Produto"].tolist(), ["REF X"])
        self.assertTrue(pd.isna(parsed.merged["Preco"].iloc[0]))

    def test_price_overrides_and_summary(self):
        orders = pd.DataFrame({"Cliente": ["Ana", "Ana"], "Produto": ["REF 1", "REF 2"], "Quantidade": [2, 1]})
        prices = pd.DataFrame({"Produto": ["REF 1"], "Preco": [3]})
        parsed = parse_inputs(orders, prices)
        overrides = pd.DataFrame({"ProdutoKey": ["ref 2"], "Preco": ["4,5"]})

        merged = apply_price_overrides(parsed.merged, overrides)
        by_client, details = build_summary(merged.dropna(subset=["Preco"]))

        self.assertEqual(float(by_client["Total"].iloc[0]), 10.5)
        self.assertEqual(details["Ana"].shape[0], 2)

    def test_message_uses_total_template_and_currency(self):
        details = pd.DataFrame(
            [{"Produto": "REF 1", "Quantidade": 2, "Preco": 3.5, "TotalItem": 7.0}]
        )

        msg = build_message(
            client="Ana",
            details=details,
            total=7,
            currency="EUR",
            intro="Olá",
            outro="Obrigado",
            total_line_template="A pagar: {total}",
        )

        self.assertIn("A pagar: 7,00 €", msg)
        self.assertNotIn("Total:", msg)

    def test_format_currency_and_fingerprint_are_stable(self):
        self.assertEqual(format_currency(1234.5, "EUR"), "1.234,50 €")
        a = pd.DataFrame({"Cliente": ["B", "A"], "Produto": ["X", "Y"], "Quantidade": [1, 2]})
        b = pd.DataFrame({"Cliente": ["A", "B"], "Produto": ["Y", "X"], "Quantidade": [2, 1]})
        self.assertEqual(stable_orders_fingerprint(a), stable_orders_fingerprint(b))


if __name__ == "__main__":
    unittest.main()
