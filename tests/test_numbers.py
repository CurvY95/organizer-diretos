import unittest

import pandas as pd

from organizer.core import coerce_number_series, normalize_quantities, parse_inputs


class NumberValidationTests(unittest.TestCase):
    def test_scientific_notation_and_localized_currency_preserve_value(self):
        values = pd.Series(["1e3", "1e-3", "1,5E+2", "1.234,56 €", "$1,234.56", "R$ 2,50", "EUR 1 234,50"])

        self.assertEqual(
            coerce_number_series(values).tolist(),
            [1000.0, 0.001, 150.0, 1234.56, 1234.56, 2.5, 1234.5],
        )

    def test_coercion_rejects_text_units_malformed_and_nonfinite_numbers(self):
        values = pd.Series(["banana", "2x", "1e", "1e3x", "1..2", "€1€2", "inf", float("inf"), "1e309"])

        self.assertTrue(coerce_number_series(values).isna().all())

    def test_quantity_default_only_fills_originally_empty_cells(self):
        values = pd.Series([None, float("nan"), pd.NA, "", " \u00a0 ", "2", "1e-3"], index=[9, 8, 7, 6, 5, 4, 3])

        normalized = normalize_quantities(values, fill_missing_with=1)

        self.assertEqual(normalized.tolist(), [1, 1, 1, 1, 1, 2, 0.001])
        self.assertEqual(normalized.index.tolist(), values.index.tolist())
        for invalid in ("banana", "2x", "nan", "inf", float("inf")):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "Quantidade"):
                    normalize_quantities(pd.Series(["", invalid]), fill_missing_with=1)

    def test_quantity_default_must_be_finite(self):
        for replacement in (float("inf"), float("nan"), "invalid"):
            with self.subTest(replacement=replacement):
                with self.assertRaises(ValueError):
                    normalize_quantities(pd.Series([""]), fill_missing_with=replacement)

    def test_parse_inputs_reports_invalid_quantity_with_default_enabled(self):
        orders = pd.DataFrame({"Cliente": ["Ana"], "Produto": ["REF1"], "Quantidade": ["banana"]})
        prices = pd.DataFrame({"Produto": ["REF1"], "Preco": [10]})

        with self.assertRaisesRegex(ValueError, "banana"):
            parse_inputs(orders, prices, fill_missing_quantity_with=1)

        orders["Quantidade"] = ["1e3"]
        parsed = parse_inputs(orders, prices, fill_missing_quantity_with=1)
        self.assertEqual(parsed.merged["TotalItem"].tolist(), [10000])


if __name__ == "__main__":
    unittest.main()
