import unittest

from organizer.labels import build_labels_html, parse_hora_to_seconds


class LabelsTests(unittest.TestCase):
    def test_parse_hora_to_seconds(self):
        self.assertEqual(parse_hora_to_seconds("1:05"), 65)
        self.assertEqual(parse_hora_to_seconds("2:03:04"), 7384)
        self.assertEqual(parse_hora_to_seconds(" 10 : 02 "), 602)
        self.assertIsNone(parse_hora_to_seconds(""))
        self.assertIsNone(parse_hora_to_seconds("abc"))

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
