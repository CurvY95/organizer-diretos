import json
import unittest
from html.parser import HTMLParser

from organizer.utils import script_json


class _ScriptParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)


class ScriptJsonTests(unittest.TestCase):
    def test_untrusted_text_cannot_close_script_or_create_html_elements(self):
        malicious = '</script><img src=x onerror="window.pwned=true"><script>alert(1)</script>'
        encoded = script_json(malicious)
        document = _ScriptParser()
        document.feed(f"<script>const message = {encoded};</script>")

        self.assertEqual(document.tags, ["script"])
        self.assertNotIn("<", encoded)
        self.assertEqual(json.loads(encoded), malicious)

    def test_reserved_characters_and_unicode_roundtrip(self):
        value = {
            "message": 'Olá José <REF> & "aspas" \\\nlinha\u2028separador\u2029parágrafo',
            "url": "https://example.test/?name=Ana&ref=<REF>",
            "values": [None, True, False, 1.5],
        }
        encoded = script_json(value)

        for character in ("<", ">", "&", "\u2028", "\u2029"):
            self.assertNotIn(character, encoded)
        for escaped in ("\\u003c", "\\u003e", "\\u0026", "\\u2028", "\\u2029"):
            self.assertIn(escaped, encoded)
        self.assertEqual(json.loads(encoded), value)


if __name__ == "__main__":
    unittest.main()
