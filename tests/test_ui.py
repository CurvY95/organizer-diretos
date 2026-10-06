"""Exercise navigation against an isolated local session, without touching user data."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from streamlit.testing.v1 import AppTest


ROOT = Path(__file__).resolve().parents[1]


class NavigationTests(unittest.TestCase):
    def test_guided_workflow_preserves_pending_edits(self):
        previous_cwd = os.getcwd()
        with tempfile.TemporaryDirectory(prefix="organizer-ui-") as directory:
            try:
                os.chdir(directory)
                with patch.dict(os.environ, {
                    "ORGANIZER_USER": "ui-test",
                    "ORGANIZER_PASS": "local-test-password",
                    "DATABASE_URL": "sqlite:///:memory:",
                }):
                    app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=20).run()
                    app.text_input(key="auth_user").set_value("ui-test")
                    app.text_input(key="auth_pass").set_value("local-test-password")
                    app.button[0].click().run()
                    self.assertFalse(app.exception)
                    self.assertFalse(app.error)

                    app.button(key="step_Preços").click().run()
                    self.assertEqual(app.session_state["nav_committed"], "Preços")
                    self.assertTrue(any("importa primeiro" in item.value for item in app.info))
                    app.session_state["uploaded_input_cache"] = {
                        "name": "example.csv",
                        "bytes": (ROOT / "examples/encomendas_comments.csv").read_bytes(),
                    }
                    app.run()
                    self.assertFalse(app.exception)
                    self.assertFalse(app.error)
                    self.assertIn("Linhas com preço", [metric.label for metric in app.metric])
                    app.button(key="step_Operação").click().run()
                    self.assertEqual(app.tabs[0].label, "Rever encomendas")

                    app.session_state["pending_comment_clear_clients"] = ["Ana"]
                    app.button(key="step_Mensagens").click().run()
                    self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
                    self.assertEqual(app.session_state["nav_committed"], "Operação")
                    next(b for b in app.button if b.label == "Continuar a editar").click().run()
                    self.assertFalse(app.session_state["show_unsaved_nav_dialog"])
                    self.assertEqual(app.session_state["pending_comment_clear_clients"], ["Ana"])

                    app.button(key="step_Mensagens").click().run()
                    next(b for b in app.button if b.label == "Sair sem guardar").click().run()
                    self.assertFalse(app.exception)
                    self.assertFalse(app.error)
                    self.assertEqual(app.session_state["nav_committed"], "Mensagens")
                    self.assertFalse(app.session_state["pending_comment_clear_clients"])
                    app.button(key="step_Etiquetas").click().run()
                    self.assertFalse(app.exception)
                    self.assertFalse(app.error)
                    self.assertEqual(app.session_state["nav_committed"], "Etiquetas")
            finally:
                os.chdir(previous_cwd)
