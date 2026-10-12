"""Exercise navigation against an isolated local session, without touching user data."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from organizer import db as database
from organizer import sessions_json


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


class HistoryTests(unittest.TestCase):
    def setUp(self):
        previous_cwd = os.getcwd()
        directory = tempfile.TemporaryDirectory(prefix="organizer-history-")
        self.addCleanup(directory.cleanup)
        os.chdir(directory.name)
        self.addCleanup(os.chdir, previous_cwd)
        environment = patch.dict(os.environ, {
            "ORGANIZER_USER": "history-test",
            "ORGANIZER_PASS": "local-test-password",
            "DATABASE_URL": f"sqlite:///{directory.name}/history.db",
        })
        environment.start()
        self.addCleanup(environment.stop)
        self.sessions_directory = Path(directory.name) / "saved/sessions"
        self.sessions_directory.mkdir(parents=True)
        sessions = patch.object(sessions_json, "SESSIONS_DIR", str(self.sessions_directory))
        sessions.start()
        self.addCleanup(sessions.stop)
        fixtures = [
            ("summer-session", "Direto de Verão", "2026-07-01T12:00:00+00:00"),
            ("winter-session", "Direto de Inverno", "2026-01-01T12:00:00+00:00"),
            ("undated-session", "Sem data", "invalid-date"),
        ]
        for session_id, label, created_at in fixtures:
            payload = {
                "id": session_id, "label": label, "created_at": created_at,
                "orders_rows": 1, "refs": 1,
                "orders": [{"Cliente": label, "Produto": "REF 1", "Quantidade": 2}],
                "price_overrides": {}, "meta": {},
            }
            (self.sessions_directory / f"{session_id}.json").write_text(json.dumps(payload), encoding="utf-8")

    def history_app(self):
        app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=20).run()
        app.text_input(key="auth_user").set_value("history-test")
        app.text_input(key="auth_pass").set_value("local-test-password")
        app.button[0].click().run()
        app.radio(key="nav_page").set_value("Histórico").run()
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])
        return app

    def test_history_searches_name_or_id_and_opens_matching_session(self):
        app = self.history_app()
        app.text_input(key="history_search").set_value("  INVERNO  ").run()
        self.assertEqual(len(app.selectbox(key="history_session_pick").options), 1)
        self.assertIn("Direto de Inverno", app.selectbox(key="history_session_pick").options[0])
        self.assertIn("01/01/2026 às 12:00", app.selectbox(key="history_session_pick").options[0])

        app.text_input(key="history_search").set_value("nenhuma sessão").run()
        self.assertFalse(any(button.label == "Abrir" for button in app.button))
        self.assertTrue(any("Não encontrei sessões" in item.value for item in app.info))

        app.text_input(key="history_search").set_value("summer-session").run()
        self.assertIn("Direto de Verão", app.selectbox(key="history_session_pick").options[0])
        self.assertIn("01/07/2026 às 13:00", app.selectbox(key="history_session_pick").options[0])
        next(button for button in app.button if button.label == "Abrir").click().run()
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])
        self.assertEqual(app.session_state["loaded_session"]["id"], "summer-session")
        self.assertEqual(app.session_state["nav_committed"], "Operação")

    def test_history_handles_missing_date_and_requires_new_delete_confirmation(self):
        app = self.history_app()
        app.text_input(key="history_search").set_value("undated-session").run()
        self.assertIn("Data indisponível", app.selectbox(key="history_session_pick").options[0])
        next(box for box in app.checkbox if box.label == "Confirmo que quero apagar").check().run()
        self.assertFalse(next(button for button in app.button if button.label == "Apagar definitivamente").disabled)

        app.text_input(key="history_search").set_value("winter-session").run()
        self.assertTrue(next(button for button in app.button if button.label == "Apagar definitivamente").disabled)
        next(box for box in app.checkbox if box.label == "Confirmo que quero apagar").check().run()
        next(button for button in app.button if button.label == "Apagar definitivamente").click().run()
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])
        self.assertFalse((self.sessions_directory / "winter-session.json").exists())
        self.assertTrue((self.sessions_directory / "undated-session.json").exists())
        self.assertTrue(any("Direto de Inverno» apagada" in item.value for item in app.success))

    def test_deleting_snapshot_removes_local_and_database_copies_only_for_its_id(self):
        engine = database.connect()
        self.addCleanup(engine.dispose)
        database.init_db(engine)
        summer_path = self.sessions_directory / "summer-session.json"
        winter_path = self.sessions_directory / "winter-session.json"
        summer = json.loads(summer_path.read_text(encoding="utf-8"))
        winter = json.loads(winter_path.read_text(encoding="utf-8"))
        winter["label"] = summer["label"]  # Different saved version of the same named direct.
        winter_path.write_text(json.dumps(winter), encoding="utf-8")
        database.save_session_payload(engine, summer)
        database.save_session_payload(engine, winter)

        app = self.history_app()
        app.text_input(key="history_search").set_value("summer-session").run()
        self.assertTrue(app.selectbox(key="history_session_pick").value["path"].startswith("db:"))
        self.assertTrue(any("Neste dispositivo" in item.value for item in app.markdown))
        next(box for box in app.checkbox if box.label == "Confirmo que quero apagar").check().run()
        next(button for button in app.button if button.label == "Apagar definitivamente").click().run()
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])
        self.assertFalse(summer_path.exists())
        self.assertIsNone(database.get_session_payload(engine, session_id="summer-session"))
        self.assertTrue(winter_path.exists())
        self.assertIsNotNone(database.get_session_payload(engine, session_id="winter-session"))
        self.assertTrue(any("Direto de Verão» apagada" in item.value for item in app.success))

    def test_partial_snapshot_deletion_reports_remaining_database_copy(self):
        engine = database.connect()
        self.addCleanup(engine.dispose)
        database.init_db(engine)
        session_path = self.sessions_directory / "summer-session.json"
        database.save_session_payload(engine, json.loads(session_path.read_text(encoding="utf-8")))
        app = self.history_app()
        app.text_input(key="history_search").set_value("summer-session").run()
        next(box for box in app.checkbox if box.label == "Confirmo que quero apagar").check().run()
        with patch.object(database, "delete_session_by_id", side_effect=RuntimeError("Unavailable test database")):
            next(button for button in app.button if button.label == "Apagar definitivamente").click().run()
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])
        self.assertFalse(session_path.exists())
        self.assertIsNotNone(database.get_session_payload(engine, session_id="summer-session"))
        self.assertTrue(any("Não foi possível apagar todas as cópias" in item.value for item in app.warning))
        self.assertFalse(any("apagada" in item.value for item in app.success))

    def test_same_named_snapshots_at_same_time_have_unique_options_and_open_correct_ids(self):
        for session_id, price in (("same-time-first", 3.0), ("same-time-second", 7.0)):
            payload = {
                "id": session_id, "label": "Direto repetido", "created_at": "2026-07-01T12:00:00+00:00",
                "orders_rows": 1, "refs": 1,
                "orders": [{"Cliente": "Ana", "Produto": "REF 1", "Quantidade": 2}],
                "price_overrides": {"ref1": price}, "meta": {},
            }
            (self.sessions_directory / f"{session_id}.json").write_text(json.dumps(payload), encoding="utf-8")
        app = self.history_app()
        for session_id, expected_price in (("same-time-first", 3.0), ("same-time-second", 7.0)):
            app.text_input(key="history_search").set_value("Direto repetido").run()
            selector = app.selectbox(key="history_session_pick")
            self.assertEqual(len(selector.options), 2)
            self.assertEqual(len(set(selector.options)), 2)
            selected = next(row for row in sessions_json.list_sessions() if row["id"] == session_id)
            selector.set_value(selected).run()
            app.run()  # A normal rerun must preserve the option's identity.
            self.assertEqual(app.selectbox(key="history_session_pick").value["id"], session_id)
            next(button for button in app.button if button.label == "Abrir").click().run()
            self.assertFalse(app.exception, [item.value for item in app.exception])
            self.assertFalse(app.error, [item.value for item in app.error])
            self.assertEqual(app.session_state["loaded_session"]["id"], session_id)
            self.assertEqual(app.session_state["price_overrides"], {"ref1": expected_price})
            app.radio(key="nav_page").set_value("Histórico").run()
