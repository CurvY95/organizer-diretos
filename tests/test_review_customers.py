"""Regressions for customer identity, saved corrections and browser components."""

import io
import json
import re
import unittest
from unittest.mock import patch

import pandas as pd
import streamlit as st

from organizer import sessions_json
from tests import test_workflow as workflow


class CustomerReviewTests(unittest.TestCase):
    # Reuse the disposable application environment without inheriting the
    # workflow test methods and running the entire original suite a second time.
    setUp = workflow.WorkflowTests.setUp
    assert_app_ok = workflow.WorkflowTests.assert_app_ok
    login = workflow.WorkflowTests.login
    click = workflow.WorkflowTests.click
    edit_cells = workflow.WorkflowTests.edit_cells
    export_bytes = staticmethod(workflow.WorkflowTests.export_bytes)

    def import_frame(self, app, frame):
        app.session_state["uploaded_input_cache"] = {
            "name": "customer-review.csv", "bytes": frame.to_csv(index=False).encode("utf-8"),
        }
        app.run()
        self.assert_app_ok(app)

    def price_all(self, app, price=10.0):
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        rows = {index: {"Preco": price} for index in app.session_state["price_draft"].index}
        self.edit_cells(app, "all_prices_editor", rows)
        self.click(app, "Guardar preços")

    @staticmethod
    def message(app):
        return next(area.value for area in app.text_area if area.label == "Mensagem deste cliente")

    @staticmethod
    def chat_url(links):
        calls = [call for call in links.call_args_list if call.args and call.args[0] == "ABRIR CHAT"]
        if not calls:
            raise AssertionError("A customer chat link was not rendered")
        return calls[-1].args[1]

    def test_homonyms_keep_messages_links_labels_and_comment_removals_separate(self):
        app = self.login()
        self.import_frame(app, pd.DataFrame({
            "Cliente": ["Ana", "Ana"], "UserId": ["11111", "33333"],
            "Produto": ["REF 1", "REF 1"], "Quantidade": [1, 2],
            "Comentario": ["comentário primeira Ana", "comentário segunda Ana"],
            "Hora": ["2:00", "5:00"],
        }))
        self.price_all(app)
        with patch.object(st, "link_button", wraps=st.link_button) as links:
            app.button(key="step_Mensagens").click().run()
            self.assert_app_ok(app)
            self.assertEqual(app.selectbox(key="messages_client_pick").options, ["Ana · ID 11111", "Ana · ID 33333"])
            self.assertIn("Ana:\n", self.message(app))
            self.assertIn("10,00 €", self.message(app))
            self.assertNotIn("20,00 €", self.message(app))
            self.assertNotIn("ID 11111", self.message(app))
            self.assertIn("selected_item_id=11111", self.chat_url(links))

            app.selectbox(key="messages_client_pick").set_value("Ana · ID 33333").run()
            self.assert_app_ok(app)
            self.assertIn("20,00 €", self.message(app))
            self.assertNotIn("30,00 €", self.message(app))
            self.assertIn("selected_item_id=33333", self.chat_url(links))

        with patch.object(st, "download_button", wraps=st.download_button) as downloads:
            app.button(key="step_Etiquetas").click().run()
            app.radio(key="labels_pick_mode").set_value("Todas").run()
            self.assert_app_ok(app)
            labels = self.export_bytes(downloads, "Descarregar etiquetas (HTML)").decode("utf-8")
            self.assertEqual(labels.count('<div class="label">'), 2)
            self.assertIn("Ana · ID 11111", labels)
            self.assertIn("Ana · ID 33333", labels)
            self.assertIn("REF 1 — 1 / m", labels)
            self.assertIn("REF 1 — 2 / m", labels)

        app.button(key="step_Operação").click().run()
        app.selectbox(key="comments_client_pick").set_value("Ana · ID 11111").run()
        app.button(key="mark_remove_comments_client").click().run()
        self.assertEqual(app.session_state["pending_comment_clear_clients"], ["user:11111"])
        app.button(key="apply_remove_comments_list").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["orders_draft"]["Comentário"].tolist(), ["", "comentário segunda Ana"])

    def test_corrected_source_ids_survive_reruns_new_import_and_saved_session(self):
        orders = pd.DataFrame({
            "Cliente": ["Ana"], "UserId": ["11111"], "ProfileId": ["ana.source"],
            "Produto": ["REF 1"], "Quantidade": [1],
        })
        app = self.login()
        self.import_frame(app, orders)
        self.price_all(app)
        app.button(key="step_Mensagens").click().run()
        app.text_input(key="dir_user_Ana").set_value("99999")
        app.text_input(key="dir_profile_Ana").set_value("ana.corrected")
        with patch.object(st, "link_button", wraps=st.link_button) as links:
            self.click(app, "Aplicar (guardar)")
            self.assertIn("selected_item_id=99999", self.chat_url(links))
            app.run()
            self.assert_app_ok(app)
            self.assertIn("selected_item_id=99999", self.chat_url(links))
        self.assertEqual(app.session_state["customer_id_overrides"]["user:11111"], {
            "user_id": "99999", "profile_id": "ana.corrected",
        })
        self.click(app, "Guardar sessão")
        snapshot = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(snapshot["meta"]["customer_id_overrides"]["user:11111"]["user_id"], "99999")

        imported = self.login()
        self.import_frame(imported, orders)
        self.price_all(imported)
        with patch.object(st, "link_button", wraps=st.link_button) as links:
            imported.button(key="step_Mensagens").click().run()
            self.assert_app_ok(imported)
            self.assertIn("selected_item_id=99999", self.chat_url(links))

        reopened = self.login()
        reopened.radio(key="nav_page").set_value("Histórico").run()
        self.click(reopened, "Abrir")
        with patch.object(st, "link_button", wraps=st.link_button) as links:
            reopened.button(key="step_Mensagens").click().run()
            self.assert_app_ok(reopened)
            self.assertIn("selected_item_id=99999", self.chat_url(links))
            self.assertEqual(reopened.text_input(key="dir_profile_Ana").value, "ana.corrected")

    def test_messages_summary_download_contains_all_customer_totals(self):
        app = self.login()
        self.import_frame(app, pd.DataFrame({
            "Cliente": ["Ana", "Bruno"], "Produto": ["REF 1", "REF 1"], "Quantidade": [1, 2],
        }))
        self.price_all(app)
        with patch.object(st, "download_button", wraps=st.download_button) as downloads:
            app.button(key="step_Mensagens").click().run()
            self.assert_app_ok(app)
            content = self.export_bytes(downloads, "Descarregar resumo (.csv)")

        self.assertTrue(content)
        summary = pd.read_csv(io.BytesIO(content))
        self.assertEqual(summary.set_index("Cliente")["Total"].to_dict(), {"Ana": 10.0, "Bruno": 20.0})
        self.assertEqual(summary.set_index("Cliente")["QuantidadeTotal"].to_dict(), {"Ana": 1.0, "Bruno": 2.0})

    def test_imported_names_and_references_are_safe_in_actual_copy_components(self):
        name = 'Ana</script><script>window.customerPayload=true</script>'
        reference = 'REF</script><img src=x onerror="window.referencePayload=true">'
        app = self.login()
        self.import_frame(app, pd.DataFrame({
            "Cliente": [name], "UserId": ["11111"], "Produto": [reference], "Quantidade": [1],
        }))
        self.price_all(app)
        with patch.object(st.components.v1, "html", wraps=st.components.v1.html) as components:
            app.button(key="step_Mensagens").click().run()
            self.assert_app_ok(app)
            app.button(key="step_Operação").click().run()
            self.assert_app_ok(app)

        scripts = [call.args[0] for call in components.call_args_list
                   if call.args and "const text = " in call.args[0]]
        self.assertGreaterEqual(len(scripts), 4)
        for source in scripts:
            self.assertNotIn(name, source)
            self.assertNotIn(reference, source)
            self.assertIn("\\u003c/script\\u003e", source)
            match = re.search(r"const text = (.+);", source)
            self.assertIsNotNone(match)
            copied = json.loads(match.group(1))
            self.assertIn(name, copied)
            self.assertIn(reference, copied)


if __name__ == "__main__":
    unittest.main()
