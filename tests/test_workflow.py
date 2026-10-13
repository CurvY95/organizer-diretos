"""Run real pages and exports with disposable files and a separate SQLite database."""

import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import streamlit as st
from streamlit.testing.v1 import AppTest
from streamlit.testing.v1.element_tree import ElementTree

from organizer import sessions_json
from organizer.core import normalize_produto_key


ROOT = Path(__file__).resolve().parents[1]
PRICES = {normalize_produto_key("REF 1"): 3.5, normalize_produto_key("REF 2"): 4.0}


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.previous_cwd = os.getcwd()
        self.directory = tempfile.TemporaryDirectory(prefix="organizer-workflow-")
        self.addCleanup(self.directory.cleanup)
        os.chdir(self.directory.name)
        self.addCleanup(os.chdir, self.previous_cwd)
        env = patch.dict(os.environ, {
            "ORGANIZER_USER": "workflow-test",
            "ORGANIZER_PASS": "local-test-password",
            "DATABASE_URL": f"sqlite:///{self.directory.name}/organizer.db",
        })
        env.start()
        self.addCleanup(env.stop)
        # This module may already be imported by another test before cwd changes.
        sessions = patch.object(sessions_json, "SESSIONS_DIR", str(Path(self.directory.name) / "saved/sessions"))
        sessions.start()
        self.addCleanup(sessions.stop)
        serialize = ElementTree.get_widget_states

        def include_editor_states(tree):
            # AppTest represents editors as UnknownElement and omits them from
            # widget serialization. Preserve their real string_value payload,
            # as data_editor.DataEditorSerde does for browser interactions.
            states = serialize(tree)
            included = {widget.id for widget in states.widgets}
            for node in tree.get("dataframe"):
                if node.key in {"comments_editor", "all_prices_editor", "labels_editor"}:
                    if node.proto.id not in included and node.key in tree.session_state:
                        widget = states.widgets.add()
                        widget.id = node.proto.id
                        widget.string_value = json.dumps(tree.session_state[node.key])
            return states

        editors = patch.object(ElementTree, "get_widget_states", include_editor_states)
        editors.start()
        self.addCleanup(editors.stop)

    def assert_app_ok(self, app):
        self.assertFalse(app.exception, [item.value for item in app.exception])
        self.assertFalse(app.error, [item.value for item in app.error])

    def login(self):
        app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=20).run()
        app.text_input(key="auth_user").set_value("workflow-test")
        app.text_input(key="auth_pass").set_value("local-test-password")
        app.button[0].click().run()
        self.assert_app_ok(app)
        return app

    def import_orders(self, app):
        app.session_state["uploaded_input_cache"] = {
            "name": "encomendas_comments.csv",
            "bytes": (ROOT / "examples/encomendas_comments.csv").read_bytes(),
        }
        app.run()
        self.assert_app_ok(app)

    def click(self, app, label):
        next(button for button in app.button if button.label == label).click().run()
        self.assert_app_ok(app)

    def edit_cells(self, app, key, rows):
        # AppTest has no data_editor interaction API. Feed its actual widget
        # patch format and rerun, as committing a cell does in the browser.
        app.session_state[key] = {"edited_rows": rows, "added_rows": [], "deleted_rows": []}
        app.run()
        self.assert_app_ok(app)

    def save_prices(self, app):
        app.button(key="step_Preços").click().run()
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        rows = {
            index: {"Preco": PRICES[row["ProdutoKey"]]}
            for index, row in app.session_state["price_draft"].iterrows()
        }
        self.edit_cells(app, "all_prices_editor", rows)
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], PRICES)

    @staticmethod
    def export_bytes(downloads, label):
        calls = [call for call in downloads.call_args_list if call.args and call.args[0] == label]
        if not calls:
            raise AssertionError(f"Export not rendered: {label}")
        return calls[-1].kwargs["data"]

    def test_clean_import_can_advance_without_false_unsaved_warning(self):
        app = self.login()
        self.import_orders(app)
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))

    def test_filtering_orders_keeps_edits_for_hidden_clients_when_applying_and_saving(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Operação").click().run()
        app.text_input(key="orders_search").set_value("Ana").run()
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}})

        app.text_input(key="orders_search").set_value("Bruno").run()
        # The sole visible row is at position zero, but belongs to original row two.
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 3.0}})
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [4.0, 1.5, 3.0])
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])

        app.text_input(key="orders_search").set_value("cliente inexistente").run()
        self.assertTrue(any("Nenhuma encomenda corresponde" in item.value for item in app.info))
        self.click(app, "Aplicar alterações")
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [4.0, 1.5, 3.0])
        app.button(key="orders_clear_filters").click().run()
        self.assertEqual(app.text_input(key="orders_search").value, "")
        app.text_input(key="orders_search").set_value("Ana").run()
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(len(saved["orders"]), 3)
        self.assertEqual([row["Cliente"] for row in saved["orders"]], ["Ana Silva", "Ana Silva", "Bruno Costa"])
        self.assertEqual([float(row["Quantidade"]) for row in saved["orders"]], [4.0, 1.5, 3.0])
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))
        self.assertIn("20,00 €", next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"))
        app.selectbox(key="messages_client_pick").set_value("Bruno Costa").run()
        self.assertIn("10,50 €", next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"))

    def test_reset_and_discard_from_filtered_orders_restore_all_rows_and_keep_navigation_guard(self):
        app = self.login()
        self.import_orders(app)
        app.text_input(key="orders_search").set_value("Bruno").run()
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 5.0}})
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 5.0])
        self.click(app, "Repor (voltar ao guardado)")
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])

        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 6.0}})
        app.button(key="step_Preços").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.assertEqual(app.session_state["nav_committed"], "Operação")
        self.click(app, "Continuar a editar")
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 6.0])
        app.button(key="step_Preços").click().run()
        self.click(app, "Sair sem guardar")
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])
        app.button(key="step_Operação").click().run()
        self.assert_app_ok(app)
        app.text_input(key="orders_search").set_value("").run()
        self.assertEqual(len(app.session_state["orders_draft"]), 3)
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])

    def test_filtered_missing_prices_preserve_hidden_saved_prices_and_allow_clearing_values(self):
        app = self.login()
        self.import_orders(app)
        app.button(key="step_Preços").click().run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 3.5}})
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], {normalize_produto_key("REF 1"): 3.5})

        app.toggle(key="prices_missing_only").set_value(True).run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 4.0}})
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], PRICES)
        self.assertEqual(len(app.session_state["price_draft"]), 2)

        app.toggle(key="prices_missing_only").set_value(False).run()
        app.text_input(key="prices_search").set_value("REF 2").run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": None}})
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], {normalize_produto_key("REF 1"): 3.5})
        app.toggle(key="prices_missing_only").set_value(True).run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 4.0}})
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(saved["price_overrides"], PRICES)
        self.assertEqual(len(saved["orders"]), 3)
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))
        self.assertIn("13,00 €", next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"))

    def test_excluded_rows_can_be_restored_and_reopening_session_clears_editor_filters(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {2: {"Incluir": False}})
        self.click(app, "Aplicar alterações")
        app.selectbox(key="orders_scope").set_value("Excluídas").run()
        app.text_input(key="orders_search").set_value("Bruno").run()
        self.edit_cells(app, "comments_editor", {0: {"Incluir": True, "Quantidade": 7.0}})
        self.click(app, "Aplicar alterações")
        self.assertEqual(app.session_state["orders_draft"]["Incluir"].tolist(), [True, True, True])
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [2.0, 1.5, 7.0])
        self.assertTrue(any("Nenhuma encomenda corresponde" in item.value for item in app.info))
        app.button(key="orders_clear_filters").click().run()
        self.assertEqual(app.text_input(key="orders_search").value, "")
        self.assertEqual(app.selectbox(key="orders_scope").value, "Todas")
        self.click(app, "Guardar sessão")

        app.button(key="step_Preços").click().run()
        app.text_input(key="prices_search").set_value("REF 2").run()
        app.toggle(key="prices_missing_only").set_value(True).run()
        app.button(key="prices_clear_filters").click().run()
        self.assertEqual(app.text_input(key="prices_search").value, "")
        self.assertFalse(app.toggle(key="prices_missing_only").value)
        app.radio(key="nav_page").set_value("Histórico").run()
        # Simulate filters retained from the previous workspace: opening a saved
        # direct must start with all of its rows and references visible.
        app.session_state["orders_search"] = "Ana"
        app.session_state["orders_scope"] = "Excluídas"
        app.session_state["prices_search"] = "REF 2"
        app.session_state["prices_missing_only"] = True
        self.click(app, "Abrir")
        self.assertEqual(app.text_input(key="orders_search").value, "")
        self.assertEqual(app.selectbox(key="orders_scope").value, "Todas")
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [2.0, 1.5, 7.0])
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.text_input(key="prices_search").value, "")
        self.assertFalse(app.toggle(key="prices_missing_only").value)

    def test_save_and_continue_commits_pending_prices(self):
        app = self.login()
        self.import_orders(app)
        app.button(key="step_Preços").click().run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 3.5}, 1: {"Preco": 4.0}})
        app.button(key="step_Mensagens").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.click(app, "Guardar e continuar")
        self.assertEqual(app.session_state["nav_committed"], "Mensagens")
        self.assertEqual(app.session_state["price_overrides"], PRICES)
        message = next(area.value for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertIn("13,00 €", message)

    def test_saved_manual_prices_remain_in_editor_after_leaving_and_returning(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [3.5, 4.0])
        self.assertEqual(app.session_state["price_overrides"], PRICES)
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))

    def test_manual_message_edits_do_not_leak_between_clients(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.selectbox(key="messages_client_pick").value, "Ana Silva")
        self.assertTrue(app.button(key="messages_previous_client").disabled)
        app.checkbox(key="messages_allow_edit").check().run()
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        message.set_value("Mensagem ajustada só para Ana").run()

        app.button(key="messages_next_client").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.selectbox(key="messages_client_pick").value, "Bruno Costa")
        self.assertTrue(app.button(key="messages_next_client").disabled)
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertIn("Bruno Costa", message.value)
        self.assertIn("3,50 €", message.value)
        self.assertNotIn("Mensagem ajustada só para Ana", message.value)
        message.set_value("Mensagem ajustada só para Bruno").run()

        app.button(key="messages_previous_client").click().run()
        self.assert_app_ok(app)
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertEqual(message.value, "Mensagem ajustada só para Ana")
        app.button(key="messages_next_client").click().run()
        self.assert_app_ok(app)
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertEqual(message.value, "Mensagem ajustada só para Bruno")

    def test_manual_message_refreshes_after_price_change_and_copies_intentional_empty_text(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Mensagens").click().run()
        app.checkbox(key="messages_allow_edit").check().run()
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        original_key = message.key
        message.set_value("Texto com o total antigo").run()

        # Recalculate the same client with a newly committed price while the
        # manual editor is visible. Its previous text must not hide the new total.
        app.session_state["price_overrides"] = {**PRICES, normalize_produto_key("REF 1"): 6.0}
        app.run()
        self.assert_app_ok(app)
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertNotEqual(message.key, original_key)
        self.assertIn("18,00 €", message.value)
        self.assertNotIn("Texto com o total antigo", message.value)

        with patch.object(st.components.v1, "html", wraps=st.components.v1.html) as components:
            message.set_value("").run()
            self.assert_app_ok(app)
            self.assertEqual(next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"), "")
            copy_scripts = [
                call.args[0] for call in components.call_args_list
                if call.args and 'id="copy_msgtab_' in call.args[0]
            ]
            self.assertEqual(len(copy_scripts), 1)
            self.assertIn('const text = "";', copy_scripts[0])

    def test_showing_order_details_preserves_unapplied_edits_and_navigation_guard(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}})
        app.toggle(key="orders_show_details").set_value(True).run()
        self.assert_app_ok(app)
        self.assertEqual(float(app.session_state["orders_working"].loc[0, "Quantidade"]), 4.0)
        self.assertEqual(float(app.session_state["orders_draft"].loc[0, "Quantidade"]), 2.0)
        app.toggle(key="orders_show_details").set_value(False).run()
        self.assert_app_ok(app)
        self.assertEqual(float(app.session_state["orders_working"].loc[0, "Quantidade"]), 4.0)
        self.assertEqual(float(app.session_state["orders_draft"].loc[0, "Quantidade"]), 2.0)

        app.button(key="step_Preços").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.assertEqual(app.session_state["nav_committed"], "Operação")
        self.click(app, "Continuar a editar")
        self.click(app, "Aplicar alterações")
        self.assertEqual(float(app.session_state["orders_draft"].loc[0, "Quantidade"]), 4.0)
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))

    def test_label_download_is_disabled_until_at_least_one_label_is_selected(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        with patch.object(st, "download_button", wraps=st.download_button) as downloads:
            app.button(key="step_Etiquetas").click().run()
            self.assert_app_ok(app)
            label_calls = [call for call in downloads.call_args_list if call.args and call.args[0] == "Descarregar etiquetas (HTML)"]
            self.assertTrue(label_calls[-1].kwargs["disabled"])
            self.assertIn("0 de 3 etiquetas selecionadas", " ".join(item.value for item in app.caption))

            app.radio(key="labels_pick_mode").set_value("Todas").run()
            self.assert_app_ok(app)
            label_calls = [call for call in downloads.call_args_list if call.args and call.args[0] == "Descarregar etiquetas (HTML)"]
            self.assertFalse(label_calls[-1].kwargs["disabled"])
            self.assertEqual(label_calls[-1].kwargs["data"].decode("utf-8").count('<div class="label">'), 3)

    def test_excel_prices_can_fill_an_existing_draft_and_be_saved(self):
        workbook = io.BytesIO()
        orders = pd.read_csv(ROOT / "examples/encomendas_comments.csv", dtype=str).fillna("")
        prices = pd.read_csv(ROOT / "examples/precos.csv", dtype=str)
        expected_prices = {normalize_produto_key("REF 1"): 10.5, normalize_produto_key("REF 2"): 2.5}
        with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
            orders.to_excel(writer, sheet_name="Comments", index=False)
            prices.to_excel(writer, sheet_name="Preços", index=False)
        app = self.login()
        app.session_state["uploaded_input_cache"] = {"name": "direto.xlsx", "bytes": workbook.getvalue()}
        app.run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_overrides"], {})
        self.assertEqual(next(metric.value for metric in app.metric if metric.label == "Linhas com preço"), "0 / 3")
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertIsInstance(app.session_state["price_draft"], pd.DataFrame)
        app.button(key="excel_prices_to_draft").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [10.5, 2.5])
        self.assertEqual(app.session_state["price_overrides"], {})
        self.assertEqual(next(metric.value for metric in app.metric if metric.label == "Linhas com preço"), "0 / 3")
        app.button(key="excel_prices_to_save").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_overrides"], expected_prices)
        self.assertEqual(next(metric.value for metric in app.metric if metric.label == "Linhas com preço"), "3 / 3")
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        message = next(area.value for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertIn("24,75 €", message)

    def test_save_and_continue_updates_messages_and_label_exports(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Operação").click().run()
        self.assert_app_ok(app)

        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 3.0}})
        app.session_state["pending_comment_clear_clients"] = ["Ana Silva"]
        app.button(key="step_Mensagens").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.assertEqual(app.session_state["nav_committed"], "Operação")

        with patch.object(st, "download_button", wraps=st.download_button) as downloads:
            self.click(app, "Guardar e continuar")
            self.assertEqual(app.session_state["nav_committed"], "Mensagens")
            committed = app.session_state["orders_draft"]
            self.assertEqual(float(committed.loc[0, "Quantidade"]), 3.0)
            self.assertEqual(committed.loc[committed["Cliente"] == "Ana Silva", "Comentário"].tolist(), ["", ""])
            self.assertEqual(app.session_state["pending_comment_clear_clients"], [])

            message = next(area.value for area in app.text_area if area.label == "Mensagem deste cliente")
            self.assertIn("Ana Silva", message)
            self.assertIn("16,50 €", message)
            text = self.export_bytes(downloads, "Descarregar texto (.txt)").decode("utf-8")
            self.assertIn("Ana Silva", text)
            self.assertIn("Bruno Costa", text)
            self.assertIn("16,50 €", text)
            self.assertIn("3,50 €", text)

            app.button(key="step_Etiquetas").click().run()
            self.assert_app_ok(app)
            app.radio(key="labels_pick_mode").set_value("Todas").run()
            self.assert_app_ok(app)
            html = self.export_bytes(downloads, "Descarregar etiquetas (HTML)").decode("utf-8")
            self.assertEqual(html.count('<div class="label">'), 3)
            self.assertIn("REF 1 — 3 / m", html)
            self.assertIn("Preço: 3,50 €", html)
            self.assertIn("Preço: 4,00 €", html)
            self.assertLess(html.index("Ana Silva"), html.index("Bruno Costa"))
            self.assertIn("size: 100mm 150mm", html)

    def test_saved_session_reopens_orders_prices_ids_and_label_order(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Operação").click().run()
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 3.0}})
        self.click(app, "Aplicar alterações")
        next(item for item in app.text_input if item.label == "Nome da sessão").set_value("Direto de teste")
        self.click(app, "Guardar sessão")
        sessions = sessions_json.list_sessions()
        self.assertEqual(len(sessions), 1)
        saved = sessions_json.load_session(sessions[0]["path"])
        self.assertEqual(saved["price_overrides"], PRICES)
        self.assertEqual(float(saved["orders"][0]["Quantidade"]), 3.0)
        self.assertEqual(saved["orders"][0]["Hora"], "1:05")
        self.assertEqual(saved["orders"][0]["UserId"], "100000000001")
        self.assertEqual(saved["orders"][0]["ProfileId"], "ana.silva")

        reopened = self.login()
        reopened.radio(key="nav_page").set_value("Histórico").run()
        self.click(reopened, "Abrir")
        reopened.button(key="step_Operação").click().run()
        self.assert_app_ok(reopened)
        self.assertEqual(reopened.session_state["price_overrides"], PRICES)
        orders = reopened.session_state["orders_draft"]
        self.assertEqual(float(orders.loc[0, "Quantidade"]), 3.0)
        self.assertEqual(orders["Hora"].tolist(), ["1:05", "2:10", "10:02"])
        reopened.button(key="step_Etiquetas").click().run()
        self.assert_app_ok(reopened)
        self.assertIn("Hora (↑)", reopened.selectbox(key="labels_order").options)

    def test_save_session_includes_unapplied_orders_and_pending_comment_removals(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}, 2: {"Incluir": False}})
        app.session_state["pending_comment_clear_clients"] = ["Ana Silva"]
        app.text_input(key="session_label").set_value("Direto com rascunho")
        self.click(app, "Guardar sessão")
        saved_rows = sessions_json.list_sessions()
        self.assertEqual(len(saved_rows), 1)
        saved = sessions_json.load_session(saved_rows[0]["path"])
        self.assertEqual(saved["label"], "Direto com rascunho")
        self.assertEqual(len(saved["orders"]), 3)
        self.assertEqual([row["Incluir"] for row in saved["orders"]], [True, True, False])
        self.assertEqual(float(saved["orders"][0]["Quantidade"]), 4.0)
        self.assertEqual([row["Comentario"] for row in saved["orders"][:2]], ["", ""])
        self.assertEqual(app.session_state["pending_comment_clear_clients"], [])
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))

    def test_save_session_from_prices_applies_price_draft_and_is_available_in_every_step(self):
        app = self.login()
        self.import_orders(app)
        app.button(key="step_Preços").click().run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 3.5}, 1: {"Preco": 4.0}})
        app.text_input(key="session_label").set_value("Preços por guardar")
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(saved["price_overrides"], PRICES)
        self.assertEqual(app.session_state["price_overrides"], PRICES)
        for destination in ("Mensagens", "Etiquetas", "Operação"):
            app.button(key=f"step_{destination}").click().run()
            self.assert_app_ok(app)
            self.assertEqual(app.session_state["nav_committed"], destination)
            self.assertEqual(app.button(key="save_workspace_session").label, "Guardar sessão")

    def test_saved_session_restores_preferences_and_personalized_message(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.session_state["currency"] = "BRL"
        app.session_state["intro"] = "Olá do direto de domingo"
        app.session_state["outro"] = "Até à próxima"
        app.session_state["total_line_template"] = "Valor: {total}"
        app.button(key="step_Mensagens").click().run()
        app.checkbox(key="messages_allow_edit").check().run()
        message = next(area for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertIn("R$", message.value)
        message.set_value("Ana, combinamos a entrega amanhã.").run()
        self.click(app, "Guardar sessão")

        reopened = self.login()
        reopened.radio(key="nav_page").set_value("Histórico").run()
        self.click(reopened, "Abrir")
        self.assertEqual(reopened.session_state["currency"], "BRL")
        self.assertEqual(reopened.session_state["intro"], "Olá do direto de domingo")
        self.assertEqual(reopened.session_state["total_line_template"], "Valor: {total}")
        self.assertEqual(reopened.session_state["outro"], "Até à próxima")
        reopened.button(key="step_Mensagens").click().run()
        reopened.checkbox(key="messages_allow_edit").check().run()
        self.assert_app_ok(reopened)
        self.assertEqual(next(area.value for area in reopened.text_area if area.label == "Mensagem deste cliente"),
                         "Ana, combinamos a entrega amanhã.")

    def test_removing_comments_keeps_other_pending_edits_and_does_not_resurrect_comments(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}})
        app.selectbox(key="comments_client_pick").set_value("Ana Silva").run()
        app.button(key="mark_remove_comments_client").click().run()
        self.assertFalse(app.button(key="apply_remove_comments_list").disabled)
        app.button(key="apply_remove_comments_list").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["orders_working"].loc[0, "Quantidade"], 4.0)
        self.assertEqual(app.session_state["orders_draft"].loc[0, "Quantidade"], 2.0)
        self.assertEqual(app.session_state["orders_draft"].loc[0, "Comentário"], "")
        self.assertEqual(app.session_state["orders_working"].loc[0, "Comentário"], "")
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(saved["orders"][0]["Comentario"], "")
        self.assertEqual(float(saved["orders"][0]["Quantidade"]), 4.0)

    def test_failed_session_save_shows_error_and_keeps_working_data(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}})
        blocked_path = Path(self.directory.name) / "not-a-directory"
        blocked_path.write_text("fixture", encoding="utf-8")
        with patch.object(sessions_json, "SESSIONS_DIR", str(blocked_path)):
            app.button(key="save_workspace_session").click().run()
        self.assertFalse(app.exception)
        self.assertTrue(any("Não consegui guardar a sessão" in error.value for error in app.error))
        self.assertEqual(float(app.session_state["orders_working"].loc[0, "Quantidade"]), 4.0)
        self.assertEqual(sessions_json.list_sessions(), [])
        self.assertNotIn("session_save_feedback", app.session_state)

    def test_session_save_does_not_create_an_unopenable_empty_snapshot(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {index: {"Incluir": False} for index in range(3)})
        app.button(key="save_workspace_session").click().run()
        self.assertFalse(app.exception)
        self.assertTrue(any("Não há encomendas incluídas" in error.value for error in app.error))
        self.assertEqual(sessions_json.list_sessions(), [])
        self.assertEqual(len(app.session_state["orders_working"]), 3)

    def test_opening_session_replaces_previous_orders_and_prices_on_any_page(self):
        sessions_json.save_session(
            label="Sessão para recuperar",
            orders_for_calc=pd.DataFrame({"Cliente": ["Cliente recuperado"], "Produto": ["REF 1"], "Quantidade": [2]}),
            price_overrides={normalize_produto_key("REF 1"): 9.0},
            meta={},
        )
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.radio(key="nav_page").set_value("Histórico").run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Histórico")
        self.click(app, "Abrir")
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_overrides"], {normalize_produto_key("REF 1"): 9.0})
        message = next(area.value for area in app.text_area if area.label == "Mensagem deste cliente")
        self.assertIn("Cliente recuperado", message)
        self.assertIn("18,00 €", message)
        self.assertNotIn("Ana Silva", message)
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [9.0])

    def test_consecutive_cell_edits_survive_reruns_and_reset_or_discard_cleanly(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 3.0}})
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [3.0, 1.5, 1.0])
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 3.0}, 1: {"Quantidade": 2.0}})
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [3.0, 2.0, 1.0])
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])
        self.click(app, "Repor (voltar ao guardado)")
        app.run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])

        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 4.0}})
        app.button(key="step_Preços").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.click(app, "Sair sem guardar")
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertEqual(app.session_state["orders_draft"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 3.5}})
        self.assertEqual(float(app.session_state["price_draft"].loc[0, "Preco"]), 3.5)
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 3.5}, 1: {"Preco": 4.0}})
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [3.5, 4.0])
        self.assertEqual(app.session_state["price_overrides"], {})
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], PRICES)

        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 9.0}})
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 9.0}, 1: {"Preco": 8.0}})
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [9.0, 8.0])
        self.click(app, "Repor rascunho (voltar ao guardado)")
        app.run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [3.5, 4.0])
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 6.0}})
        app.button(key="step_Operação").click().run()
        self.assertTrue(app.session_state["show_unsaved_nav_dialog"])
        self.click(app, "Sair sem guardar")
        self.assertEqual(app.session_state["nav_committed"], "Operação")
        self.assertEqual(app.session_state["orders_working"]["Quantidade"].tolist(), [2.0, 1.5, 1.0])
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_draft"]["Preco"].tolist(), [3.5, 4.0])
        self.assertEqual(app.session_state["price_overrides"], PRICES)


if __name__ == "__main__":
    unittest.main()
