"""Regression coverage for source changes, saved drafts and empty values."""

import io
import unittest
from unittest.mock import patch

import pandas as pd
import streamlit as st

from organizer import sessions_json
from organizer.core import normalize_produto_key
from tests import test_workflow as workflow


class ReviewStateTests(unittest.TestCase):
    # Reuse the isolated app harness without inheriting its test methods.
    setUp = workflow.WorkflowTests.setUp
    assert_app_ok = workflow.WorkflowTests.assert_app_ok
    login = workflow.WorkflowTests.login
    import_orders = workflow.WorkflowTests.import_orders
    click = workflow.WorkflowTests.click
    edit_cells = workflow.WorkflowTests.edit_cells
    save_prices = workflow.WorkflowTests.save_prices

    def test_switching_excel_sheet_replaces_all_editor_state_and_prices(self):
        workbook = io.BytesIO()
        with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
            pd.DataFrame({"Cliente": ["Ana"], "Produto": ["REF1"], "Quantidade": [2]}).to_excel(
                writer, sheet_name="Comments", index=False,
            )
            pd.DataFrame({"Cliente": ["Carla"], "Produto": ["REF9"], "Quantidade": [4]}).to_excel(
                writer, sheet_name="Segunda", index=False,
            )
        app = self.login()
        app.session_state["uploaded_input_cache"] = {"name": "direto.xlsx", "bytes": workbook.getvalue()}
        app.run()
        self.assert_app_ok(app)
        app.button(key="step_Preços").click().run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 8.0}})
        self.click(app, "Guardar preços")
        app.text_input(key="prices_search").set_value("REF1").run()
        app.button(key="step_Operação").click().run()
        app.text_input(key="orders_search").set_value("Ana").run()
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": 9.0}})

        app.selectbox(key="orders_sheet_pick_cached").set_value("Segunda").run()
        self.assert_app_ok(app)
        for key in ("orders_draft", "orders_working", "orders_editor_base"):
            self.assertEqual(app.session_state[key]["Cliente"].tolist(), ["Carla"])
            self.assertEqual(app.session_state[key]["Quantidade"].tolist(), [4.0])
        self.assertEqual(app.text_input(key="orders_search").value, "")
        self.assertEqual(app.session_state["price_overrides"], {})
        self.click(app, "Aplicar alterações")
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(saved["orders"][0]["Cliente"], "Carla")
        self.assertEqual(float(saved["orders"][0]["Quantidade"]), 4.0)
        app.button(key="step_Preços").click().run()
        self.assertEqual(app.text_input(key="prices_search").value, "")
        self.assertEqual(app.session_state["price_draft"]["Produto"].tolist(), ["REF9"])
        # Change twice from Prices, where no orders editor is created between
        # sources. The independent workspace source must still clear prices.
        app.selectbox(key="orders_sheet_pick_cached").set_value("Comments").run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_draft"]["Produto"].tolist(), ["REF1"])
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 6.0}})
        self.click(app, "Guardar preços")
        app.selectbox(key="orders_sheet_pick_cached").set_value("Segunda").run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_overrides"], {})
        self.assertEqual(app.session_state["price_draft"]["Produto"].tolist(), ["REF9"])

    def test_normalized_reference_has_one_price_row_and_one_effective_price(self):
        app = self.login()
        app.session_state["uploaded_input_cache"] = {
            "name": "references.csv",
            "bytes": b"Cliente;Produto;Quantidade\nAna;REF1;2\nBruno;ref1;1\n",
        }
        app.run()
        self.assert_app_ok(app)
        app.button(key="step_Preços").click().run()
        self.assertEqual(len(app.session_state["price_draft"]), 1)
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": 7.0}})
        self.click(app, "Guardar preços")
        self.assertEqual(app.session_state["price_overrides"], {normalize_produto_key("REF1"): 7.0})
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertIn("14,00 €", next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"))
        app.selectbox(key="messages_client_pick").set_value("Bruno").run()
        self.assertIn("7,00 €", next(area.value for area in app.text_area if area.label == "Mensagem deste cliente"))

    def test_clearing_all_prices_in_reopened_session_does_not_restore_snapshot_values(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        self.click(app, "Guardar sessão")
        app.radio(key="nav_page").set_value("Histórico").run()
        self.click(app, "Abrir")
        app.button(key="step_Preços").click().run()
        self.edit_cells(app, "all_prices_editor", {0: {"Preco": None}, 1: {"Preco": None}})
        self.click(app, "Guardar preços")
        app.run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["price_overrides"], {})
        self.assertTrue(app.session_state["price_draft"]["Preco"].isna().all())
        self.click(app, "Guardar sessão")
        payloads = [sessions_json.load_session(row["path"]) for row in sessions_json.list_sessions()]
        self.assertTrue(any(payload["price_overrides"] == {} for payload in payloads))
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertTrue(any("Sem mensagens" in warning.value for warning in app.warning))

    def test_saved_excluded_rows_return_in_editor_but_stay_out_of_messages(self):
        app = self.login()
        self.import_orders(app)
        self.save_prices(app)
        app.button(key="step_Operação").click().run()
        self.edit_cells(app, "comments_editor", {2: {"Incluir": False}})
        self.click(app, "Guardar sessão")
        saved = sessions_json.load_session(sessions_json.list_sessions()[0]["path"])
        self.assertEqual(len(saved["orders"]), 3)
        self.assertEqual([row["Incluir"] for row in saved["orders"]], [True, True, False])

        app.radio(key="nav_page").set_value("Histórico").run()
        self.click(app, "Abrir")
        self.assertEqual(app.session_state["orders_draft"]["Incluir"].tolist(), [True, True, False])
        app.selectbox(key="orders_scope").set_value("Excluídas").run()
        self.assertEqual(app.selectbox(key="orders_scope").value, "Excluídas")
        app.button(key="step_Mensagens").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.selectbox(key="messages_client_pick").options, ["Ana Silva"])
        with patch.object(st, "download_button", return_value=False) as downloads:
            app.button(key="step_Etiquetas").click().run()
            app.radio(key="labels_pick_mode").set_value("Todas").run()
            self.assert_app_ok(app)
            labels = workflow.WorkflowTests.export_bytes(downloads, "Descarregar etiquetas (HTML)").decode("utf-8")
            self.assertNotIn("Bruno Costa", labels)
            self.assertIn("Ana Silva", labels)
        app.button(key="step_Operação").click().run()
        app.selectbox(key="orders_scope").set_value("Excluídas").run()
        self.edit_cells(app, "comments_editor", {0: {"Incluir": True}})
        self.click(app, "Aplicar alterações")
        app.button(key="step_Mensagens").click().run()
        self.assertEqual(app.selectbox(key="messages_client_pick").options, ["Ana Silva", "Bruno Costa"])

    def test_applying_empty_quantity_normalizes_committed_working_and_editor_together(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": None}})
        self.click(app, "Aplicar alterações")
        for key in ("orders_draft", "orders_working", "orders_editor_base"):
            self.assertEqual(float(app.session_state[key].loc[0, "Quantidade"]), 1.0)
        pd.testing.assert_frame_equal(app.session_state["orders_draft"], app.session_state["orders_working"])
        app.button(key="step_Preços").click().run()
        self.assert_app_ok(app)
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))

    def test_saving_pending_empty_quantity_also_keeps_editor_consistent(self):
        app = self.login()
        self.import_orders(app)
        self.edit_cells(app, "comments_editor", {0: {"Quantidade": None}})
        self.click(app, "Guardar sessão")
        for key in ("orders_draft", "orders_working", "orders_editor_base"):
            self.assertEqual(float(app.session_state[key].loc[0, "Quantidade"]), 1.0)
        app.button(key="step_Preços").click().run()
        self.assertEqual(app.session_state["nav_committed"], "Preços")
        self.assertFalse(any(button.label == "Guardar e continuar" for button in app.button))


if __name__ == "__main__":
    unittest.main()
