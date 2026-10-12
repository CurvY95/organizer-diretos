import unittest

import pandas as pd

from organizer.editor_views import filter_rows, merge_rows


class EditorViewTests(unittest.TestCase):
    def test_search_is_literal_accent_insensitive_and_keeps_row_indices(self):
        frame = pd.DataFrame(
            {"Cliente": ["Ana Sá", "Bruno Costa", None], "Referência": ["REF[1]", "REF2", "REF3"]},
            index=[4, 9, 15],
        )
        self.assertEqual(filter_rows(frame, "ANA SA", ["Cliente"]).index.tolist(), [4])
        self.assertEqual(filter_rows(frame, "[1]", ["Referência"]).index.tolist(), [4])
        self.assertTrue(filter_rows(frame, "inexistente", ["Cliente"]).empty)
        pd.testing.assert_frame_equal(filter_rows(frame, "  ", ["Cliente"]), frame)

    def test_merging_nonconsecutive_view_preserves_hidden_rows_and_cleared_cells(self):
        frame = pd.DataFrame({"Preço": [3.5, 4.0, 7.0], "Nota": ["A", "B", "C"]}, index=[4, 9, 15])
        view = frame.loc[[15, 4]].copy()
        view.loc[15, "Preço"] = None
        view.loc[4, "Nota"] = ""
        merged = merge_rows(frame, view)
        self.assertEqual(merged.index.tolist(), [4, 9, 15])
        self.assertTrue(pd.isna(merged.loc[15, "Preço"]))
        self.assertEqual(merged.loc[4, "Nota"], "")
        pd.testing.assert_series_equal(merged.loc[9], frame.loc[9])
        self.assertEqual(frame.loc[4, "Nota"], "A")
        pd.testing.assert_frame_equal(merge_rows(frame, frame.iloc[:0]), frame)


if __name__ == "__main__":
    unittest.main()
