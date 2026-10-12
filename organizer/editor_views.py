"""Search editable tables while retaining rows outside the current view."""

import unicodedata
from collections.abc import Sequence

import pandas as pd


def _search_text(value: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", value.casefold())
        if not unicodedata.combining(char)
    )


def filter_rows(frame: pd.DataFrame, query: str, columns: Sequence[str]) -> pd.DataFrame:
    """Match literal text without case/accents, keeping original row indices."""
    needle = _search_text(query.strip())
    if not needle:
        return frame.copy()
    matches = pd.Series(False, index=frame.index)
    for column in columns:
        if column in frame.columns:
            values = frame[column].fillna("").astype(str).map(_search_text)
            matches |= values.str.contains(needle, regex=False)
    return frame.loc[matches].copy()


def merge_rows(full: pd.DataFrame, edited: pd.DataFrame) -> pd.DataFrame:
    """Apply a fixed-row editor view, including cleared cells, to its full draft."""
    if not full.index.is_unique or not edited.index.is_unique:
        raise ValueError("Editor rows need unique indices.")
    if not edited.index.isin(full.index).all() or not edited.columns.isin(full.columns).all():
        raise ValueError("Editor view contains unknown rows or columns.")
    result = full.copy()
    result.loc[edited.index, edited.columns] = edited
    return result
