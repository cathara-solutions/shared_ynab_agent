from __future__ import annotations

import pytest

from services.transactions import get_category_mappings_df


class FakeSheetsClient:
    def __init__(self, values):
        self.values = values
        self.calls = []

    def get_values(self, spreadsheet_id, range_, **params):
        self.calls.append((spreadsheet_id, range_, params))
        return {"values": self.values}


def test_get_category_mappings_df_parses_headers(monkeypatch) -> None:
    client = FakeSheetsClient(
        [
            ["Category", "Bucket"],
            ["Rent", "Housing"],
            ["Coffee", "Food"],
        ]
    )
    df = get_category_mappings_df(spreadsheet_id="sheet-123", sheets_client=client)

    assert client.calls == [("sheet-123", "Category Mappings", {})]
    assert list(df.columns) == ["Category", "Bucket"]
    assert df.shape == (2, 2)
    assert df.iloc[0]["Category"] == "Rent"


def test_get_category_mappings_df_empty(monkeypatch) -> None:
    client = FakeSheetsClient([])
    df = get_category_mappings_df(spreadsheet_id="sheet-123", sheets_client=client)
    assert df.empty


def test_get_category_mappings_df_requires_sheet_id(monkeypatch) -> None:
    with pytest.raises(RuntimeError):
        get_category_mappings_df(
            spreadsheet_id=None, sheets_client=FakeSheetsClient([])
        )
