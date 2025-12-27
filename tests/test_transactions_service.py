from __future__ import annotations

import pandas as pd
import pytest

from services.transactions import (
    filter_shared_transactions_for_user,
    get_category_mappings_df,
)


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


def test_filter_shared_transactions_for_user_filters_by_user_column() -> None:
    df = pd.DataFrame(
        {
            "User 1": ["Rent", "Coffee", "Fuel"],
            "User 2": ["Groceries", "Rent", "Gym"],
            "Shared": [True, False, "true"],
        }
    )
    users_df = pd.DataFrame(
        {
            "User Number": [1, 2],
            "To Share Flag": ["", ""],
        }
    )

    transactions = [
        {
            "id": "1",
            "categories": [
                {"category_name": "Rent"},
            ],
        },
        {
            "id": "2",
            "categories": [
                {"category_name": "Coffee"},
            ],
        },
        {
            "id": "3",
            "categories": [
                {"category_name": "Gym"},
            ],
        },
        {
            "id": "4",
            "categories": [
                {"category_name": "Fuel"},
            ],
        },
    ]

    # User 1: shared categories Rent (True) and Fuel ("true"); Coffee is not shared.
    filtered_user1 = filter_shared_transactions_for_user(
        transactions,
        user_number=1,
        category_mappings_df=df,
        user_settings_df=users_df,
    )
    assert {tx["id"] for tx in filtered_user1} == {"1", "4"}

    # User 2: shared categories Groceries (True) and Gym ("true"); Rent is not shared.
    filtered_user2 = filter_shared_transactions_for_user(
        transactions,
        user_number=2,
        category_mappings_df=df,
        user_settings_df=users_df,
    )
    assert {tx["id"] for tx in filtered_user2} == {"3"}


def test_filter_shared_transactions_includes_flag_matches() -> None:
    df = pd.DataFrame(
        {
            "User 1": ["Rent", "Coffee", "Fuel"],
            "Shared": [True, False, False],
        }
    )
    users_df = pd.DataFrame(
        {
            "User Number": [1],
            "To Share Flag": ["blue"],
        }
    )
    transactions = [
        {"id": "1", "categories": [{"category_name": "Rent"}], "flag_color": ""},
        {"id": "2", "categories": [{"category_name": "Coffee"}], "flag_color": "blue"},
        {"id": "3", "categories": [{"category_name": "Snacks"}], "flag_color": "red"},
    ]

    filtered = filter_shared_transactions_for_user(
        transactions,
        user_number=1,
        category_mappings_df=df,
        user_settings_df=users_df,
    )

    # Should include Rent (shared category) and Coffee (flag match), but not Snacks.
    assert {tx["id"] for tx in filtered} == {"1", "2"}
