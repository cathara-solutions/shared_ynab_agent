from __future__ import annotations

from api.ynab_client import YNABClient


def test_normalize_transaction_strips_emoji_and_separates_words() -> None:
    client = YNABClient()
    tx = {
        "id": "t1",
        "date": "2024-01-01",
        "category_name": "Coffee☕Shop",
        "account_name": "Checking❣️Account",
        "account_id": "acc-1",
        "amount": 1000,
        "cleared": "cleared",
        "approved": True,
        "payee_name": "Cafe",
        "deleted": False,
    }

    normalized = client._normalize_transaction(tx)  # pylint: disable=protected-access
    assert normalized["categories"][0]["category_name"] == "Coffee Shop"
    assert normalized["account_name"] == "Checking Account"
    assert normalized["account_id"] == "acc-1"


def test_normalize_transaction_strips_bmp_and_astral_emoji() -> None:
    client = YNABClient()
    tx = {
        "id": "t2",
        "date": "2024-02-02",
        "category_name": "Groceries 🛒",
        "account_name": "Brokerage 🏦",
        "account_id": "acc-2",
        "amount": 2000,
        "cleared": "cleared",
        "approved": True,
        "payee_name": "Market",
        "deleted": False,
    }

    normalized = client._normalize_transaction(tx)  # pylint: disable=protected-access
    assert normalized["categories"][0]["category_name"] == "Groceries"
    assert normalized["account_name"] == "Brokerage"
