from __future__ import annotations

import logging
import os
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv

from api import GoogleSheetsClient, YNABClient
from logging_config import configure_logging

logger = logging.getLogger(__name__)

# Load environment variables from .env when running locally.
load_dotenv()


def get_category_mappings_df(
    spreadsheet_id: Optional[str] = None,
    sheets_client: Optional[GoogleSheetsClient] = None,
) -> pd.DataFrame:
    """
    Fetch the full "Category Mappings" sheet and return it as a DataFrame.

    Uses SPREADSHEET_ID env var by default; callers can inject a client for testing.
    """
    sheet_id = spreadsheet_id or os.getenv("SPREADSHEET_ID")
    if not sheet_id:
        logger.debug("SPREADSHEET_ID is not set; cannot fetch category mappings")
        raise RuntimeError("SPREADSHEET_ID is not set")

    client = sheets_client or GoogleSheetsClient()
    logger.debug("Fetching category mappings for spreadsheet_id=%s", sheet_id)

    try:
        result = client.get_values(sheet_id, "Category Mappings")
    except Exception as exc:
        logger.debug("Failed to fetch Category Mappings sheet: %s", exc, exc_info=True)
        raise

    values = result.get("values", [])
    if not values:
        logger.warning("No data returned for Category Mappings sheet")
        return pd.DataFrame()

    try:
        # Treat first row as headers, remaining rows as data.
        if len(values) > 1:
            headers, rows = values[0], values[1:]
            df = pd.DataFrame(rows, columns=headers)
        else:
            df = pd.DataFrame(values)
    except Exception as exc:
        logger.debug(
            "Failed to build DataFrame from sheet values: %s", exc, exc_info=True
        )
        raise

    logger.debug(
        "Loaded category mappings with shape %s and columns %s",
        df.shape,
        list(df.columns),
    )
    return df


def get_user_settings_df(
    spreadsheet_id: Optional[str] = None,
    sheets_client: Optional[GoogleSheetsClient] = None,
) -> pd.DataFrame:
    """
    Fetch the full "Users" sheet and return it as a DataFrame.

    Uses SPREADSHEET_ID env var by default; callers can inject a client for testing.
    """
    sheet_id = spreadsheet_id or os.getenv("SPREADSHEET_ID")
    if not sheet_id:
        logger.debug("SPREADSHEET_ID is not set; cannot fetch user settings")
        raise RuntimeError("SPREADSHEET_ID is not set")

    client = sheets_client or GoogleSheetsClient()
    logger.debug("Fetching user settings for spreadsheet_id=%s", sheet_id)

    try:
        result = client.get_values(sheet_id, "Users")
    except Exception as exc:
        logger.debug("Failed to fetch Users sheet: %s", exc, exc_info=True)
        raise

    values = result.get("values", [])
    if not values:
        logger.warning("No data returned for Users sheet")
        return pd.DataFrame()

    try:
        if len(values) > 1:
            headers, rows = values[0], values[1:]
            df = pd.DataFrame(rows, columns=headers)
        else:
            df = pd.DataFrame(values)
    except Exception as exc:
        logger.debug("Failed to build user settings DataFrame: %s", exc, exc_info=True)
        raise

    logger.debug(
        "Loaded user settings with shape %s and columns %s", df.shape, list(df.columns)
    )
    return df


def filter_shared_transactions_for_user(
    transactions: list[dict[str, Any]],
    user_number: int,
    spreadsheet_id: Optional[str] = None,
    sheets_client: Optional[GoogleSheetsClient] = None,
    category_mappings_df: Optional[pd.DataFrame] = None,
    user_settings_df: Optional[pd.DataFrame] = None,
) -> list[dict[str, Any]]:
    """
    Return transactions whose category is marked shared for the given user.

    Categories come from the User {n} column where the Shared flag is true in
    the Category Mappings sheet. Transactions are expected in the normalized
    shape produced by YNABClient._normalize_transaction. Transactions whose
    flag_color matches the user's "To Share Flag" setting are also included.
    """

    logger.debug("Filtering transactions for shared categories")

    if user_number < 1:
        raise ValueError("user_number must be >= 1")

    # Load category mappings and user settings (use provided dataframes if supplied).
    df = (
        category_mappings_df
        if category_mappings_df is not None
        else get_category_mappings_df(
            spreadsheet_id=spreadsheet_id, sheets_client=sheets_client
        )
    )
    users_df = (
        user_settings_df
        if user_settings_df is not None
        else get_user_settings_df(
            spreadsheet_id=spreadsheet_id, sheets_client=sheets_client
        )
    )

    if df.empty and (users_df is None or users_df.empty):
        logger.debug("No category mappings or user settings found.")
        return []

    user_col = f"User {user_number}"
    # If category columns are missing, skip category-based filtering.
    if not df.empty and ("Shared" not in df.columns or user_col not in df.columns):
        logger.debug(f'{user_col} or "Shared" columns not in dataframe')
        df = pd.DataFrame()  # ignore categories if columns missing

    # Helper to interpret truthy values in the Shared column.
    def _to_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"true", "1", "yes", "y"}

    # Build set of shared categories for this user.
    shared_categories: set[str] = set()
    if not df.empty:
        shared_mask = df["Shared"].apply(_to_bool)
        shared_categories = {
            str(category).strip().lower()
            for category in df.loc[shared_mask, user_col].tolist()
            if str(category).strip()
        }

    # Resolve the "To Share Flag" value for this user.
    to_share_flag: str | None = None
    if users_df is not None and not users_df.empty:
        try:
            users_df["User Number"] = pd.to_numeric(
                users_df["User Number"], errors="coerce"
            )
            flag_row = users_df.loc[users_df["User Number"] == user_number]
            if not flag_row.empty:
                to_share_flag = (
                    str(flag_row.iloc[0].get("To Share Flag") or "").strip().lower()
                )
        except Exception as exc:
            logger.debug("Failed to parse user settings: %s", exc, exc_info=True)

    # Check if any category name contains a shared category substring.
    def _tx_has_shared_category(tx: dict[str, Any]) -> bool:
        for category in tx.get("categories", []) or []:
            name = str(category.get("category_name") or "").strip().lower()
            if not name:
                continue
            for shared_cat in shared_categories:
                if shared_cat and shared_cat in name:
                    return True
        return False

    # Check if the transaction's flag matches the user's share flag.
    def _tx_matches_flag(tx: dict[str, Any]) -> bool:
        if not to_share_flag:
            return False
        flag = str(tx.get("flag_color") or "").strip().lower()

        return bool(flag) and flag == to_share_flag

    # Combine category and flag matches; de-duplicate by id.
    filtered: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for tx in transactions:
        if _tx_has_shared_category(tx) or _tx_matches_flag(tx):
            tx_id = str(tx.get("id") or id(tx))
            if tx_id not in seen_ids:
                seen_ids.add(tx_id)
                filtered.append(tx)

    return filtered


def main() -> None:
    configure_logging(level=logging.DEBUG)
    ynab = YNABClient()
    users_df = get_user_settings_df()
    # week_ago =
    for index, row in users_df.iterrows():
        budget_id = ynab.get_budget_id_by_name(row["Name"])
        transactions = ynab.get_transactions(
            budget_id=budget_id, since_date="2025-12-20"
        )
        shared_transactions = filter_shared_transactions_for_user(
            transactions=transactions, user_number=1
        )
        print(f"Transactions for {row["Name"]}: {shared_transactions}")


if __name__ == "__main__":
    main()
