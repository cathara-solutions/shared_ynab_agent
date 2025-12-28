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
    # Trace entry and parameters.
    logger.debug(
        "get_category_mappings_df called with spreadsheet_id=%s", spreadsheet_id
    )

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
        logger.debug(
            "Returning empty category mappings DataFrame because sheet has no values"
        )
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
    # Trace entry and parameters.
    logger.debug("get_user_settings_df called with spreadsheet_id=%s", spreadsheet_id)

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
        logger.debug(
            "Returning empty user settings DataFrame because sheet has no values"
        )
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
    Transactions from the shared account are excluded unless they carry the
    user's share flag.
    """

    # Trace entry and key parameters.
    logger.debug(
        "filter_shared_transactions_for_user called for user_number=%s with %s transactions",
        user_number,
        len(transactions),
    )

    if user_number < 1:
        raise ValueError("user_number must be >= 1")

    # Load category mappings and user settings (use provided dataframes if supplied).
    category_mapping_df = (
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

    if category_mapping_df.empty and (users_df is None or users_df.empty):
        logger.debug("No category mappings or user settings found.")
        return []

    user_col = f"User {user_number}"
    # If category columns are missing, skip category-based filtering.
    if not category_mapping_df.empty and (
        "Shared" not in category_mapping_df.columns
        or user_col not in category_mapping_df.columns
    ):
        logger.debug(f'{user_col} or "Shared" columns not in dataframe')
        category_mapping_df = pd.DataFrame()  # ignore categories if columns missing

    # Helper to interpret truthy values in the Shared column.
    def _to_bool(value: object) -> bool:
        """Convert a value to boolean using common truthy strings."""
        logger.debug("_to_bool called with value=%s", value)
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"true", "1", "yes", "y"}

    # Build set of shared categories for this user.
    shared_categories: set[str] = set()
    if not category_mapping_df.empty:
        shared_mask = category_mapping_df["Shared"].apply(_to_bool)
        shared_categories = {
            str(category).strip().lower()
            for category in category_mapping_df.loc[shared_mask, user_col].tolist()
            if str(category).strip()
        }

    # Resolve the "To Share Flag" value for this user, and shared account substring.
    to_share_flag: str | None = None
    shared_account_substr: str | None = None
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
                shared_account_substr = (
                    str(flag_row.iloc[0].get("Shared Account") or "").strip().lower()
                )
        except Exception as exc:
            logger.debug("Failed to parse user settings: %s", exc, exc_info=True)

    # Check if any category name contains a shared category substring.
    def _tx_has_shared_category(tx: dict[str, Any]) -> bool:
        """Return True if a transaction has a category containing a shared category substring."""
        logger.debug("_tx_has_shared_category evaluating tx id=%s", tx.get("id"))
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
        """Return True if the transaction's flag matches the user's share flag."""
        logger.debug("_tx_matches_flag evaluating tx id=%s", tx.get("id"))
        if not to_share_flag:
            return False
        flag = str(tx.get("flag_color") or "").strip().lower()

        return bool(flag) and flag == to_share_flag

    # Check if the transaction is from the shared account and lacks the share flag.
    def _tx_is_shared_account_without_flag(tx: dict[str, Any]) -> bool:
        """Return True if the transaction is from the shared account but lacks the share flag."""
        logger.debug(
            "_tx_is_shared_account_without_flag evaluating tx id=%s", tx.get("id")
        )
        if not shared_account_substr:
            return False
        acct_name = str(tx.get("account_name") or "").strip().lower()
        if not acct_name:
            return False
        return shared_account_substr in acct_name and not _tx_matches_flag(tx)

    # Combine category and flag matches; de-duplicate by id.
    filtered: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for tx in transactions:
        # Skip shared account transactions unless they carry the share flag.
        if _tx_is_shared_account_without_flag(tx):
            continue

        if _tx_has_shared_category(tx) or _tx_matches_flag(tx):
            tx_id = str(tx.get("id") or id(tx))
            if tx_id not in seen_ids:
                seen_ids.add(tx_id)
                filtered.append(tx)

    return filtered


def split_transactions_between_users(
    transactions: list[dict[str, Any]],
    source_user: dict[str, Any],  # {user_num:1, budget_id:xxx}
    target_user: dict[str, Any],
    category_mappings_df: pd.DataFrame,
    users_df: pd.DataFrame,
    ynab_client: Optional[YNABClient] = None,
) -> list[dict[str, Any]]:
    """
    Split transactions between a source user and target user.

    - Skip transactions already split (flag matches source user's Shared Flag).
    - Create new transactions per remaining transaction with amounts scaled by
      the target user's Shared Percentage unless the source account is already the shared account.
    - Assign accounts using each user's Shared Account and resolve account_ids.
    - Map categories using category_mappings_df (fallback to Alias == "Default")
      and resolve category_ids for both budgets with minimal API calls.
    - Returns a list of dicts grouping original, source (if created), and target transactions.
    """

    # Trace entry and key parameters.
    logger.debug(
        "split_transactions_between_users called: source_user=%s, target_user=%s, transactions=%s",
        source_user,
        target_user,
        len(transactions),
    )

    client = ynab_client or YNABClient()
    users_df = users_df.copy()

    def _contains_substring(haystack: str, needle: str) -> bool:
        """Case-insensitive substring check after stripping."""
        h = (haystack or "").strip().lower()
        n = (needle or "").strip().lower()
        return bool(h) and bool(n) and n in h

    def _get_user_row(user_num: int) -> pd.Series:
        """Fetch the user settings row for the given user number."""
        logger.debug("_get_user_row called for user_num=%s", user_num)
        users_df["User Number"] = pd.to_numeric(
            users_df["User Number"], errors="coerce"
        )
        row = users_df.loc[users_df["User Number"] == user_num]
        if row.empty:
            raise ValueError(f"No user settings found for user_num={user_num}")
        return row.iloc[0]

    source_row = _get_user_row(source_user["user_num"])
    target_row = _get_user_row(target_user["user_num"])

    shared_flag = str(source_row.get("Shared Flag") or "").strip().lower()
    shared_pct_raw = target_row.get("Share Percentage")
    shared_pct = 0.0
    if shared_pct_raw not in (None, ""):
        try:
            shared_pct = float(str(shared_pct_raw).strip())
        except (TypeError, ValueError):
            logger.debug(
                "Shared Percentage '%s' for user %s is not numeric; defaulting to 0",
                shared_pct_raw,
                target_user.get("user_num"),
            )

    source_shared_account = str(source_row.get("Shared Account") or "").strip()
    target_shared_account = str(target_row.get("Shared Account") or "").strip()

    # Pre-fetch accounts and categories once per budget to minimize API calls.
    def _get_accounts(budget_id: str) -> list[dict[str, Any]]:
        """Fetch accounts for a budget."""
        logger.debug("_get_accounts called for budget_id=%s", budget_id)
        resp = client.get(f"budgets/{budget_id}/accounts")
        return resp.get("data", {}).get("accounts", [])

    def _get_categories(budget_id: str) -> list[dict[str, Any]]:
        """Fetch and flatten categories for a budget."""
        logger.debug("_get_categories called for budget_id=%s", budget_id)
        resp = client.get(f"budgets/{budget_id}/categories")
        groups = resp.get("data", {}).get("category_groups", [])
        flattened: list[dict[str, Any]] = []
        for group in groups:
            flattened.extend(group.get("categories", []) or [])
        return flattened

    source_accounts = _get_accounts(source_user["budget_id"])
    target_accounts = _get_accounts(target_user["budget_id"])
    source_categories = _get_categories(source_user["budget_id"])
    target_categories = _get_categories(target_user["budget_id"])

    def _account_lookup(
        budget_accounts: list[dict[str, Any]], account_name: str
    ) -> str:
        """Resolve an account id by name from a pre-fetched account list."""
        logger.debug("_account_lookup called for account_name=%s", account_name)
        return client.get_id_by_name(
            path=None,
            name=account_name,
            list_key="accounts",
            name_key="name",
            id_key="id",
            objects=budget_accounts,
        )

    def _category_lookup(
        budget_categories: list[dict[str, Any]], category_name: str
    ) -> str:
        """Resolve a category id by name from a pre-fetched category list."""
        logger.debug("_category_lookup called for category_name=%s", category_name)
        return client.get_id_by_name(
            path=None,
            name=category_name,
            list_key="categories",
            name_key="name",
            id_key="id",
            objects=budget_categories,
        )

    # Prepare category mapping helpers.
    source_col = f"User {source_user['user_num']}"
    target_col = f"User {target_user['user_num']}"
    alias_col = "Alias"

    def _default_target_category() -> str | None:
        """Return default target category name from the mapping sheet."""
        logger.debug("_default_target_category called")
        if (
            alias_col not in category_mappings_df.columns
            or target_col not in category_mappings_df.columns
        ):
            logger.debug("Alias column not found in dataframe")
            return None
        default_rows = category_mappings_df.loc[
            category_mappings_df[alias_col].str.strip().str.lower() == "default"
        ]
        if default_rows.empty:
            logger.debug("No default category values")
            return None
        return str(default_rows.iloc[0].get(target_col) or "").strip()

    default_target_cat = _default_target_category()

    def _map_target_category(source_category_name: str) -> str | None:
        """Map a source category to the target user's category name."""
        logger.debug(
            "_map_target_category called for source_category_name=%s",
            source_category_name,
        )
        if (
            category_mappings_df.empty
            or source_col not in category_mappings_df.columns
            or target_col not in category_mappings_df.columns
        ):
            logger.debug(
                "Category mappings df empty OR User %s not in df OR User %s not in df",
                source_user["user_num"],
                target_user["user_num"],
            )
            return None
        # Match when the mapping value is a substring of the source category name (case-insensitive).
        source_lower = source_category_name.strip().lower()
        mask = (
            category_mappings_df[source_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .apply(lambda val: _contains_substring(source_lower, val))
        )
        match = category_mappings_df.loc[mask]
        if not match.empty:
            mapped = str(match.iloc[0].get(target_col) or "").strip()
            if mapped:
                return mapped
        return default_target_cat

    grouped_results: list[dict[str, Any]] = []

    def _build_categories(
        categories: list[dict[str, Any]],
        budget_categories: list[dict[str, Any]],
        map_target: bool,
        amount_multiplier: float,
    ) -> list[dict[str, Any]]:
        """Construct category payloads with mapped names/ids and scaled amounts."""
        logger.debug(
            "_build_categories called map_target=%s amount_multiplier=%s",
            map_target,
            amount_multiplier,
        )
        built: list[dict[str, Any]] = []
        for cat in categories or []:
            name = str(cat.get("category_name") or "").strip()
            if not name:
                continue
            target_name = _map_target_category(name) if map_target else name
            if not target_name:
                continue
            try:
                cat_id = _category_lookup(budget_categories, target_name)
            except Exception as exc:
                logger.debug(
                    "Could not resolve category '%s': %s",
                    target_name,
                    exc,
                    exc_info=True,
                )
                continue
            amount = (cat.get("amount") or 0) * amount_multiplier
            built.append(
                {
                    "category_name": target_name,
                    "category_id": cat_id,
                    "amount": amount,
                    "memo": cat.get("memo") or "",
                    "deleted": bool(cat.get("deleted")),
                }
            )
        return built

    for tx in transactions:
        # Trace each transaction as it is processed.
        logger.debug("Processing transaction id=%s for splitting", tx.get("id"))
        tx_flag = str(tx.get("flag_color") or "").strip().lower()
        if shared_flag and tx_flag == shared_flag:
            # Skip if already split (flag matches source shared flag).
            continue

        original_account_lower = str(tx.get("account_name") or "").strip().lower()
        source_shared_account_lower = source_shared_account.strip().lower()
        skip_source_tx = _contains_substring(
            original_account_lower, source_shared_account_lower
        )

        # Resolve accounts (skip source lookup if we will not emit a source transaction).
        try:
            source_account_id = (
                None
                if skip_source_tx
                else _account_lookup(source_accounts, source_shared_account)
            )
        except Exception as exc:
            logger.debug(
                "Could not resolve source account '%s': %s",
                source_shared_account,
                exc,
                exc_info=True,
            )
            continue
        try:
            target_account_id = _account_lookup(target_accounts, target_shared_account)
        except Exception as exc:
            logger.debug(
                "Could not resolve target account '%s': %s",
                target_shared_account,
                exc,
                exc_info=True,
            )
            continue

        source_categories_built: list[dict[str, Any]] = []
        if not skip_source_tx:
            source_categories_built = _build_categories(
                tx.get("categories", []),
                source_categories,
                map_target=False,
                amount_multiplier=-shared_pct,
            )
        # If the source transaction is already on the shared account, don't scale by shared_pct;
        # flip the sign so the target mirrors the original.
        target_multiplier = -1.0 if skip_source_tx else shared_pct
        target_categories_built = _build_categories(
            tx.get("categories", []),
            target_categories,
            map_target=True,
            amount_multiplier=target_multiplier,
        )

        source_total = sum(cat.get("amount") or 0 for cat in source_categories_built)
        target_total = sum(cat.get("amount") or 0 for cat in target_categories_built)

        grouped_results.append(
            {
                "original": {
                    **tx,
                    "budget_id": source_user.get("budget_id"),
                    "user_num": source_user.get("user_num"),
                },
                "source": (
                    None
                    if skip_source_tx
                    else {
                        **tx,
                        "budget_id": source_user.get("budget_id"),
                        "total_amount": source_total,
                        "account_name": source_shared_account,
                        "account_id": source_account_id,
                        "categories": source_categories_built,
                        "id": None,
                        "flag_color": None,
                        "user_num": source_user.get("user_num"),
                    }
                ),
                "target": {
                    **tx,
                    "budget_id": target_user.get("budget_id"),
                    "total_amount": target_total,
                    "account_name": target_shared_account,
                    "account_id": target_account_id,
                    "categories": target_categories_built,
                    "id": None,
                    "flag_color": None,
                    "user_num": target_user.get("user_num"),
                },
            }
        )

    return grouped_results


def upsert_shared_transactions(
    transactions: list[dict[str, Any]],
    spreadsheet_id: Optional[str] = None,
    sheets_client: Optional[GoogleSheetsClient] = None,
    users_df: Optional[pd.DataFrame] = None,
    ynab_client: Optional[YNABClient] = None,
) -> list[dict[str, Any]]:
    """
    Upsert transactions: update flag_color on existing, create new ones otherwise.

    - For transactions with an id: patch flag_color to the user's Shared Flag (lowercase).
    - For transactions without an id: create the transaction in the provided budget/account.
    """
    client = ynab_client or YNABClient()
    users_df = (
        users_df
        if users_df is not None
        else get_user_settings_df(
            spreadsheet_id=spreadsheet_id, sheets_client=sheets_client
        )
    )

    def _shared_flag_for_user(user_num: int) -> str:
        try:
            users_df["User Number"] = pd.to_numeric(
                users_df["User Number"], errors="coerce"
            )
            row = users_df.loc[users_df["User Number"] == user_num]
            if row.empty:
                return ""
            return str(row.iloc[0].get("Shared Flag") or "").strip().lower()
        except Exception as exc:
            logger.debug(
                "Failed to resolve shared flag for user %s: %s",
                user_num,
                exc,
                exc_info=True,
            )
            return ""

    results: list[dict[str, Any]] = []

    for tx in transactions:
        user_num = tx.get("user_num")
        budget_id = tx.get("budget_id")
        account_id = tx.get("account_id")
        tx_id = tx.get("id")
        if user_num is None or not budget_id:
            logger.debug("Skipping transaction missing user_num or budget_id: %s", tx)
            continue

        shared_flag = _shared_flag_for_user(int(user_num))

        if tx_id:
            payload = {"transactions": [{"id": tx_id, "flag_color": shared_flag}]}
            try:
                resp = client.patch(
                    f"budgets/{budget_id}/transactions",
                    json=payload,
                )
                results.append(
                    {
                        "action": "update",
                        "transaction_id": tx_id,
                        "budget_id": budget_id,
                        "response": resp,
                    }
                )
            except Exception as exc:
                logger.debug(
                    "Failed to update transaction %s: %s", tx_id, exc, exc_info=True
                )
            continue

        if not account_id:
            logger.debug("Skipping create due to missing account_id: %s", tx)
            continue

        categories = tx.get("categories") or []
        subtransactions = []
        for cat in categories:
            subtransactions.append(
                {
                    "amount": int(cat.get("amount")),
                    "category_id": cat.get("category_id"),
                    "memo": cat.get("memo") or "",
                }
            )

        transaction_body = {
            "account_id": account_id,
            "date": (
                tx.get("date").isoformat()
                if hasattr(tx.get("date"), "isoformat")
                else tx.get("date")
            ),
            "amount": (
                int(tx.get("total_amount"))
                if tx.get("total_amount") is not None
                else int(tx.get("amount"))
            ),
            "payee_name": tx.get("payee_name"),
            "memo": tx.get("memo") or "",
            "subtransactions": subtransactions or None,
        }

        if len(categories) == 1:
            transaction_body["category_id"] = categories[0].get("category_id")
            transaction_body["memo"] += categories[0].get("memo")
            del transaction_body["subtransactions"]

        transaction_body = {k: v for k, v in transaction_body.items() if v is not None}

        try:
            resp = client.post(
                f"budgets/{budget_id}/transactions",
                json={"transaction": transaction_body},
            )
            results.append(
                {"action": "create", "budget_id": budget_id, "response": resp}
            )
        except Exception as exc:
            logger.debug(
                "Failed to create transaction for user %s: %s",
                user_num,
                exc,
                exc_info=True,
            )

    return results


def main() -> None:
    configure_logging(level=logging.DEBUG)
    ynab = YNABClient()
    users_df = get_user_settings_df()
    category_mappings_df = get_category_mappings_df()
    # week_ago =
    users = []

    for index, row in users_df.iterrows():
        budget_id = ynab.get_budget_id_by_name(row["Name"])
        user_num = int(row["User Number"])
        transactions = ynab.get_transactions(
            budget_id=budget_id, since_date="2025-12-20"
        )
        shared_transactions = filter_shared_transactions_for_user(
            transactions=transactions,
            user_number=user_num,
            category_mappings_df=category_mappings_df,
        )

        user = {
            "user": {"budget_id": budget_id, "user_num": user_num},
            "shared_transactions": shared_transactions,
        }
        users.append(user)

    # Compare every user against every other user (all ordered pairs).
    for i, source_user in enumerate(users):
        for j, target_user in enumerate(users):
            if i == j:
                continue  # Skip same-user pair.

            transactions = source_user.get("shared_transactions") or []
            logger.debug(
                "Splitting transactions from source_user_index=%s to target_user_index=%s; tx_count=%s",
                i,
                j,
                len(transactions),
            )
            grouped = split_transactions_between_users(
                transactions=transactions,
                source_user=source_user.get("user"),
                target_user=target_user.get("user"),
                category_mappings_df=category_mappings_df,
                users_df=users_df,
                ynab_client=ynab,
            )
            print(
                f"Source {source_user['user']['user_num']} -> Target {target_user['user']['user_num']}: "
                f"{sum(1 for g in grouped if g['source'])} source txns, "
                f"{len(grouped)} target txns"
            )

            final_tx = pd.DataFrame(grouped)
            if not final_tx.empty:
                final_tx_list = final_tx.iloc[0].values.tolist()
                results = upsert_shared_transactions(
                    transactions=final_tx_list, users_df=users_df, ynab_client=ynab
                )
                print(results)
            # print(final_tx.iloc[0].values.tolist())
        # print(f"Transactions for {row["Name"]}: {shared_transactions}")


if __name__ == "__main__":
    main()
