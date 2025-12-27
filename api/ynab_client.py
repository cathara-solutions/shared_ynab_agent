from __future__ import annotations

import re
from datetime import date, datetime
from difflib import get_close_matches
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from .client import AbstractApiClient


def strip_emoji(text: str | None) -> str:
    """
    Remove emoji and nearby joiner/variation chars so text is spaced cleanly.

    Handles both BMP symbols (e.g., coffee cup) and astral-plane emoji.
    """
    if not text:
        return ""

    emoji_re = re.compile(
        r"[\U0001F1E6-\U0001F1FF"  # regional indicators / flags
        r"\U0001F300-\U0001FAFF"  # pictographs, symbols, emoji
        r"\u2600-\u27BF"  # misc symbols + dingbats (includes coffee cup, heart exclamation)
        r"\ufe0f"  # variation selector
        r"\u200d"  # zero-width joiner
        r"]",
        flags=re.UNICODE,
    )
    cleaned = emoji_re.sub(" ", text)
    return " ".join(cleaned.split())


class YNABClient(AbstractApiClient):
    def __init__(self, base_url: Optional[str] = None) -> None:
        load_dotenv()
        super().__init__(base_url=base_url or "https://api.ynab.com/v1")

    @property
    def api_key_env_var(self) -> str:
        return "YNAB_API_KEY"

    def get_transactions(
        self,
        budget_id: str,
        since_date: date | str,
    ) -> list[dict[str, Any]]:
        """Fetch transactions since a date, handling pagination and normalization."""
        try:
            iso_date = (
                since_date.isoformat()
                if isinstance(since_date, date)
                else datetime.fromisoformat(str(since_date)).date().isoformat()
            )
        except ValueError:
            self.logger.debug(
                "Invalid since_date provided: %s", since_date, exc_info=True
            )
            raise

        self.logger.debug(
            "Fetching transactions for budget=%s since=%s", budget_id, iso_date
        )

        page = 1
        all_transactions: list[dict[str, Any]] = []
        last_first_id: str | None = None

        try:
            while True:
                payload = self.get(
                    f"budgets/{budget_id}/transactions",
                    params={"since_date": iso_date, "page": page},
                )
                transactions = payload.get("data", {}).get("transactions", [])
                if not transactions:
                    break

                first_id = transactions[0].get("id")
                if last_first_id and first_id == last_first_id:
                    # Defensive: stop if the API ignores page to avoid infinite loop.
                    break
                last_first_id = first_id

                for tx in transactions:
                    all_transactions.append(self._normalize_transaction(tx))

                # Stop when this page was short; YNAB returns <= page size per page.
                if len(transactions) < 200:
                    break
                page += 1
        except requests.RequestException as exc:
            self.logger.error(
                "Failed fetching transactions for budget=%s: %s",
                budget_id,
                exc,
                exc_info=True,
            )
            self.logger.debug("RequestException fetching transactions", exc_info=True)
            raise

        self.logger.debug("Fetched %s transactions", len(all_transactions))
        return all_transactions

    def _normalize_transaction(self, tx: dict[str, Any]) -> dict[str, Any]:
        """Normalize a YNAB transaction into the agreed schema."""

        try:
            categories: list[dict[str, Any]] = []
            subtransactions = tx.get("subtransactions") or []
            if subtransactions:
                # Use category data from each subtransaction; ignore the synthetic "Split".
                for sub in subtransactions:
                    name = strip_emoji(sub.get("category_name") or "")
                    if name.lower() == "split":
                        continue
                    categories.append(
                        {
                            "category_name": name,
                            "amount": sub.get("amount"),
                            "memo": sub.get("memo") or "",
                            "deleted": bool(sub.get("deleted")),
                        }
                    )
            else:
                # No subs: fall back to the transaction's own category fields.
                name = strip_emoji(tx.get("category_name") or "")
                if name.lower() != "split":
                    categories.append(
                        {
                            "category_name": name,
                            "amount": tx.get("amount"),
                            "memo": tx.get("memo") or "",
                            "deleted": bool(tx.get("deleted")),
                        }
                    )

            return {
                "id": tx.get("id", ""),
                "date": datetime.fromisoformat(tx.get("date", "")).date(),
                "total_amount": tx.get("amount"),
                "cleared": tx.get("cleared"),  # Keep raw YNAB cleared value.
                "approved": bool(tx.get("approved")),
                "payee_name": tx.get("payee_name") or "",
                "account_name": strip_emoji(tx.get("account_name") or ""),
                "deleted": bool(tx.get("deleted")),
                "flag_color": tx.get("flag_color") or "",
                "categories": categories,
            }
        except Exception as exc:
            self.logger.error(
                "Failed to normalize transaction %s: %s",
                tx.get("id"),
                exc,
                exc_info=True,
            )
            self.logger.debug(
                "Normalization exception for transaction %s",
                tx.get("id"),
                exc_info=True,
            )
            raise

    def get_id_by_name(
        self,
        path: str | None,
        name: str,
        list_key: str = "budgets",
        name_key: str = "name",
        id_key: str = "id",
        objects: Optional[list[dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> str:
        """
        Return an object's id whose name matches the provided (possibly partial) name.

        - path: API path (e.g., "budgets"); if None, uses provided objects
        - list_key: key within data holding the list (default: "budgets")
        - name_key/id_key: keys for name and id fields on each object
        - objects: optional pre-fetched list to search (bypasses GET)
        - kwargs: extra params for GET
        """
        if objects is None:
            if not path:
                raise ValueError("path is required when objects are not provided")
            self.logger.debug("Searching for %s containing: %s", path, name)
            response = self.get(path, params=kwargs or None)
            objects = response.get("data", {}).get(list_key, [])
        else:
            self.logger.debug(
                "Searching provided objects for name containing: %s", name
            )
        if not objects:
            self.logger.debug("No %s available when searching for %s", list_key, name)
            raise ValueError(f"No {list_key} available to search")

        query = name.lower()

        # Prefer case-insensitive substring matches (cheap and predictable).
        substring_matches = [
            obj for obj in objects if query in str(obj.get(name_key, "")).lower()
        ]
        if substring_matches:
            # If multiple match, pick the first.
            return substring_matches[0][id_key]

        # Fallback to closest name using simple fuzzy matching.
        names = [str(obj.get(name_key, "")) for obj in objects]
        best_match = get_close_matches(query, names, n=1, cutoff=0.6)
        if best_match:
            matched_name = best_match[0]
            for obj in objects:
                if str(obj.get(name_key, "")) == matched_name:
                    return obj[id_key]

        self.logger.debug("No object matched search term '%s' at path %s", name, path)
        raise ValueError(f"No object found matching '{name}' at path '{path}'")

    def get_budget_id_by_name(self, name: str) -> str:
        """Return budget id whose name matches the provided (possibly partial) name."""
        return self.get_id_by_name(
            "budgets", name, list_key="budgets", name_key="name", id_key="id"
        )

    def get_category_id_by_name(self, budget_id: str, name: str) -> str:
        """
        Return category id by name within a budget, searching all category groups.
        """
        self.logger.debug(
            "Searching for category containing '%s' in budget %s", name, budget_id
        )
        response = self.get(f"budgets/{budget_id}/categories")
        groups = response.get("data", {}).get("category_groups", [])
        categories: list[dict[str, Any]] = []
        for group in groups:
            categories.extend(group.get("categories", []) or [])
        return self.get_id_by_name(
            path=None,
            name=name,
            list_key="categories",
            name_key="name",
            id_key="id",
            objects=categories,
        )

    def get_account_id_by_name(self, budget_id: str, name: str) -> str:
        """Return account id by name within a budget."""
        self.logger.debug(
            "Searching for account containing '%s' in budget %s", name, budget_id
        )
        path = f"budgets/{budget_id}/accounts"
        return self.get_id_by_name(
            path=path,
            name=name,
            list_key="accounts",
            name_key="name",
            id_key="id",
        )
