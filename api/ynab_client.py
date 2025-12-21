from __future__ import annotations

import re
from datetime import date, datetime
from difflib import get_close_matches
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from .client import AbstractApiClient


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

        def strip_emoji(text: str | None) -> str:
            # Remove emoji/symbol codepoints to keep category names clean.
            return re.sub(r"[\U00010000-\U0010ffff]", "", text or "").strip()

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
                "deleted": bool(tx.get("deleted")),
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

    def get_budget_id_by_name(self, name: str) -> str:
        """Return budget id whose name matches the provided (possibly partial) name."""
        self.logger.debug("Searching for budget name containing: %s", name)
        response = self.get("budgets")
        budgets = response.get("data", {}).get("budgets", [])
        if not budgets:
            self.logger.debug("No budgets returned when searching for %s", name)
            raise ValueError("No budgets returned from YNAB")

        query = name.lower()

        # Prefer case-insensitive substring matches (cheap and predictable).
        substring_matches = [b for b in budgets if query in b.get("name", "").lower()]
        if substring_matches:
            # If multiple match, pick the first; adjust if you want tie-breaking.
            return substring_matches[0]["id"]

        # Fallback to closest name using simple fuzzy matching.
        names = [b.get("name", "") for b in budgets]
        best_match = get_close_matches(query, names, n=1, cutoff=0.6)
        if best_match:
            matched_name = best_match[0]
            for b in budgets:
                if b.get("name", "") == matched_name:
                    return b["id"]

        self.logger.debug("No budget matched search term '%s'", name)
        raise ValueError(f"No budget found matching '{name}'")
