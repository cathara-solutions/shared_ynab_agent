import logging
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

from api import YNABClient
from logging_config import configure_logging
from services.share_transactions import (
    filter_shared_transactions_for_user,
    get_category_mappings_df,
    get_user_settings_df,
    split_transactions_between_users,
    upsert_shared_transactions,
)


class UserSummary(BaseModel):
    budget_id: str
    user_num: int


class CategoryEntry(BaseModel):
    category_name: str
    amount: Optional[int] = None
    memo: str = ""
    deleted: bool = False


class Transaction(BaseModel):
    id: str
    date: date
    total_amount: Optional[int] = None
    cleared: Optional[str] = None
    approved: bool
    payee_name: str
    account_name: str
    deleted: bool
    flag_color: str
    categories: List[CategoryEntry] = []


class CategoryWithId(CategoryEntry):
    category_id: Optional[str] = None
    amount: Optional[float] = None


class OriginalTransaction(Transaction):
    budget_id: str
    user_num: int


class SplitTransactionPayload(BaseModel):
    id: Optional[str] = None
    date: date
    total_amount: Optional[float] = None
    cleared: Optional[str] = None
    approved: bool
    payee_name: str
    account_name: str
    account_id: Optional[str] = None
    deleted: bool
    flag_color: Optional[str] = None
    budget_id: str
    user_num: int
    categories: List[CategoryWithId] = []


class SharedTransactionsResponse(BaseModel):
    user: UserSummary
    shared_transactions: List[Transaction]


class ErrorResponse(BaseModel):
    detail: str


class SplitPreviewResult(BaseModel):
    original: OriginalTransaction
    source: Optional[SplitTransactionPayload] = None
    target: SplitTransactionPayload


class UpsertResult(BaseModel):
    action: str
    budget_id: str
    transaction_id: Optional[str] = None
    response: Dict[str, Any]


def verify_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """Simple API key check using the x-api-key header."""
    expected = os.getenv("API_KEY")
    if not expected or not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def create_app() -> FastAPI:
    """Build the FastAPI application."""
    configure_logging(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    app = FastAPI(title="Shared YNAB Agent", dependencies=[Depends(verify_api_key)])

    # Initialize shared resources once at startup to avoid repeated construction and logging noise.
    try:
        app.state.category_mappings_df = get_category_mappings_df()
    except Exception as exc:
        logger.exception(
            "Failed to load category mappings at startup: %s", exc, exc_info=True
        )
        app.state.category_mappings_df = None

    try:
        app.state.users_df = get_user_settings_df()
    except Exception as exc:
        logger.exception(
            "Failed to load user settings at startup: %s", exc, exc_info=True
        )
        app.state.users_df = None

    try:
        app.state.ynab_client = YNABClient()
    except Exception as exc:
        logger.exception(
            "Failed to initialize YNAB client at startup: %s", exc, exc_info=True
        )
        app.state.ynab_client = None

    @app.get(
        "/health",
        tags=["system"],
        description="Check the health of the API.",
    )
    async def health() -> dict[str, str]:
        return {"status": "Ok"}

    def _resolve_resources():
        category_mappings_df = getattr(app.state, "category_mappings_df", None)
        users_df = getattr(app.state, "users_df", None)
        ynab_client = getattr(app.state, "ynab_client", None)

        if category_mappings_df is None:
            raise HTTPException(
                status_code=404, detail="Error finding Google Sheet category mappings"
            )

        if users_df is None:
            raise HTTPException(
                status_code=404, detail="Error finding Google Sheet user settings"
            )

        if ynab_client is None:
            raise HTTPException(
                status_code=401, detail="Issue creating YNAB credentials"
            )

        return category_mappings_df, users_df, ynab_client

    async def _collect_shared_transactions(
        since_date: Optional[date],
    ) -> List[SharedTransactionsResponse]:
        category_mappings_df, users_df, ynab = _resolve_resources()

        users: list[SharedTransactionsResponse] = []
        effective_since = since_date or (date.today() - timedelta(days=7))

        for _, row in users_df.iterrows():
            budget_name = row.get("Budget Name")

            try:
                budget_id = ynab.get_budget_id_by_name(budget_name)
            except Exception:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Failed to get budget by id: {budget_name} "
                        "with this budget name value of the row"
                    ),
                )
            if not budget_id:
                raise HTTPException(status_code=404, detail="Budget not found")

            try:
                user_num = int(row["User Number"])
            except Exception:
                raise HTTPException(status_code=404, detail="User not found")

            transactions = ynab.get_transactions(
                budget_id=budget_id, since_date=effective_since
            )

            try:
                shared_transactions = filter_shared_transactions_for_user(
                    transactions=transactions,
                    user_number=user_num,
                    category_mappings_df=category_mappings_df,
                )
            except Exception:
                raise HTTPException(
                    status_code=404, detail="Issue retrieving shared transactions"
                )

            users.append(
                SharedTransactionsResponse(
                    user=UserSummary(budget_id=budget_id, user_num=user_num),
                    shared_transactions=shared_transactions,
                )
            )

        return users

    @app.get(
        "/transactions/shared",
        tags=["transactions"],
        description="Retrieve all transactions that should be shared amongst users according to Google Sheets configs.",
        response_model=List[SharedTransactionsResponse],
        responses={
            401: {
                "description": "Issue creating YNAB credentials",
                "model": ErrorResponse,
            },
            404: {
                "description": "Issues retrieving Google Sheets data, budgets, users, or shared transactions",
                "model": ErrorResponse,
                "content": {
                    "application/json": {
                        "examples": {
                            "missing_user_settings": {
                                "summary": "User settings not found",
                                "value": {
                                    "detail": "Error finding Google Sheet user settings"
                                },
                            },
                            "missing_category_mappings": {
                                "summary": "Category mappings not found",
                                "value": {
                                    "detail": "Error finding Google Sheet category mappings"
                                },
                            },
                            "budget_lookup_failure": {
                                "summary": "Budget lookup failed",
                                "value": {
                                    "detail": (
                                        "Failed to get budget by id: Example Budget "
                                        "with this budget name value of the row"
                                    )
                                },
                            },
                            "budget_not_found": {
                                "summary": "Budget not found",
                                "value": {"detail": "Budget not found"},
                            },
                            "user_not_found": {
                                "summary": "User not found",
                                "value": {"detail": "User not found"},
                            },
                            "shared_transactions_issue": {
                                "summary": "Issue retrieving shared transactions",
                                "value": {
                                    "detail": "Issue retrieving shared transactions"
                                },
                            },
                        }
                    }
                },
            },
        },
    )
    async def get_shared_transactions(
        since_date: Optional[date] = Query(default=None, example=date(2025, 12, 30)),
    ) -> List[SharedTransactionsResponse]:
        return await _collect_shared_transactions(since_date)

    @app.post(
        "/transactions/split/preview",
        tags=["transactions"],
        description="Preview transactions to be created/edited based on split settings in Google Sheets configs.",
        response_model=List[SplitPreviewResult],
        responses={
            400: {
                "description": "Invalid request payload",
                "model": ErrorResponse,
            },
            401: {
                "description": "Issue creating YNAB credentials",
                "model": ErrorResponse,
            },
            404: {
                "description": "Issues retrieving Google Sheets data, budgets, users, or shared transactions",
                "model": ErrorResponse,
            },
            500: {
                "description": "Issue splitting shared transactions",
                "model": ErrorResponse,
            },
        },
    )
    async def preview_split_transactions(
        shared: Optional[List[SharedTransactionsResponse]] = None,
        since_date: Optional[date] = Query(default=None, example=date(2025, 12, 30)),
    ) -> List[SplitPreviewResult]:
        shared_transactions = shared
        if shared_transactions is None:
            logger.debug("Transaction not provided in request body")
            shared_transactions = await _collect_shared_transactions(since_date)

        results: List[SplitPreviewResult] = []

        category_mappings_df, users_df, ynab_client = _resolve_resources()

        try:
            for i, source_user in enumerate(shared_transactions or []):
                for j, target_user in enumerate(shared_transactions or []):
                    if i == j:
                        continue

                    transactions = [
                        tx.model_dump()
                        for tx in (source_user.shared_transactions or [])
                    ]
                    grouped = split_transactions_between_users(
                        transactions=transactions,
                        source_user=source_user.user.model_dump(),
                        target_user=target_user.user.model_dump(),
                        category_mappings_df=category_mappings_df,
                        users_df=users_df,
                        ynab_client=ynab_client,
                    )
                    results.extend(SplitPreviewResult(**g) for g in grouped)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=500, detail="Issue splitting shared transactions"
            )

        return results

    @app.post(
        "/transactions/split",
        tags=["transactions"],
        description="Generate/update transactions that should be split based on settings in Google Sheets configs.",
        response_model=List[UpsertResult],
        responses={
            400: {
                "description": "Invalid request payload",
                "model": ErrorResponse,
            },
            401: {
                "description": "Issue creating YNAB credentials",
                "model": ErrorResponse,
            },
            404: {
                "description": "Issues retrieving Google Sheets data, budgets, users, or shared transactions",
                "model": ErrorResponse,
            },
            500: {
                "description": "Issue splitting or upserting shared transactions",
                "model": ErrorResponse,
            },
        },
    )
    async def split_transactions_endpoint(
        shared: Optional[List[SplitPreviewResult]] = None,
        since_date: Optional[date] = Query(default=None, example=date(2025, 12, 30)),
    ) -> List[UpsertResult]:
        # If no body is provided, reuse preview logic first.
        preview_data = shared
        if preview_data is None:
            logger.debug(
                "No split preview provided in request body; generating preview"
            )
            preview_data = await preview_split_transactions(
                shared=None, since_date=since_date
            )

        category_mappings_df, users_df, ynab_client = _resolve_resources()

        # If preview data came from request body, we still need to rebuild grouped dicts to feed upsert.
        results: List[UpsertResult] = []
        try:
            # Convert preview data back into grouped dictionaries compatible with upsert_shared_transactions.
            grouped_dicts: List[dict] = []
            for preview in preview_data or []:
                grouped_dicts.append(
                    {
                        "original": preview.original.model_dump(),
                        "source": (
                            preview.source.model_dump()
                            if preview.source is not None
                            else None
                        ),
                        "target": preview.target.model_dump(),
                    }
                )

            # When preview was not provided (we generated it), we already got grouped dicts as SplitPreviewResult from the preview endpoint.
            upsert_payload: List[Dict[str, Any]] = []
            for grouped in grouped_dicts:
                if grouped.get("original"):
                    upsert_payload.append(grouped["original"])
                if grouped.get("source"):
                    upsert_payload.append(grouped["source"])
                if grouped.get("target"):
                    upsert_payload.append(grouped["target"])

            upsert_results = upsert_shared_transactions(
                transactions=upsert_payload,
                users_df=users_df,
                ynab_client=ynab_client,
            )
            results = [UpsertResult(**res) for res in upsert_results]
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=500,
                detail="Issue splitting or upserting shared transactions",
            )

        return results

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
