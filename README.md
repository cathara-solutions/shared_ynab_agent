# Shared YNAB Agent

An assistant service to read household budgets from YNAB, pull sharing rules from Google Sheets, identify “shared” transactions, and split/upsert them across multiple user budgets.

## Project Layout

- `main.py` — FastAPI entrypoint. Exposes health/base routes plus:
  - `GET /transactions` (always 400), `GET /transactions/shared`, `POST /transactions/split/preview`, `POST /transactions/split`.
  - Initializes logging, Google Sheet dataframes, and a shared `YNABClient` on startup.
  - Pydantic models define request/response schemas for OpenAPI docs.
- `api/`
  - `client.py` — Base API client with auth header handling and request helpers.
  - `google_sheets_client.py` — Minimal wrapper around the Google Sheets API for reading “Users” and “Category Mappings” sheets.
  - `ynab_client.py` — YNAB REST client; fetches/normalizes transactions, resolves IDs by name, and queries budgets/accounts/categories.
- `services/`
  - `share_transactions.py` — Core logic for:
    - Loading Google Sheets data (`get_user_settings_df`, `get_category_mappings_df`).
    - Selecting shared transactions per user (`filter_shared_transactions_for_user`).
    - Splitting transactions between user budgets (`split_transactions_between_users`).
    - Upserting transactions/flags back to YNAB (`upsert_shared_transactions`).
- `tests/` — Pytest coverage for Sheets client, YNAB client, and transaction sharing logic.
- `logging_config.py` — Root logger setup; app defaults to DEBUG and logs to stdout.
- `.env.example` — Expected environment variables (see below).

## FastAPI Endpoints (high level)

- `GET /health` — Simple liveness check.
- `GET /` — Base endpoint placeholder.
- `GET /transactions` — Always returns HTTP 400 (invalid usage guard).
- `GET /transactions/shared` — Retrieves shared transactions for all users (optionally filtered by `since_date`, defaults to one week ago). Returns a list of users with their shared transactions. Errors surface for missing Sheets data, missing budgets/users, YNAB credential issues, or shared-transaction filtering failures.
- `POST /transactions/split/preview` — Accepts optional body of shared-transaction responses; if omitted, fetches shared data first. Runs the split logic for all user pairs and returns previewed grouped transactions (original/source/target) without writing to YNAB.
- `POST /transactions/split` — Accepts optional body of split previews; if omitted, performs the preview internally, then upserts transactions/flags to YNAB. Returns upsert results (action/budget/transaction id/response). Surfaces 401/404/500 errors for credential, Sheets, or processing issues.

Descriptions for each endpoint are scaffolded as `description="TODO: add description"` in `main.py` for you to refine in OpenAPI docs.

## Configuration

- Copy `.env.example` to `.env` and set:
  - `YNAB_API_KEY` — Personal access token for YNAB API.
  - `SPREADSHEET_ID` — Google Sheet containing “Users” and “Category Mappings” tabs.
  - `GOOGLE_SERVICE_ACCOUNT_FILE` — Path to credentials file (for local dev if you keep the file).
  - `GOOGLE_CREDENTIALS_B64` — Base64 of the service account JSON; app writes it to a temp file and points `GOOGLE_SERVICE_ACCOUNT_FILE` at it (useful for Railway/containers).
  - `API_KEY` — Required in the `x-api-key` header on all requests.
- Logging writes to stdout; level is DEBUG by default.

## Running the FastAPI app

1. Install dependencies: `poetry install`
2. Start dev server: `poetry run uvicorn main:app --reload`
3. Open docs: `http://localhost:8000/docs` (or `/redoc`) for schemas and examples.

Auth: All endpoints require `x-api-key` matching the `API_KEY` env var.

Google credentials: For container/Railway deploys, set `GOOGLE_CREDENTIALS_B64` and the app will write a temp `credentials.json` and set `GOOGLE_SERVICE_ACCOUNT_FILE`. Locally you can keep `credentials.json` and set `GOOGLE_SERVICE_ACCOUNT_FILE=credentials.json`.

Logging: Logs go to stdout/stderr (no `log.txt` in production). Pipe to a file locally if desired.

Railway quick-notes: Set start command `uvicorn main:app --host 0.0.0.0 --port $PORT`; configure env vars per environment (`API_KEY`, `YNAB_API_KEY`, `SPREADSHEET_ID`, `GOOGLE_CREDENTIALS_B64`). Filesystem is ephemeral; rely on env vars/base64 materialization for secrets.

## Development & Tests

- Run tests: `poetry run pytest`
- Code style: `pre-commit` hooks are configured (see `.pre-commit-config.yaml`).

## Notes for contributors (human or AI)

- The sharing rules live in Google Sheets; most errors around missing data are surfaced as 404s from endpoints.
- `split_transactions_between_users` expects plain dict transactions shaped like `YNABClient._normalize_transaction` output.
 - `upsert_shared_transactions` consumes grouped dicts (original/source/target) and pushes updates/creates to YNAB.
- Keep endpoint descriptions in `main.py` updated to improve generated OpenAPI docs.
