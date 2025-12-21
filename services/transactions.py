from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from api import GoogleSheetsClient
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


def main() -> None:
    configure_logging(level=logging.DEBUG)
    get_category_mappings_df()


if __name__ == "__main__":
    main()
