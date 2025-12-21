from __future__ import annotations

import os

import pytest

from services.transactions import get_category_mappings_df

requires_sheet_env = pytest.mark.skipif(
    not os.getenv("SPREADSHEET_ID") or not os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
    reason="Requires SPREADSHEET_ID and GOOGLE_SERVICE_ACCOUNT_FILE env vars",
)


@pytest.mark.integration
@requires_sheet_env
def test_get_category_mappings_df_integration() -> None:
    df = get_category_mappings_df()
    assert not df.empty
