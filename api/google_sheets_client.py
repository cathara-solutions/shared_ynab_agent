from __future__ import annotations

import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

from .client import AbstractApiClient


class GoogleSheetsClient(AbstractApiClient):
    """Client for Google Sheets API using a service account credential."""

    SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)

    def __init__(
        self,
        base_url: Optional[str] = None,
        service_account_file: Optional[str] = None,
    ) -> None:
        load_dotenv()
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.service_account_file = service_account_file or os.getenv(
            "GOOGLE_SERVICE_ACCOUNT_FILE"
        )
        self._credentials: Optional[service_account.Credentials] = None
        super().__init__(base_url=base_url or "https://sheets.googleapis.com/v4")

    @property
    def api_key_env_var(self) -> str:
        return "GOOGLE_SERVICE_ACCOUNT_FILE"

    def _load_credentials(self) -> service_account.Credentials:
        try:
            # Ensure we have a file path from the constructor or environment.
            if not self.service_account_file:
                # Fail fast when no credential file is configured.
                raise RuntimeError(
                    "Service account file is not set; set GOOGLE_SERVICE_ACCOUNT_FILE"
                )
            # Lazily load the credentials from the JSON file with required scopes.
            if self._credentials is None:
                self._credentials = (
                    service_account.Credentials.from_service_account_file(
                        self.service_account_file, scopes=self.SCOPES
                    )
                )
            # Refresh if the token is missing or no longer valid.
            if not self._credentials.valid or not self._credentials.token:
                self._credentials.refresh(GoogleAuthRequest())
            # Return the ready-to-use credential object.
            return self._credentials
        except Exception as exc:
            self.logger.debug("Failed to load credentials: %s", exc, exc_info=True)
            raise

    @property
    def _api_key(self) -> str:
        try:
            credentials = self._load_credentials()
            if not credentials.valid or not credentials.token:
                credentials.refresh(GoogleAuthRequest())
            return credentials.token
        except Exception as exc:
            self.logger.debug(
                "Failed to obtain Google Sheets API token: %s", exc, exc_info=True
            )
            raise

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        # Ensure the session has a fresh bearer token before each request.
        try:
            self.session.headers.update({"Authorization": f"Bearer {self._api_key}"})
            return super()._request(method, path, **kwargs)
        except Exception as exc:
            self.logger.debug(
                "Request to Google Sheets API failed: %s %s: %s",
                method.upper(),
                path,
                exc,
                exc_info=True,
            )
            raise

    def get_values(self, spreadsheet_id: str, range_: str, **params: Any) -> Any:
        """
        Fetch values for a given sheet range (A1 notation).

        Optional query params can include majorDimension, valueRenderOption, etc.
        """
        try:
            return self.get(
                f"spreadsheets/{spreadsheet_id}/values/{range_}",
                params=params or None,
            )
        except Exception as exc:
            self.logger.debug(
                "Failed to fetch values for spreadsheet_id=%s range=%s: %s",
                spreadsheet_id,
                range_,
                exc,
                exc_info=True,
            )
            raise
