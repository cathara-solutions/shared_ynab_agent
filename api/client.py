from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Optional

import requests
from dotenv import load_dotenv


class AbstractApiClient(ABC):
    """Base client that manages a requests Session and API key handling."""

    def __init__(self, base_url: Optional[str] = None) -> None:
        load_dotenv()
        self.base_url = base_url.rstrip("/") if base_url else None
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self._api_key}"})
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

    @property
    @abstractmethod
    def api_key_env_var(self) -> str:
        """Name of the environment variable that stores the API key."""

    @property
    def _api_key(self) -> str:
        api_key = os.getenv(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(
                f"API key environment variable '{self.api_key_env_var}' is not set"
            )
        return api_key

    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if self.base_url:
            return f"{self.base_url}/{path.lstrip('/')}"
        return path

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        url = self._build_url(path)
        response = self.session.request(method=method, url=url, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise requests.HTTPError(
                f"{method.upper()} {url} failed with status {response.status_code}: {response.text}",
                response=response,
                request=response.request,
            ) from exc
        return response.json()

    def get(self, path: str, **kwargs: Any) -> Any:
        self.logger.debug("GET %s starting", path)
        return self._request("get", path, **kwargs)

    def post(
        self,
        path: str,
        json: Any | None = None,
        data: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        self.logger.debug("POST %s starting", path)
        return self._request("post", path, json=json, data=data, **kwargs)
