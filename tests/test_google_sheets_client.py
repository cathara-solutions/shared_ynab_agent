from __future__ import annotations

from api.google_sheets_client import GoogleSheetsClient, service_account


class DummyCredentials:
    def __init__(self) -> None:
        self.valid = False
        self.token = None
        self.refreshed = False

    def refresh(self, *_args, **_kwargs) -> None:
        self.refreshed = True
        self.valid = True
        self.token = "dummy-token"


def test_load_credentials_refreshes(monkeypatch, tmp_path) -> None:
    """Ensure credentials load via env path and refresh when not valid."""
    dummy_path = tmp_path / "creds.json"
    dummy_path.write_text("{}")

    captured = {}

    def fake_from_file(path, scopes):
        captured["path"] = path
        captured["scopes"] = scopes
        return DummyCredentials()

    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(dummy_path))
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_file",
        staticmethod(fake_from_file),
    )

    client = GoogleSheetsClient()
    creds = client._load_credentials()

    assert isinstance(creds, DummyCredentials)
    assert creds.refreshed is True
    assert creds.token == "dummy-token"
    assert captured["path"] == str(dummy_path)
    assert captured["scopes"] == client.SCOPES


def test_api_key_refreshes_when_missing_token(monkeypatch) -> None:
    """_api_key should refresh credentials lacking a token."""
    creds = DummyCredentials()
    creds.token = None
    creds.valid = False

    client = GoogleSheetsClient(service_account_file="unused.json")
    monkeypatch.setattr(client, "_load_credentials", lambda: creds)

    token = client._api_key

    assert token == "dummy-token"
    assert creds.refreshed is True
    assert creds.valid is True
