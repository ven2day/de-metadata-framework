import pytest
from unittest.mock import MagicMock, patch, call

import ingestion.pyfiles.vault_client as vault_module
from ingestion.pyfiles.vault_client import (
    get_vault_client,
    get_kv_secret,
    get_transit_encryption_key,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_mock_client(authenticated: bool = True) -> MagicMock:
    client = MagicMock()
    client.is_authenticated.return_value = authenticated
    return client


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the module-level singleton before every test."""
    vault_module._client = None
    yield
    vault_module._client = None


# ── get_vault_client ──────────────────────────────────────────────────────────

class TestGetVaultClient:

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_returns_authenticated_client(self, mock_hvac):
        mock_client = _make_mock_client(authenticated=True)
        mock_hvac.return_value = mock_client

        result = get_vault_client()

        assert result is mock_client
        mock_hvac.assert_called_once()

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_raises_when_not_authenticated(self, mock_hvac):
        mock_client = _make_mock_client(authenticated=False)
        mock_hvac.return_value = mock_client

        with pytest.raises(RuntimeError, match="Vault authentication failed"):
            get_vault_client()

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_returns_cached_client_on_second_call(self, mock_hvac):
        mock_client = _make_mock_client(authenticated=True)
        mock_hvac.return_value = mock_client

        first  = get_vault_client()
        second = get_vault_client()

        assert first is second
        mock_hvac.assert_called_once()

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_recreates_client_when_singleton_unauthenticated(self, mock_hvac):
        stale  = _make_mock_client(authenticated=False)
        fresh  = _make_mock_client(authenticated=True)
        mock_hvac.side_effect = [fresh]

        vault_module._client = stale

        result = get_vault_client()

        assert result is fresh
        mock_hvac.assert_called_once()

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_passes_namespace_when_set(self, mock_hvac):
        mock_client = _make_mock_client(authenticated=True)
        mock_hvac.return_value = mock_client

        with patch.object(vault_module, "VAULT_NAMESPACE", "my-ns"):
            get_vault_client()

        _, kwargs = mock_hvac.call_args
        assert kwargs.get("namespace") == "my-ns"

    @patch("ingestion.pyfiles.vault_client.hvac.Client")
    def test_passes_none_namespace_when_empty(self, mock_hvac):
        mock_client = _make_mock_client(authenticated=True)
        mock_hvac.return_value = mock_client

        with patch.object(vault_module, "VAULT_NAMESPACE", ""):
            get_vault_client()

        _, kwargs = mock_hvac.call_args
        assert kwargs.get("namespace") is None


# ── get_kv_secret ─────────────────────────────────────────────────────────────

class TestGetKvSecret:

    def _inject_client(self, mock_client: MagicMock) -> None:
        vault_module._client = mock_client

    def test_kv_v2_returns_correct_value(self):
        client = _make_mock_client()
        client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"SALT_KEY": "abc123", "OTHER": "xyz"}}
        }
        self._inject_client(client)

        result = get_kv_secret(path="pipeline/creds", key="SALT_KEY")

        assert result == "abc123"
        client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="pipeline/creds",
            mount_point="secret",
            raise_on_deleted_version=True,
        )

    def test_kv_v1_returns_correct_value(self):
        client = _make_mock_client()
        client.secrets.kv.v1.read_secret.return_value = {
            "data": {"API_KEY": "secret-value"}
        }
        self._inject_client(client)

        result = get_kv_secret(path="pipeline/creds", key="API_KEY", kv_version=1)

        assert result == "secret-value"
        client.secrets.kv.v1.read_secret.assert_called_once_with(
            path="pipeline/creds",
            mount_point="secret",
        )

    def test_kv_v2_custom_mount_point(self):
        client = _make_mock_client()
        client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"TOKEN": "t123"}}
        }
        self._inject_client(client)

        result = get_kv_secret(path="app/tokens", key="TOKEN", mount_point="kv")

        assert result == "t123"
        client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="app/tokens",
            mount_point="kv",
            raise_on_deleted_version=True,
        )

    def test_missing_key_raises_key_error_v2(self):
        client = _make_mock_client()
        client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"OTHER_KEY": "value"}}
        }
        self._inject_client(client)

        with pytest.raises(KeyError, match="MISSING_KEY"):
            get_kv_secret(path="pipeline/creds", key="MISSING_KEY")

    def test_missing_key_raises_key_error_v1(self):
        client = _make_mock_client()
        client.secrets.kv.v1.read_secret.return_value = {
            "data": {"OTHER_KEY": "value"}
        }
        self._inject_client(client)

        with pytest.raises(KeyError, match="MISSING_KEY"):
            get_kv_secret(path="pipeline/creds", key="MISSING_KEY", kv_version=1)

    def test_returns_value_as_string(self):
        client = _make_mock_client()
        client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"PORT": 5432}}
        }
        self._inject_client(client)

        result = get_kv_secret(path="db/config", key="PORT")

        assert isinstance(result, str)
        assert result == "5432"

    def test_vault_error_propagates(self):
        client = _make_mock_client()
        client.secrets.kv.v2.read_secret_version.side_effect = Exception("connection refused")
        self._inject_client(client)

        with pytest.raises(Exception, match="connection refused"):
            get_kv_secret(path="pipeline/creds", key="SALT_KEY")


# ── get_transit_encryption_key ────────────────────────────────────────────────

class TestGetTransitEncryptionKey:

    def _inject_client(self, mock_client: MagicMock) -> None:
        vault_module._client = mock_client

    def test_returns_latest_version_key(self):
        client = _make_mock_client()
        client.secrets.transit.export_key.return_value = {
            "data": {"keys": {"1": "base64encodedkey=="}}
        }
        self._inject_client(client)

        result = get_transit_encryption_key(key_name="pipeline-key")

        assert result == "base64encodedkey=="
        client.secrets.transit.export_key.assert_called_once_with(
            name="pipeline-key",
            key_type="encryption-key",
            mount_point="transit",
        )

    def test_returns_highest_version_when_multiple_exist(self):
        client = _make_mock_client()
        client.secrets.transit.export_key.return_value = {
            "data": {
                "keys": {
                    "1": "oldkey==",
                    "2": "newerkey==",
                    "3": "latestkey==",
                }
            }
        }
        self._inject_client(client)

        result = get_transit_encryption_key(key_name="pipeline-key")

        assert result == "latestkey=="

    def test_custom_key_type_and_mount(self):
        client = _make_mock_client()
        client.secrets.transit.export_key.return_value = {
            "data": {"keys": {"1": "hmackeydata=="}}
        }
        self._inject_client(client)

        result = get_transit_encryption_key(
            key_name="hmac-key",
            key_type="hmac-key",
            mount_point="crypto",
        )

        assert result == "hmackeydata=="
        client.secrets.transit.export_key.assert_called_once_with(
            name="hmac-key",
            key_type="hmac-key",
            mount_point="crypto",
        )

    def test_vault_error_propagates(self):
        client = _make_mock_client()
        client.secrets.transit.export_key.side_effect = Exception("key not exportable")
        self._inject_client(client)

        with pytest.raises(Exception, match="key not exportable"):
            get_transit_encryption_key(key_name="pipeline-key")
