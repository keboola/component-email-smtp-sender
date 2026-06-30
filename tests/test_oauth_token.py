"""
Regression tests for O365 OAuth token acquisition error handling.

When MSAL fails to acquire an access token (e.g. invalid_client credentials),
the client must raise a UserException so the failure surfaces to the user as a
user/configuration error (exit 1) rather than an opaque internal error (exit 2).
"""

from unittest.mock import MagicMock, patch

import pytest
from keboola.component.exceptions import UserException

from client import SMTPClient


def _make_oauth_client() -> SMTPClient:
    return SMTPClient(
        sender_email_address="sender@example.com",
        password="",
        server_host="smtp.office365.com",
        server_port=587,
        use_oauth=True,
        tenant_id="tenant-id",
        client_id="client-id",
        client_secret="client-secret",
    )


def test_init_o365_smtp_server_raises_user_exception_on_invalid_client():
    """An invalid_client token response must raise UserException, not bare Exception."""
    client = _make_oauth_client()

    with patch("client.msal.ConfidentialClientApplication") as mock_app_cls:
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "AADSTS7000215: Invalid client secret provided.",
        }
        mock_app_cls.return_value = mock_app

        with pytest.raises(UserException) as exc_info:
            client.init_smtp_server()

    assert "invalid_client" in str(exc_info.value)


def test_init_o365_smtp_server_message_is_clean_without_error_description():
    """When MSAL omits error_description, the message must not contain dangling punctuation."""
    client = _make_oauth_client()

    with patch("client.msal.ConfidentialClientApplication") as mock_app_cls:
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"error": "invalid_client"}
        mock_app_cls.return_value = mock_app

        with pytest.raises(UserException) as exc_info:
            client.init_smtp_server()

    message = str(exc_info.value)
    assert "invalid_client" in message
    assert " - ." not in message
