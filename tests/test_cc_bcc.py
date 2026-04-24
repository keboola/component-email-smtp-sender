"""
Tests for CC and BCC email support.

Covers:
- SMTPClient.build_email() with cc_email_address and bcc_email_address parameters
- Address whitelist validation for CC and BCC recipients
- O365 send path with CC and BCC recipients
"""

from unittest.mock import MagicMock, patch

import pytest
from keboola.component.exceptions import UserException

from client import SMTPClient

# ==================== Helpers ====================


def _make_client(**overrides) -> SMTPClient:
    """Create an SMTPClient with mocked SMTP server initialization."""
    defaults = dict(
        sender_email_address="sender@example.com",
        password="password",
        server_host="smtp.example.com",
        server_port=465,
        connection_protocol="SSL",
    )
    defaults.update(overrides)
    with patch.object(SMTPClient, "_init_ssl_smtp_server"):
        return SMTPClient(**defaults)


# ==================== Tests for build_email with CC/BCC ====================


class TestBuildEmailCcBcc:
    """Tests for build_email() with cc_email_address and bcc_email_address parameters."""

    def test_no_cc_bcc_by_default(self):
        """Without CC/BCC params, headers should not be set."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
        )
        assert email["To"] == "user@example.com"
        assert email["Cc"] is None
        assert email["Bcc"] is None

    def test_cc_single_address(self):
        """Single CC address should be set in Cc header."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc@example.com",
        )
        assert email["To"] == "to@example.com"
        assert email["Cc"] == "cc@example.com"
        assert email["Bcc"] is None

    def test_bcc_single_address(self):
        """Single BCC address should be set in Bcc header."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            bcc_email_address="bcc@example.com",
        )
        assert email["To"] == "to@example.com"
        assert email["Cc"] is None
        assert email["Bcc"] == "bcc@example.com"

    def test_cc_multiple_addresses(self):
        """Multiple CC addresses (comma-separated) should be parsed and formatted."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc1@example.com, cc2@example.com",
        )
        assert email["Cc"] == "cc1@example.com, cc2@example.com"

    def test_bcc_multiple_addresses_semicolon(self):
        """Multiple BCC addresses (semicolon-separated) should be parsed and formatted."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            bcc_email_address="bcc1@example.com; bcc2@example.com",
        )
        assert email["Bcc"] == "bcc1@example.com, bcc2@example.com"

    def test_cc_and_bcc_together(self):
        """Both CC and BCC can be set simultaneously."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc@example.com",
            bcc_email_address="bcc@example.com",
        )
        assert email["To"] == "to@example.com"
        assert email["Cc"] == "cc@example.com"
        assert email["Bcc"] == "bcc@example.com"

    def test_empty_cc_bcc_strings_not_set(self):
        """Empty strings for CC/BCC should not set headers."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="",
            bcc_email_address="",
        )
        assert email["Cc"] is None
        assert email["Bcc"] is None


# ==================== Tests for whitelist validation with CC/BCC ====================


class TestWhitelistCcBcc:
    """Tests for address whitelist validation on CC and BCC recipients."""

    def test_cc_allowed_by_whitelist(self):
        """CC address matching whitelist should not raise."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        email = client.build_email(
            recipient_email_address="to@allowed.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc@allowed.com",
        )
        assert email["Cc"] == "cc@allowed.com"

    def test_cc_rejected_by_whitelist(self):
        """CC address not matching whitelist should raise UserException."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="to@allowed.com",
                subject="Test",
                rendered_plaintext_message="Body",
                cc_email_address="cc@forbidden.com",
            )

    def test_bcc_allowed_by_whitelist(self):
        """BCC address matching whitelist should not raise."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        email = client.build_email(
            recipient_email_address="to@allowed.com",
            subject="Test",
            rendered_plaintext_message="Body",
            bcc_email_address="bcc@allowed.com",
        )
        assert email["Bcc"] == "bcc@allowed.com"

    def test_bcc_rejected_by_whitelist(self):
        """BCC address not matching whitelist should raise UserException."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="to@allowed.com",
                subject="Test",
                rendered_plaintext_message="Body",
                bcc_email_address="bcc@forbidden.com",
            )

    def test_multiple_cc_one_rejected(self):
        """If one CC address in a multi-address string is disallowed, it should raise."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="to@allowed.com",
                subject="Test",
                rendered_plaintext_message="Body",
                cc_email_address="cc1@allowed.com, cc2@forbidden.com",
            )

    def test_multiple_bcc_one_rejected(self):
        """If one BCC address in a multi-address string is disallowed, it should raise."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="to@allowed.com",
                subject="Test",
                rendered_plaintext_message="Body",
                bcc_email_address="bcc1@allowed.com; bcc2@forbidden.com",
            )

    def test_no_whitelist_allows_all_cc_bcc(self):
        """Without whitelist, any CC/BCC addresses should be accepted."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="to@any.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc@any.com",
            bcc_email_address="bcc@any.com",
        )
        assert email["Cc"] == "cc@any.com"
        assert email["Bcc"] == "bcc@any.com"


# ==================== Tests for O365 with CC/BCC ====================


class TestO365CcBcc:
    """Tests for send_email_via_o365_oauth() with CC and BCC recipients."""

    def test_o365_cc_added(self):
        """CC recipients should be added via cc.add() on O365 message."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc1@example.com, cc2@example.com",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        assert mock_message.cc.add.call_count == 2
        cc_calls = [call.args[0] for call in mock_message.cc.add.call_args_list]
        assert cc_calls == ["cc1@example.com", "cc2@example.com"]

    def test_o365_bcc_added(self):
        """BCC recipients should be added via bcc.add() on O365 message."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            bcc_email_address="bcc1@example.com; bcc2@example.com",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        assert mock_message.bcc.add.call_count == 2
        bcc_calls = [call.args[0] for call in mock_message.bcc.add.call_args_list]
        assert bcc_calls == ["bcc1@example.com", "bcc2@example.com"]

    def test_o365_no_cc_bcc_when_not_set(self):
        """Without CC/BCC, cc.add() and bcc.add() should not be called."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        mock_message.cc.add.assert_not_called()
        mock_message.bcc.add.assert_not_called()

    def test_o365_cc_and_bcc_together(self):
        """Both CC and BCC should be handled simultaneously in O365."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="to@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_address="cc@example.com",
            bcc_email_address="bcc@example.com",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        mock_message.cc.add.assert_called_once_with("cc@example.com")
        mock_message.bcc.add.assert_called_once_with("bcc@example.com")
        mock_message.send.assert_called_once()
