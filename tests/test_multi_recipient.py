"""
Tests for multi-recipient email support via the send_to_all_recipients checkbox.

Covers:
- SMTPClient._parse_recipient_addresses() static method
- SMTPClient.build_email() with parse_multiple_recipients flag
- SMTPClient.check_email_mask() with single address (after refactor)
- SMTPClient.send_email_via_o365_oauth() with multiple recipients
"""

from unittest.mock import MagicMock, patch

import pytest
from keboola.component.exceptions import UserException

from client import SMTPClient
from component import Component

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


# ==================== Tests for _parse_recipient_addresses ====================


class TestParseRecipientAddresses:
    """Tests for the _parse_recipient_addresses() static method."""

    def test_single_address(self):
        """Single address should return a list with one element."""
        result = SMTPClient._parse_recipient_addresses("user@example.com")
        assert result == ["user@example.com"]

    def test_comma_separated(self):
        """Comma-separated addresses should be split into individual addresses."""
        result = SMTPClient._parse_recipient_addresses("a@x.com, b@x.com, c@x.com")
        assert result == ["a@x.com", "b@x.com", "c@x.com"]

    def test_semicolon_separated(self):
        """Semicolon-separated addresses should be split into individual addresses."""
        result = SMTPClient._parse_recipient_addresses("a@x.com; b@x.com; c@x.com")
        assert result == ["a@x.com", "b@x.com", "c@x.com"]

    def test_mixed_separators(self):
        """Mix of commas and semicolons should all be treated identically as separators."""
        result = SMTPClient._parse_recipient_addresses("a@x.com, b@x.com; c@x.com")
        assert result == ["a@x.com", "b@x.com", "c@x.com"]

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace around addresses should be stripped."""
        result = SMTPClient._parse_recipient_addresses("  a@x.com ;  b@x.com  ,  c@x.com  ")
        assert result == ["a@x.com", "b@x.com", "c@x.com"]

    def test_empty_segments_ignored(self):
        """Empty segments from consecutive separators should be ignored."""
        result = SMTPClient._parse_recipient_addresses("a@x.com,,b@x.com;;c@x.com")
        assert result == ["a@x.com", "b@x.com", "c@x.com"]

    def test_no_whitespace_separators(self):
        """Addresses without spaces around separators should work."""
        result = SMTPClient._parse_recipient_addresses("a@x.com;b@x.com")
        assert result == ["a@x.com", "b@x.com"]

    def test_single_address_with_whitespace(self):
        """Single address with surrounding whitespace should be stripped."""
        result = SMTPClient._parse_recipient_addresses("  user@example.com  ")
        assert result == ["user@example.com"]


# ==================== Tests for build_email with parse_multiple_recipients flag ====================


class TestBuildEmailMultiRecipient:
    """Tests for build_email() with the parse_multiple_recipients flag."""

    def test_single_recipient_default(self):
        """Single recipient with default flag should produce a simple To header."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
        )
        assert email["To"] == "user@example.com"

    def test_flag_false_does_not_parse(self):
        """With parse_multiple_recipients=False, multi-address string is used as-is (single recipient)."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=False,
        )
        # The whole string is treated as one recipient (not parsed)
        assert email["To"] == "a@x.com; b@x.com"

    def test_flag_true_parses_semicolons(self):
        """With parse_multiple_recipients=True, semicolons are parsed into separate recipients."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )
        assert email["To"] == "a@x.com, b@x.com"

    def test_flag_true_parses_commas(self):
        """With parse_multiple_recipients=True, commas are parsed into separate recipients."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="a@x.com, b@x.com, c@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )
        assert email["To"] == "a@x.com, b@x.com, c@x.com"

    def test_flag_true_parses_mixed_separators(self):
        """With parse_multiple_recipients=True, mixed separators are all treated the same."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com, c@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )
        assert email["To"] == "a@x.com, b@x.com, c@x.com"

    def test_flag_true_whitelist_checks_each_address(self):
        """With parse_multiple_recipients=True and whitelist, each address is checked individually."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        email = client.build_email(
            recipient_email_address="a@allowed.com; b@allowed.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )
        assert email["To"] == "a@allowed.com, b@allowed.com"

    def test_flag_true_whitelist_rejects_disallowed(self):
        """With parse_multiple_recipients=True and whitelist, a disallowed address raises."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="a@allowed.com; b@forbidden.com",
                subject="Test",
                rendered_plaintext_message="Body",
                parse_multiple_recipients=True,
            )

    def test_cc_header_set_when_cc_addresses_provided(self):
        """cc_email_addresses parses into a comma-separated Cc header."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_addresses="cc1@x.com; cc2@x.com, cc3@x.com",
        )
        assert email["To"] == "user@example.com"
        assert email["Cc"] == "cc1@x.com, cc2@x.com, cc3@x.com"

    def test_cc_header_absent_when_not_provided(self):
        """Without cc_email_addresses the Cc header should not be present."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
        )
        assert email["Cc"] is None

    def test_cc_whitelist_rejects_disallowed(self):
        """CC addresses are validated against the whitelist."""
        client = _make_client(address_whitelist=["*@allowed.com"])
        with pytest.raises(UserException, match="does not match any of the allowed masks"):
            client.build_email(
                recipient_email_address="user@allowed.com",
                subject="Test",
                rendered_plaintext_message="Body",
                cc_email_addresses="cc@forbidden.com",
            )

    def test_from_and_subject_preserved(self):
        """From and Subject headers should be set correctly with multi-recipient."""
        client = _make_client()
        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com",
            subject="Important Subject",
            rendered_plaintext_message="Body text",
            parse_multiple_recipients=True,
        )
        assert email["From"] == "sender@example.com"
        assert email["Subject"] == "Important Subject"


# ==================== Tests for check_email_mask (single address) ====================


class TestCheckEmailMask:
    """Tests for check_email_mask() after refactor to accept single addresses."""

    def test_single_allowed_address(self):
        """Single matching address should not raise."""
        client = _make_client(address_whitelist=["*@example.com"])
        client.check_email_mask("user@example.com")  # should not raise

    def test_single_disallowed_address(self):
        """Single non-matching address should raise."""
        client = _make_client(address_whitelist=["*@example.com"])
        with pytest.raises(UserException, match="does not match"):
            client.check_email_mask("user@forbidden.com")

    def test_wildcard_matching(self):
        """Wildcard patterns should work correctly."""
        client = _make_client(address_whitelist=["admin*@example.com"])
        client.check_email_mask("admin123@example.com")  # should not raise

    def test_exact_match(self):
        """Exact email match should work."""
        client = _make_client(address_whitelist=["specific@example.com"])
        client.check_email_mask("specific@example.com")  # should not raise
        with pytest.raises(UserException):
            client.check_email_mask("other@example.com")


# ==================== Tests for send_email_via_o365_oauth with multiple recipients ====================


class TestO365MultiRecipient:
    """Tests for send_email_via_o365_oauth() with multiple recipients."""

    def test_single_recipient_added(self):
        """Single recipient should be added once to O365 message."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        # Build an email with single recipient
        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        mock_message.to.add.assert_called_once_with("user@example.com")

    def test_multiple_recipients_added_individually(self):
        """With parse_multiple_recipients=True, each recipient is added individually via to.add()."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com; c@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        assert mock_message.to.add.call_count == 3
        calls = [call.args[0] for call in mock_message.to.add.call_args_list]
        assert calls == ["a@x.com", "b@x.com", "c@x.com"]

    def test_cc_recipients_added_individually(self):
        """CC header parses into individual email_.cc.add() calls on O365."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="user@example.com",
            subject="Test",
            rendered_plaintext_message="Body",
            cc_email_addresses="cc1@x.com; cc2@x.com",
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        mock_message.to.add.assert_called_once_with("user@example.com")
        assert mock_message.cc.add.call_count == 2
        cc_calls = [call.args[0] for call in mock_message.cc.add.call_args_list]
        assert cc_calls == ["cc1@x.com", "cc2@x.com"]

    def test_o365_send_called(self):
        """O365 message.send() should be called after adding recipients."""
        client = _make_client()
        mock_message = MagicMock()
        mock_account = MagicMock()
        mock_account.new_message.return_value = mock_message
        client.smtp_server = mock_account

        email = client.build_email(
            recipient_email_address="a@x.com; b@x.com",
            subject="Test",
            rendered_plaintext_message="Body",
            parse_multiple_recipients=True,
        )

        client.send_email_via_o365_oauth(email, message_body="Body", attachments_paths=[])

        mock_message.send.assert_called_once()


# ==================== Tests for Component._merge_cc_addresses ====================


class TestMergeCcAddresses:
    """Tests for Component._merge_cc_addresses static method."""

    def test_no_inputs_returns_none(self):
        assert Component._merge_cc_addresses() is None
        assert Component._merge_cc_addresses(None) is None
        assert Component._merge_cc_addresses("") is None
        assert Component._merge_cc_addresses(None, None, "") is None

    def test_single_list(self):
        assert Component._merge_cc_addresses("a@x.com, b@x.com") == "a@x.com, b@x.com"

    def test_merge_static_and_row(self):
        result = Component._merge_cc_addresses("static@x.com", "row1@x.com; row2@x.com")
        assert result == "static@x.com, row1@x.com, row2@x.com"

    def test_deduplicates_case_insensitive(self):
        result = Component._merge_cc_addresses("Shared@X.com, a@x.com", "shared@x.com; b@x.com")
        assert result == "Shared@X.com, a@x.com, b@x.com"

    def test_ignores_empty_segments(self):
        assert Component._merge_cc_addresses(",, ;; a@x.com ; ") == "a@x.com"
