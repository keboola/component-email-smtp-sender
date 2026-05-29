"""
Tests for per-recipient send-status logging in send_emails().

Covers:
- Success log: "Email sent successfully to ..." is emitted after successful send
- Failure log: "Failed to send email to ...: <error>" is emitted on SMTP exception
- Outer-failure log: "Failed to process row for recipient ...: <error>" on row-level errors
- Summary log: "Email send loop complete: N sent, M failed" at end of loop
"""

import csv
import logging
import os
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from component import Component
from configuration import Configuration


def _make_advanced_config(**overrides) -> dict:
    config = {
        "configuration_type": "advanced",
        "connection_config": {
            "use_oauth": False,
            "creds_config": {
                "sender_email_address": "test@example.com",
                "server_host": "smtp.example.com",
                "server_port": 465,
            },
        },
        "advanced_options": {
            "email_data_table_name": "email_data.csv",
            "recipient_email_address_column": "email",
            "subject_config": {
                "subject_source": "from_template_definition",
                "subject_template_definition": "Test Subject",
            },
            "message_body_config": {
                "message_body_source": "from_template_definition",
                "use_html_template": False,
                "plaintext_template_definition": "Hello",
            },
            "include_attachments": False,
            "attachments_config": {
                "attachments_source": "all_input_files",
            },
        },
        "continue_on_error": True,
        "dry_run": False,
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and key in config and isinstance(config[key], dict):
            config[key].update(value)
        else:
            config[key] = value
    return config


@pytest.fixture
def send_component(tmp_path):
    """Create a Component wired for send_emails() with a real CSV input table."""
    comp = Component.__new__(Component)
    comp.cfg = Configuration.load_from_dict(_make_advanced_config())

    mock_config = MagicMock()
    mock_config.parameters = _make_advanced_config()

    with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
        mock_conf_prop.return_value = mock_config
        comp.environment_variables = MagicMock()

        # Wire a real results CSV writer
        results_path = tmp_path / "results.csv"
        results_file = open(results_path, "w", newline="")
        from component import RESULT_TABLE_COLUMNS

        comp._results_writer = csv.DictWriter(results_file, fieldnames=RESULT_TABLE_COLUMNS)
        comp._results_writer.writeheader()
        comp._results_writer.errors = False

        # Mock client
        comp._client = MagicMock()
        comp._client.sender_email_address = "test@example.com"
        comp._client.disable_attachments = False

        def _build_email(**kwargs):
            msg = MagicMock()
            msg.__getitem__ = lambda self, key: {
                "To": kwargs.get("recipient_email_address", ""),
                "From": "test@example.com",
                "Subject": kwargs.get("subject", "Test Subject"),
                "Cc": kwargs.get("cc_email_address"),
                "Bcc": kwargs.get("bcc_email_address"),
            }.get(key)
            return msg

        comp._client.build_email.side_effect = _build_email

        yield comp
        results_file.close()


def _write_email_csv(path, rows: list[dict[str, str]]) -> str:
    filepath = os.path.join(path, "email_data.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return filepath


class TestSuccessLog:
    """After successful send_email(), an info log is emitted per recipient."""

    def test_success_log_emitted(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(str(tmp_path), [{"email": "alice@example.com"}])
        send_component._client.send_email.return_value = None

        with caplog.at_level(logging.INFO):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Email sent successfully to alice@example.com" in r.message for r in caplog.records)

    def test_success_log_multiple_recipients(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(
            str(tmp_path),
            [{"email": "alice@example.com"}, {"email": "bob@example.com"}],
        )
        send_component._client.send_email.return_value = None

        with caplog.at_level(logging.INFO):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        messages = [r.message for r in caplog.records]
        assert any("Email sent successfully to alice@example.com" in m for m in messages)
        assert any("Email sent successfully to bob@example.com" in m for m in messages)


class TestFailureLog:
    """On SMTP exception, a warning log is emitted with the error message."""

    def test_failure_log_emitted(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(str(tmp_path), [{"email": "fail@example.com"}])
        send_component._client.send_email.side_effect = Exception("Connection refused")

        with caplog.at_level(logging.WARNING):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Failed to send email to fail@example.com: Connection refused" in r.message for r in caplog.records)

    def test_failure_log_contains_error_detail(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(str(tmp_path), [{"email": "fail@example.com"}])
        send_component._client.send_email.side_effect = TimeoutError("SMTP timeout after 30s")

        with caplog.at_level(logging.WARNING):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("SMTP timeout after 30s" in r.message for r in warning_records)


class TestOuterFailureLog:
    """Row-level processing errors (before SMTP call) emit a warning log."""

    def test_outer_failure_log_emitted(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(str(tmp_path), [{"email": "user@example.com"}])
        send_component._client.build_email.side_effect = KeyError("missing_column")

        with caplog.at_level(logging.WARNING):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Failed to process row for recipient user@example.com" in r.message for r in caplog.records)


class TestSummaryLog:
    """End-of-loop summary log reports ok and error counts."""

    def test_summary_all_ok(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(
            str(tmp_path),
            [{"email": "a@example.com"}, {"email": "b@example.com"}],
        )
        send_component._client.send_email.return_value = None

        with caplog.at_level(logging.INFO):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Email send loop complete: 2 sent, 0 failed" in r.message for r in caplog.records)

    def test_summary_mixed(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(
            str(tmp_path),
            [{"email": "ok@example.com"}, {"email": "fail@example.com"}],
        )
        send_component._client.send_email.side_effect = [None, Exception("error")]

        with caplog.at_level(logging.INFO):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Email send loop complete: 1 sent, 1 failed" in r.message for r in caplog.records)

    def test_summary_all_failed(self, send_component, tmp_path, caplog):
        csv_path = _write_email_csv(str(tmp_path), [{"email": "fail@example.com"}])
        send_component._client.send_email.side_effect = Exception("down")

        with caplog.at_level(logging.INFO):
            send_component.send_emails(
                attachments_paths_by_filename={},
                email_data_table_path=csv_path,
            )

        assert any("Email send loop complete: 0 sent, 1 failed" in r.message for r in caplog.records)
