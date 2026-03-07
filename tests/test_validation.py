"""
Comprehensive test suite for configuration validation in Component.

Tests cover:
- _validate_run_configuration() method
- _resolve_data_source_table_path() method
- load_email_data_table_path() method
- _load_attachment_tables() method
- _return_table_path() method
- validate_single_table_() sync action method
- validate_subject_() sync action helper
- validate_plaintext_template_() / validate_html_template_() sync action helpers
- validate_attachments_() sync action helper
- validate_config() sync action method
- _parse_template_placeholders() static method
"""

import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration

# ==================== Fixtures & shared helpers ====================


@pytest.fixture
def mock_environment():
    """Mock environment variables."""
    env = MagicMock()
    env.stack_id = "connection.keboola.com"
    env.project_id = "123"
    env.run_id = "456"
    return env


@pytest.fixture
def mock_tables_mapping():
    """Mock tables_input_mapping with common test tables."""
    table1 = MagicMock()
    table1.destination = "email_basis.csv"
    table1.source = "out.c-bucket.email_basis"

    table2 = MagicMock()
    table2.destination = "email_export.csv"
    table2.source = "out.c-bucket.email_export_dataset"

    return [table1, table2]


@pytest.fixture
def mock_input_tables():
    """Mock get_input_tables_definitions() return value."""
    table1 = MagicMock()
    table1.name = "email_basis"
    table1.full_path = "/data/in/tables/email_basis.csv"

    table2 = MagicMock()
    table2.name = "email_export_dataset"
    table2.full_path = "/data/in/tables/email_export.csv"

    return [table1, table2]


@pytest.fixture
def component(mock_environment, mock_tables_mapping, mock_input_tables):
    """
    Create a Component instance with mocked internals for testing.

    Returns a Component with:
    - Valid advanced configuration
    - Mocked environment variables
    - Mocked tables_input_mapping
    - Mocked get_input_tables_definitions()
    """
    # ComponentBase.__init__ requires a real KBC_DATADIR on disk with config.json,
    # Storage API credentials, and other runtime infrastructure that doesn't exist
    # in unit tests. __new__ skips __init__ entirely so we can patch in only the
    # minimal attributes each test actually needs.
    comp = Component.__new__(Component)
    comp.cfg = Configuration.load_from_dict(make_advanced_config())

    mock_config = MagicMock()
    mock_config.tables_input_mapping = mock_tables_mapping
    mock_config.parameters = make_advanced_config()

    with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
        mock_conf_prop.return_value = mock_config
        comp._configuration_mock = mock_conf_prop

        comp.environment_variables = mock_environment
        comp.get_input_tables_definitions = MagicMock(return_value=mock_input_tables)
        comp._init_storage_client = MagicMock()
        comp._count_csv_rows = MagicMock(return_value=100)

        yield comp


def make_advanced_config(**overrides) -> dict:
    """
    Build a valid advanced configuration dict with optional overrides.

    Args:
        **overrides: Nested dict of overrides to apply to the base config

    Returns:
        Complete configuration dict
    """
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
            "email_data_table_name": "email_basis.csv",
            "recipient_email_address_column": "RECIPIENT_EMAIL",
            "subject_config": {
                "subject_source": "from_template_definition",
                "subject_template_definition": "Test Subject",
            },
            "message_body_config": {
                "message_body_source": "from_template_definition",
                "use_html_template": False,
                "plaintext_template_definition": "Test Body",
            },
            "include_attachments": True,
            "attachments_config": {
                "attachments_source": "all_input_files",
            },
        },
        "continue_on_error": True,
        "dry_run": False,
    }

    _deep_update(config, overrides)
    return config


def _deep_update(base_dict, update_dict):
    """Recursively update nested dictionary."""
    for key, value in update_dict.items():
        if isinstance(value, dict) and key in base_dict and isinstance(base_dict[key], dict):
            _deep_update(base_dict[key], value)
        else:
            base_dict[key] = value


@contextmanager
def make_component_for_sync(config: dict, table_csv: str = None):
    """
    Build a Component with mocked internals for sync action validation tests.

    Optionally writes `table_csv` content to a temp file and mocks
    `_return_table_path` to return its path.
    """
    tmpdir_holder = []

    comp = Component.__new__(Component)
    comp.cfg = Configuration.load_from_dict(config)

    mock_env = MagicMock()
    mock_env.stack_id = "connection.keboola.com"
    mock_env.token = None
    comp.environment_variables = mock_env

    table1 = MagicMock()
    table1.destination = "email_basis.csv"
    table1.source = "out.c-bucket.email_basis"

    mock_conf = MagicMock()
    mock_conf.tables_input_mapping = [table1]
    mock_conf.action = "validate_config"

    with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
        mock_conf_prop.return_value = mock_conf

        if table_csv is not None:
            tmpdir = tempfile.mkdtemp()
            tmpdir_holder.append(tmpdir)
            csv_path = Path(tmpdir) / "email_basis.csv"
            csv_path.write_text(table_csv)
            comp._return_table_path = MagicMock(return_value=str(csv_path))
        else:
            comp._return_table_path = MagicMock(return_value=None)

        yield comp

    if tmpdir_holder:
        shutil.rmtree(tmpdir_holder[0], ignore_errors=True)


def _mock_template_file(comp, content: str) -> None:
    """
    Set up the three mocks needed to simulate a template file in Storage.

    Mocks _list_files_in_sync_actions, creates a real temp file with `content`,
    and mocks _download_file_from_storage_api to return its path.
    """
    comp._list_files_in_sync_actions = MagicMock(return_value=[{"id": 1, "name": "template.txt"}])
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(content)
        tmp_path = f.name
    comp._download_file_from_storage_api = MagicMock(return_value=tmp_path)


@contextmanager
def make_component_for_validate_config(config: dict, image_parameters: dict = None):
    """
    Build a Component for validate_config() tests with sub-validators pre-mocked to SUCCESS.

    All base validators are mocked so tests can focus on routing logic by overriding
    specific validators or asserting on call counts.
    """
    comp = Component.__new__(Component)
    comp.cfg = Configuration.load_from_dict(config)

    mock_env = MagicMock()
    mock_env.stack_id = "connection.keboola.com"
    mock_env.token = None
    comp.environment_variables = mock_env

    table1 = MagicMock()
    table1.destination = "email_basis.csv"
    table1.source = "out.c-bucket.email_basis"

    mock_conf = MagicMock()
    mock_conf.tables_input_mapping = [table1]
    mock_conf.action = "validate_config"
    mock_conf.image_parameters = image_parameters or {}

    with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
        mock_conf_prop.return_value = mock_conf

        comp.test_smtp_server_connection_ = MagicMock(return_value=_make_success())
        comp.validate_subject_ = MagicMock(return_value=_make_success())
        comp.validate_plaintext_template_ = MagicMock(return_value=_make_success())
        comp.validate_html_template_ = MagicMock(return_value=_make_success())
        comp.validate_attachments_ = MagicMock(return_value=_make_success())
        comp.validate_single_table_ = MagicMock(return_value=_make_success())
        comp._return_table_path = MagicMock(return_value=None)

        yield comp


def _make_success():
    from keboola.component.sync_actions import MessageType, ValidationResult

    return ValidationResult("✅ OK", MessageType.SUCCESS)


def _make_danger(msg: str = "❌ Something failed"):
    from keboola.component.sync_actions import MessageType, ValidationResult

    return ValidationResult(msg, MessageType.DANGER)


# ==================== Tests for _validate_run_configuration() ====================


class TestValidateRunConfiguration:
    """Tests for the _validate_run_configuration() method."""

    def test_basic_mode_skips_validation(self, component):
        """Basic mode should skip all validation."""
        component.cfg = Configuration.load_from_dict(make_advanced_config(configuration_type="basic"))
        component._validate_run_configuration()

    def test_single_table_missing_source_table(self, component):
        """Single table mode without source table specified should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={"attachments_config": {"attachments_source": "single_table", "source_table": None}}
            )
        )
        with pytest.raises(UserException, match="Source table must be specified"):
            component._validate_run_configuration()

    def test_single_table_source_table_not_found(self, component):
        """Single table mode with non-existent source table should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "single_table",
                        "source_table": "nonexistent.csv",
                    }
                }
            )
        )
        with pytest.raises(UserException, match="not found in input tables"):
            component._validate_run_configuration()

    def test_single_table_neither_toggle_enabled(self, component):
        """Single table mode with neither CSV sample nor snapshot link enabled should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "single_table",
                        "source_table": "email_export.csv",
                        "include_csv_sample": False,
                        "include_snapshot_link": False,
                    }
                }
            )
        )
        with pytest.raises(UserException, match="At least one option must be enabled"):
            component._validate_run_configuration()

    @pytest.mark.parametrize(
        "include_csv_sample, include_snapshot_link",
        [
            pytest.param(True, False, id="csv_sample_only"),
            pytest.param(False, True, id="snapshot_link_only"),
            pytest.param(True, True, id="both_toggles"),
        ],
    )
    def test_single_table_valid_toggle_combinations(self, component, include_csv_sample, include_snapshot_link):
        """Single table mode should pass when at least one output toggle is enabled."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "single_table",
                        "source_table": "email_export.csv",
                        "include_csv_sample": include_csv_sample,
                        "include_snapshot_link": include_snapshot_link,
                    }
                }
            )
        )
        component._validate_run_configuration()  # should not raise

    @pytest.mark.parametrize(
        "config_override",
        [
            {"advanced_options": {"include_attachments": False}},
            {"advanced_options": {"attachments_config": {"attachments_source": "from_table"}}},
            {"advanced_options": {"attachments_config": {"attachments_source": "all_input_files"}}},
        ],
    )
    def test_data_preview_not_active_skips_validation(self, component, config_override):
        """Data preview validation skipped when not active."""
        component.cfg = Configuration.load_from_dict(make_advanced_config(**config_override))
        component._validate_run_configuration()  # should not raise

    def test_unknown_attachments_source(self, component):
        """Unknown attachment source value (e.g., legacy 'data_preview') should raise clear error."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(advanced_options={"attachments_config": {"attachments_source": "data_preview"}})
        )
        with pytest.raises(
            UserException,
            match="Unknown attachment source: 'data_preview'. Valid options: 'from_table', 'single_table', 'all_input_files'",
        ):
            component._validate_run_configuration()


# ==================== Tests for _resolve_data_source_table_path() ====================


class TestResolveDataSourceTablePath:
    """Tests for the _resolve_data_source_table_path() method."""

    @pytest.mark.parametrize(
        "destination,expected_path",
        [
            ("email_basis.csv", "/data/in/tables/email_basis.csv"),
            ("email_export.csv", "/data/in/tables/email_export.csv"),
        ],
    )
    def test_resolve_path_success(self, component, destination, expected_path):
        """Resolving existing table should return correct path."""
        result = component._resolve_data_source_table_path(destination)
        assert result == expected_path

    def test_resolve_path_not_found(self, component):
        """Resolving non-existent table should raise with available tables."""
        with pytest.raises(UserException, match="not found in input tables"):
            component._resolve_data_source_table_path("nonexistent.csv")

    def test_resolve_path_shows_available_tables(self, component):
        """Error message should list available tables."""
        with pytest.raises(UserException) as exc_info:
            component._resolve_data_source_table_path("nonexistent.csv")
        assert "email_basis.csv" in str(exc_info.value)
        assert "email_export.csv" in str(exc_info.value)


# ==================== Tests for load_email_data_table_path() ====================


class TestLoadEmailDataTablePath:
    """Tests for the load_email_data_table_path() static method."""

    @pytest.mark.parametrize(
        "table_name,expected_path",
        [
            ("email_basis.csv", "/data/in/tables/email_basis.csv"),
            ("email_export.csv", "/data/in/tables/email_export.csv"),
        ],
    )
    def test_load_table_path_success(self, mock_input_tables, table_name, expected_path):
        """Loading existing table should return correct path."""
        result = Component.load_email_data_table_path(mock_input_tables, table_name)
        assert result == expected_path

    def test_load_table_path_not_found_returns_none(self, mock_input_tables):
        """Loading non-existent table should return None."""
        result = Component.load_email_data_table_path(mock_input_tables, "nonexistent.csv")
        assert result is None


# ==================== Tests for _load_attachment_tables() ====================


class TestLoadAttachmentTables:
    """Tests for the _load_attachment_tables() static method."""

    def test_exclude_table_correctly(self, mock_input_tables):
        """Excluded table should not be in result."""
        result = Component._load_attachment_tables(mock_input_tables, "email_basis.csv")
        assert "email_basis.csv" not in result
        assert "email_export.csv" in result

    def test_other_tables_included(self, mock_input_tables):
        """Non-excluded tables should be included with correct paths."""
        result = Component._load_attachment_tables(mock_input_tables, "email_basis.csv")
        assert result["email_export.csv"] == "/data/in/tables/email_export.csv"

    def test_keys_are_filenames(self, mock_input_tables):
        """Result keys should be filenames extracted from full_path."""
        result = Component._load_attachment_tables(mock_input_tables, "nonexistent.csv")
        assert "email_basis.csv" in result
        assert "email_export.csv" in result
        assert result["email_basis.csv"] == "/data/in/tables/email_basis.csv"


# ==================== Tests for _load_attachment_files() ====================


class TestLoadAttachmentFiles:
    """Tests for the _load_attachment_files() method.

    The method renames each input file from its hashed storage path
    (e.g. /data/in/files/12345_invoice.pdf) to its human-readable name
    (e.g. /data/in/files/invoice.pdf) and returns a {filename: new_path} dict.

    BUG REPRODUCED (Keboola error: "Error loading attachments: 'str' object has no
    attribute 'with_segments'"):
    The original code called Path.rename(original_path, new_path) where both
    arguments are plain strings. In Python 3.12+, Path.rename() called as a
    class method requires a Path as its first argument (self), not a str.
    Fix: Path(original_path).rename(new_path) — instantiate first, then rename.
    """

    def _make_component(self, plaintext_template_path=None, html_template_path=None):
        """Build a minimal Component with template path attributes set."""
        comp = Component.__new__(Component)
        comp.plaintext_template_path = plaintext_template_path
        comp.html_template_path = html_template_path
        return comp

    def _make_file_mock(self, full_path: str, name: str):
        """Build a file definition mock matching keboola.component's FileDefinition API."""
        f = MagicMock()
        f.full_path = full_path
        f.name = name
        return f

    def test_renames_files_and_returns_dict(self, tmp_path):
        """
        REGRESSION: reproduces "Error loading attachments: 'str' object has no
        attribute 'with_segments'" from Keboola.

        Path.rename(str, str) fails in Python 3.12+ because the class-method form
        requires a Path instance as self, not a plain string. The fix is
        Path(original_path).rename(new_path).
        """
        # Create a real file with a hashed storage name (as Keboola downloads it)
        hashed = tmp_path / "12345_invoice_2024_q4.pdf"
        hashed.write_bytes(b"%PDF placeholder")

        expected_new_path = str(tmp_path / "invoice_2024_q4.pdf")

        file_mock = self._make_file_mock(str(hashed), "invoice_2024_q4.pdf")
        comp = self._make_component()

        result = comp._load_attachment_files({"invoice_2024_q4.pdf": [file_mock]})

        assert result == {"invoice_2024_q4.pdf": expected_new_path}
        assert not hashed.exists(), "original hashed file should be gone after rename"
        assert (tmp_path / "invoice_2024_q4.pdf").exists(), "renamed file should exist"

    def test_multiple_files_all_renamed(self, tmp_path):
        """All input files (except templates) are renamed and returned."""
        files_data = [
            ("11111_invoice_2024_q4.pdf", "invoice_2024_q4.pdf"),
            ("22222_service_agreement.pdf", "service_agreement.pdf"),
            ("33333_quarterly_report.xlsx", "quarterly_report.xlsx"),
        ]
        in_files_by_name = {}
        for hashed_name, clean_name in files_data:
            path = tmp_path / hashed_name
            path.write_bytes(b"content")
            in_files_by_name[clean_name] = [self._make_file_mock(str(path), clean_name)]

        comp = self._make_component()
        result = comp._load_attachment_files(in_files_by_name)

        assert set(result.keys()) == {"invoice_2024_q4.pdf", "service_agreement.pdf", "quarterly_report.xlsx"}
        for clean_name in result:
            assert (tmp_path / clean_name).exists()

    def test_template_files_excluded(self, tmp_path):
        """Files matching plaintext_template_path or html_template_path are skipped."""
        plaintext_hashed = tmp_path / "99999_template.txt"
        plaintext_hashed.write_text("Dear {{name}}")
        attachment_hashed = tmp_path / "44444_invoice.pdf"
        attachment_hashed.write_bytes(b"data")

        plaintext_mock = self._make_file_mock(str(plaintext_hashed), "template.txt")
        attachment_mock = self._make_file_mock(str(attachment_hashed), "invoice.pdf")

        comp = self._make_component(plaintext_template_path=str(plaintext_hashed))

        result = comp._load_attachment_files(
            {
                "template.txt": [plaintext_mock],
                "invoice.pdf": [attachment_mock],
            }
        )

        assert "template.txt" not in result, "template file must be excluded"
        assert "invoice.pdf" in result

    def test_empty_input_returns_empty_dict(self):
        """No input files → empty result, no error."""
        comp = self._make_component()
        result = comp._load_attachment_files({})
        assert result == {}


# ==================== Tests for _return_table_path() ====================


class TestReturnTablePath:
    """Tests for the _return_table_path() method.

    This method has two branches:
    - action == "run": resolves table path from in-memory input table definitions
    - any other action (sync actions): delegates to _download_table_from_storage_api()

    Critical regression guard: the "run" branch must match by
    Path(table.full_path).name (e.g. "email_basis.csv"), NOT by table.name
    (e.g. "email_basis" — the Keboola storage table name, which has no extension).
    Using table.name would silently return None for every lookup.
    """

    @contextmanager
    def _make_component(self, action: str):
        """Build a minimal Component with mocked configuration.action."""
        comp = Component.__new__(Component)

        table1 = MagicMock()
        table1.name = "email_basis"  # storage table name — no .csv extension
        table1.full_path = "/data/in/tables/email_basis.csv"

        table2 = MagicMock()
        table2.name = "email_export_dataset"  # storage table name — no .csv extension
        table2.full_path = "/data/in/tables/email_export.csv"

        mock_conf = MagicMock()
        mock_conf.action = action

        with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
            mock_conf_prop.return_value = mock_conf
            comp.get_input_tables_definitions = MagicMock(return_value=[table1, table2])
            comp._download_table_from_storage_api = MagicMock(return_value="/tmp/downloaded.csv")
            comp._mock_conf = mock_conf
            yield comp

    @pytest.mark.parametrize(
        "table_name, expected_path",
        [
            pytest.param("email_basis.csv", "/data/in/tables/email_basis.csv", id="first_table"),
            pytest.param("email_export.csv", "/data/in/tables/email_export.csv", id="second_table"),
        ],
    )
    def test_run_action_matches_by_filename(self, table_name, expected_path):
        """action=run: table is found by filename (Path(full_path).name), not by table.name.

        REGRESSION GUARD: if the code used `table.name == table_name` instead of
        `Path(table.full_path).name == table_name`, this test fails because
        table.name="email_basis" != "email_basis.csv".
        """
        with self._make_component("run") as comp:
            result = comp._return_table_path(table_name)
        assert result == expected_path

    def test_run_action_not_found_returns_none(self):
        """action=run: table name not in input tables → returns None (no exception)."""
        with self._make_component("run") as comp:
            result = comp._return_table_path("nonexistent.csv")
        assert result is None

    def test_sync_action_delegates_to_storage_api(self):
        """action=validate_config (any non-run): calls _download_table_from_storage_api, not get_input_tables_definitions."""
        with self._make_component("validate_config") as comp:
            result = comp._return_table_path("email_basis.csv")
        assert result == "/tmp/downloaded.csv"
        comp._download_table_from_storage_api.assert_called_once_with("email_basis.csv")
        comp.get_input_tables_definitions.assert_not_called()


# ==================== Tests for validate_single_table_() Sync Action ====================


class TestValidateSingleTableSyncAction:
    """Tests for the validate_single_table_() sync action helper method."""

    @pytest.fixture
    def base_config(self):
        """Base configuration for sync action tests."""
        return {
            "smtp_server_config": {
                "host": "smtp.example.com",
                "port": 587,
                "use_tls": True,
                "sender_email_address": "sender@example.com",
                "password": "#password",
            },
            "email_data_table_name": "email_basis.csv",
            "subject": "Test Subject",
            "plaintext_body": "Test body",
            "advanced_options": {
                "include_attachments": True,
                "attachments_config": {
                    "attachments_source": "single_table",
                    "source_table": None,
                    "include_csv_sample": False,
                    "include_snapshot_link": False,
                },
            },
        }

    def _run(self, base_config, mock_tables_mapping) -> object:
        """Build a minimal Component and run validate_single_table_(), returning the result."""
        comp = Component.__new__(Component)
        comp.cfg = Configuration.load_from_dict(base_config)
        mock_config = MagicMock()
        mock_config.parameters = base_config
        mock_config.tables_input_mapping = mock_tables_mapping
        with patch.object(type(comp), "configuration", new_callable=PropertyMock) as p:
            p.return_value = mock_config
            return comp.validate_single_table_()

    def test_missing_source_table(self, base_config, mock_tables_mapping):
        """Should return error when source_table is not set."""
        result = self._run(base_config, mock_tables_mapping)
        assert "❌ Source table must be specified for single table mode" in result.message
        assert result.type.name == "ERROR"

    def test_source_table_not_found(self, base_config, mock_tables_mapping):
        """Should return error when source_table doesn't exist in input tables."""
        base_config["advanced_options"]["attachments_config"]["source_table"] = "nonexistent.csv"
        base_config["advanced_options"]["attachments_config"]["include_csv_sample"] = True
        result = self._run(base_config, mock_tables_mapping)
        assert "❌" in result.message
        assert "nonexistent.csv" in result.message
        assert result.type.name == "ERROR"

    def test_neither_toggle_enabled(self, base_config, mock_tables_mapping):
        """Should return error when both toggles are False."""
        base_config["advanced_options"]["attachments_config"]["source_table"] = "email_basis.csv"
        result = self._run(base_config, mock_tables_mapping)
        assert "❌ At least one option must be enabled" in result.message
        assert result.type.name == "ERROR"

    def test_multiple_errors_shown_together(self, base_config, mock_tables_mapping):
        """Should show all errors at once (not fail-fast)."""
        # source_table=None AND both toggles False → two errors
        result = self._run(base_config, mock_tables_mapping)
        assert "❌ Source table must be specified for single table mode" in result.message
        assert "❌ At least one option must be enabled" in result.message
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize(
        "include_csv_sample, include_snapshot_link",
        [
            pytest.param(True, False, id="csv_only"),
            pytest.param(False, True, id="snapshot_only"),
            pytest.param(True, True, id="both"),
        ],
    )
    def test_valid_toggle_combinations(
        self, base_config, mock_tables_mapping, include_csv_sample, include_snapshot_link
    ):
        """Should succeed when source_table exists and at least one toggle is enabled."""
        base_config["advanced_options"]["attachments_config"]["source_table"] = "email_basis.csv"
        base_config["advanced_options"]["attachments_config"]["include_csv_sample"] = include_csv_sample
        base_config["advanced_options"]["attachments_config"]["include_snapshot_link"] = include_snapshot_link
        result = self._run(base_config, mock_tables_mapping)
        assert "✅" in result.message
        assert result.type.name == "SUCCESS"


# ==================== Tests for _parse_template_placeholders() ====================


class TestParseTemplatePlaceholders:
    @pytest.mark.parametrize(
        "template, expected",
        [
            pytest.param(None, set(), id="none"),
            pytest.param("", set(), id="empty_string"),
            pytest.param("Hello world", set(), id="no_placeholders"),
            pytest.param("Hello {{name}}", {"name"}, id="single_placeholder"),
            pytest.param("Hi {{name}}, your order {{order_id}} is ready", {"name", "order_id"}, id="multiple"),
            pytest.param("{{name}} and {{name}} again", {"name"}, id="duplicates_deduplicated"),
        ],
    )
    def test_parse_template_placeholders(self, template, expected):
        assert Component._parse_template_placeholders(template) == expected


# ==================== Tests for validate_subject_() ====================


class TestValidateSubject:
    # --- from_table ---

    def test_from_table_column_exists(self):
        """subject_source=from_table, column present in table → success."""
        config = make_advanced_config(
            advanced_options={"subject_config": {"subject_source": "from_table", "subject_column": "subject"}}
        )
        with make_component_for_sync(config, table_csv="recipient_email,subject\ntest@example.com,Hello\n") as comp:
            result = comp.validate_subject_()
        assert result.message == "✅ Subject column exists in the input table"
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("column_value", [None, ""], ids=["none", "empty_string"])
    def test_from_table_column_not_specified(self, column_value):
        """subject_source=from_table, column None or empty → not specified error."""
        config = make_advanced_config(
            advanced_options={"subject_config": {"subject_source": "from_table", "subject_column": column_value}}
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_subject_()
        assert result.message == "❌ Subject column is not specified"
        assert result.type.name == "ERROR"

    def test_from_table_column_missing_from_csv(self):
        """subject_source=from_table, column exists but cell templates reference missing columns → error."""
        config = make_advanced_config(
            advanced_options={"subject_config": {"subject_source": "from_table", "subject_column": "subject"}}
        )
        with make_component_for_sync(
            config, table_csv="recipient_email,subject\ntest@example.com,{{nonexistent}}\n"
        ) as comp:
            result = comp.validate_subject_()
        assert "❌" in result.message
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize("table_name", [None, ""], ids=["none", "empty_string"])
    def test_from_table_email_data_table_not_specified(self, table_name):
        """subject_source=from_table, email_data_table_name None or empty → table not specified error."""
        config = make_advanced_config(
            advanced_options={
                "email_data_table_name": table_name,
                "subject_config": {"subject_source": "from_table", "subject_column": "subject"},
            }
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_subject_()
        assert result.message == "❌ Email data table is not specified"
        assert result.type.name == "ERROR"

    def test_from_table_column_not_in_csv_headers(self):
        """subject_source=from_table, subject_column name absent from CSV headers → unhandled KeyError (bug).

        BUG: _get_missing_columns_from_table does row[column] without checking if the column exists
        as a header, so a misconfigured column name raises KeyError instead of returning a clean
        validation error. This test documents the current broken behavior.
        """
        config = make_advanced_config(
            advanced_options={"subject_config": {"subject_source": "from_table", "subject_column": "nonexistent_col"}}
        )
        with make_component_for_sync(config, table_csv="recipient_email,subject\ntest@example.com,Hello\n") as comp:
            with pytest.raises(KeyError, match="nonexistent_col"):
                comp.validate_subject_()

    # --- from_template_definition ---

    @pytest.mark.parametrize(
        "template",
        [
            pytest.param("Hello world", id="no_placeholders"),
            pytest.param(None, id="none"),
            pytest.param("", id="empty_string"),
        ],
    )
    def test_template_no_placeholders(self, template):
        """subject_source=from_template_definition, no placeholders in template → no placeholders message."""
        config = make_advanced_config(
            advanced_options={
                "subject_config": {
                    "subject_source": "from_template_definition",
                    "subject_template_definition": template,
                }
            }
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_subject_()
        assert result.message == "✅ Subject has no placeholders to validate"
        assert result.type.name == "SUCCESS"

    def test_template_with_valid_placeholders(self):
        """subject_source=from_template_definition, placeholders match columns → success."""
        config = make_advanced_config(
            advanced_options={
                "subject_config": {
                    "subject_source": "from_template_definition",
                    "subject_template_definition": "Hello {{name}}",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = comp.validate_subject_()
        assert result.message == "✅ All subject placeholders are present in the input table"
        assert result.type.name == "SUCCESS"

    def test_template_with_invalid_placeholders(self):
        """subject_source=from_template_definition, placeholder not in columns → missing columns error."""
        config = make_advanced_config(
            advanced_options={
                "subject_config": {
                    "subject_source": "from_template_definition",
                    "subject_template_definition": "Order {{order_id}} ready",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = comp.validate_subject_()
        assert result.type.name == "ERROR"
        assert "❌ Missing columns:" in result.message
        assert "order_id" in result.message

    def test_template_columns_not_accessible(self):
        """subject_source=from_template_definition, Storage API fails → cannot validate error."""
        from keboola.component.sync_actions import MessageType, ValidationResult

        config = make_advanced_config(
            advanced_options={
                "subject_config": {
                    "subject_source": "from_template_definition",
                    "subject_template_definition": "Hello {{name}}",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(
                return_value=ValidationResult("Couldn't fetch columns", MessageType.DANGER)
            )
            result = comp.validate_subject_()
        assert result.type.name == "ERROR"
        assert "❌ Cannot validate placeholders: email data table is not accessible" in result.message


# ==================== Tests for validate_plaintext_template_() / validate_html_template_() ====================


class TestValidateTemplate:
    """Tests for _validate_template() via validate_plaintext_template_() and validate_html_template_().

    All tests are parametrized over plaintext=True/False so the same scenarios are
    verified for both the plaintext and HTML template validators.
    """

    def _call(self, comp, plaintext: bool):
        return comp.validate_plaintext_template_() if plaintext else comp.validate_html_template_()

    def _col_key(self, plaintext: bool) -> str:
        return "plaintext_template_column" if plaintext else "html_template_column"

    def _def_key(self, plaintext: bool) -> str:
        return "plaintext_template_definition" if plaintext else "html_template_definition"

    def _file_key(self, plaintext: bool) -> str:
        return "plaintext_template_filename" if plaintext else "html_template_filename"

    # --- from_table ---

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_from_table_column_exists(self, plaintext):
        """message_body_source=from_table, template column present → success."""
        expected = (
            "✅ Plaintext template column exists in the input table"
            if plaintext
            else "✅ HTML template column exists in the input table"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_table",
                    "use_html_template": not plaintext,
                    self._col_key(plaintext): "body_col",
                }
            }
        )
        with make_component_for_sync(config, table_csv="recipient_email,body_col\ntest@example.com,Hello\n") as comp:
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("plaintext", [True, False])
    @pytest.mark.parametrize("column_value", [None, ""], ids=["none", "empty_string"])
    def test_from_table_column_not_specified(self, plaintext, column_value):
        """message_body_source=from_table, column None or empty → not specified error."""
        expected = (
            "❌ Plaintext template column is not specified" if plaintext else "❌ HTML template column is not specified"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_table",
                    "use_html_template": not plaintext,
                    self._col_key(plaintext): column_value,
                }
            }
        )
        with make_component_for_sync(config, table_csv="recipient_email,body_col\ntest@example.com,Hello\n") as comp:
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize("plaintext", [True, False])
    @pytest.mark.parametrize("table_name", [None, ""], ids=["none", "empty_string"])
    def test_from_table_email_data_table_not_specified(self, plaintext, table_name):
        """message_body_source=from_table, email_data_table_name None or empty → table not specified error."""
        config = make_advanced_config(
            advanced_options={
                "email_data_table_name": table_name,
                "message_body_config": {
                    "message_body_source": "from_table",
                    "use_html_template": not plaintext,
                    self._col_key(plaintext): "body_col",
                },
            }
        )
        with make_component_for_sync(config) as comp:
            result = self._call(comp, plaintext)
        assert result.message == "❌ Email data table is not specified"
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_from_table_column_not_in_csv_headers(self, plaintext):
        """message_body_source=from_table, column name absent from CSV headers → unhandled KeyError (bug).

        BUG: same root cause as TestValidateSubject.test_from_table_column_not_in_csv_headers —
        _get_missing_columns_from_table does row[column] without checking fieldnames first.
        """
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_table",
                    "use_html_template": not plaintext,
                    self._col_key(plaintext): "nonexistent_col",
                }
            }
        )
        with make_component_for_sync(config, table_csv="recipient_email,body\ntest@example.com,Hello\n") as comp:
            with pytest.raises(KeyError, match="nonexistent_col"):
                self._call(comp, plaintext)

    # --- from_template_definition ---

    @pytest.mark.parametrize("plaintext", [True, False])
    @pytest.mark.parametrize(
        "template",
        [
            pytest.param("Hello world, no placeholders here.", id="no_placeholders"),
            pytest.param(None, id="none"),
        ],
    )
    def test_definition_no_placeholders(self, plaintext, template):
        """message_body_source=from_template_definition, no placeholders in template → no placeholders message."""
        expected = (
            "✅ Plaintext template has no placeholders to validate"
            if plaintext
            else "✅ HTML template has no placeholders to validate"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_definition",
                    "use_html_template": not plaintext,
                    self._def_key(plaintext): template,
                }
            }
        )
        with make_component_for_sync(config) as comp:
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_definition_with_valid_placeholders(self, plaintext):
        """message_body_source=from_template_definition, placeholders match columns → success."""
        expected = (
            "✅ All plaintext template placeholders are present in the input table"
            if plaintext
            else "✅ All HTML template placeholders are present in the input table"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_definition",
                    "use_html_template": not plaintext,
                    self._def_key(plaintext): "Hello {{name}}",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_definition_with_invalid_placeholders(self, plaintext):
        """message_body_source=from_template_definition, placeholder not in columns → missing columns error."""
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_definition",
                    "use_html_template": not plaintext,
                    self._def_key(plaintext): "Hello {{nonexistent}}",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = self._call(comp, plaintext)
        assert result.type.name == "ERROR"
        assert "❌ Missing columns:" in result.message
        assert "nonexistent" in result.message

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_definition_columns_not_accessible(self, plaintext):
        """message_body_source=from_template_definition, Storage API fails → cannot validate error."""
        from keboola.component.sync_actions import MessageType, ValidationResult

        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_definition",
                    "use_html_template": not plaintext,
                    self._def_key(plaintext): "Hello {{name}}",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            comp._load_table_columns = MagicMock(
                return_value=ValidationResult("Couldn't fetch columns", MessageType.DANGER)
            )
            result = self._call(comp, plaintext)
        assert result.type.name == "ERROR"
        assert "❌ Cannot validate placeholders: email data table is not accessible" in result.message

    # --- from_template_file ---

    @pytest.mark.parametrize("plaintext", [True, False])
    @pytest.mark.parametrize("filename", [None, ""], ids=["none", "empty_string"])
    def test_file_filename_not_specified(self, plaintext, filename):
        """message_body_source=from_template_file, filename None or empty → not specified error."""
        expected = (
            "❌ Plaintext template filename is not specified"
            if plaintext
            else "❌ HTML template filename is not specified"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_file",
                    "use_html_template": not plaintext,
                    self._file_key(plaintext): filename,
                }
            }
        )
        with make_component_for_sync(config) as comp:
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_file_no_placeholders(self, plaintext):
        """message_body_source=from_template_file, file has no placeholders → no placeholders message."""
        expected = (
            "✅ Plaintext template has no placeholders to validate"
            if plaintext
            else "✅ HTML template has no placeholders to validate"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_file",
                    "use_html_template": not plaintext,
                    self._file_key(plaintext): "template.txt",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            _mock_template_file(comp, "Hello world, no placeholders.")
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_file_with_valid_placeholders(self, plaintext):
        """message_body_source=from_template_file, file has valid placeholders → success."""
        expected = (
            "✅ All plaintext template placeholders are present in the input table"
            if plaintext
            else "✅ All HTML template placeholders are present in the input table"
        )
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_file",
                    "use_html_template": not plaintext,
                    self._file_key(plaintext): "template.txt",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            _mock_template_file(comp, "Hello {{name}}")
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = self._call(comp, plaintext)
        assert result.message == expected
        assert result.type.name == "SUCCESS"

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_file_with_invalid_placeholders(self, plaintext):
        """message_body_source=from_template_file, file has placeholder not in columns → error."""
        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_file",
                    "use_html_template": not plaintext,
                    self._file_key(plaintext): "template.txt",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            _mock_template_file(comp, "Hello {{nonexistent}}")
            comp._load_table_columns = MagicMock(return_value=["name", "email"])
            result = self._call(comp, plaintext)
        assert result.type.name == "ERROR"
        assert "❌ Missing columns:" in result.message
        assert "nonexistent" in result.message

    @pytest.mark.parametrize("plaintext", [True, False])
    def test_file_columns_not_accessible(self, plaintext):
        """message_body_source=from_template_file, Storage API fails for columns → cannot validate error."""
        from keboola.component.sync_actions import MessageType, ValidationResult

        config = make_advanced_config(
            advanced_options={
                "message_body_config": {
                    "message_body_source": "from_template_file",
                    "use_html_template": not plaintext,
                    self._file_key(plaintext): "template.txt",
                }
            }
        )
        with make_component_for_sync(config) as comp:
            _mock_template_file(comp, "Hello {{name}}")
            comp._load_table_columns = MagicMock(
                return_value=ValidationResult("Couldn't fetch columns", MessageType.DANGER)
            )
            result = self._call(comp, plaintext)
        assert result.type.name == "ERROR"
        assert "❌ Cannot validate placeholders: email data table is not accessible" in result.message


# ==================== Tests for validate_attachments_() ====================


class TestValidateAttachments:
    @pytest.mark.parametrize("column_value", [None, ""], ids=["none", "empty_string"])
    def test_from_table_column_not_specified(self, column_value):
        """attachments_source=from_table, column None or empty → not specified error."""
        config = make_advanced_config(
            advanced_options={
                "attachments_config": {"attachments_source": "from_table", "attachments_column": column_value}
            }
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_attachments_()
        assert result.message == "❌ Attachments column is not specified"
        assert result.type.name == "ERROR"

    @pytest.mark.parametrize("table_name", [None, ""], ids=["none", "empty_string"])
    def test_from_table_email_data_table_not_specified(self, table_name):
        """attachments_source=from_table, email_data_table_name None or empty → table not specified error."""
        config = make_advanced_config(
            advanced_options={
                "email_data_table_name": table_name,
                "attachments_config": {"attachments_source": "from_table", "attachments_column": "attachments"},
            }
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_attachments_()
        assert result.message == "❌ Email data table is not specified"
        assert result.type.name == "ERROR"

    def test_from_table_invalid_json(self):
        """attachments_source=from_table, column contains non-JSON data → clear error."""
        config = make_advanced_config(
            advanced_options={
                "attachments_config": {"attachments_source": "from_table", "attachments_column": "recipient_email"}
            }
        )
        with make_component_for_sync(config, table_csv="recipient_email,body\ntest@example.com,Hello\n") as comp:
            comp._list_files_in_sync_actions = MagicMock(return_value=[])
            result = comp.validate_attachments_()
        assert "❌" in result.message
        assert "recipient_email" in result.message
        assert "valid JSON" in result.message
        assert result.type.name == "ERROR"

    def test_from_table_valid(self):
        """attachments_source=from_table, valid JSON column with matching files → success."""
        config = make_advanced_config(
            advanced_options={
                "attachments_config": {"attachments_source": "from_table", "attachments_column": "attachments"}
            }
        )
        csv = 'recipient_email,attachments\ntest@example.com,"[""report.pdf""]"\n'
        with make_component_for_sync(config, table_csv=csv) as comp:
            comp._list_files_in_sync_actions = MagicMock(return_value=[{"name": "report.pdf", "id": 1}])
            result = comp.validate_attachments_()
        assert result.message == "✅ All attachments are present"
        assert result.type.name == "SUCCESS"

    def test_from_table_missing_attachments(self):
        """attachments_source=from_table, JSON column references files not in Storage → missing attachments error."""
        config = make_advanced_config(
            advanced_options={
                "attachments_config": {"attachments_source": "from_table", "attachments_column": "attachments"}
            }
        )
        csv = 'recipient_email,attachments\ntest@example.com,"[""missing.pdf""]"\n'
        with make_component_for_sync(config, table_csv=csv) as comp:
            comp._list_files_in_sync_actions = MagicMock(return_value=[])
            result = comp.validate_attachments_()
        assert result.type.name == "ERROR"
        assert "❌ Missing attachments:" in result.message
        assert "missing.pdf" in result.message

    def test_all_input_files_skips_validation(self):
        """attachments_source=all_input_files → from_table branch skipped, returns success."""
        config = make_advanced_config(
            advanced_options={"attachments_config": {"attachments_source": "all_input_files"}}
        )
        with make_component_for_sync(config) as comp:
            result = comp.validate_attachments_()
        assert result.message == "✅ All attachments are present"
        assert result.type.name == "SUCCESS"


# ==================== Tests for validate_config() ====================


class TestValidateConfig:
    """Tests for the validate_config() sync action method."""

    @pytest.mark.parametrize("column_value", [None, ""], ids=["none", "empty_string"])
    def test_recipient_column_not_specified(self, column_value):
        """recipient_email_address_column None or empty → early return with error."""
        config = make_advanced_config(advanced_options={"recipient_email_address_column": column_value})
        with make_component_for_sync(config) as comp:
            result = comp.validate_config()
        assert result.message == "❌ Recipient email address column is not specified"
        assert result.type.name == "ERROR"

    def test_full_validation_all_pass(self):
        """All three base sub-validators SUCCESS → aggregated SUCCESS with messages joined by double newline."""
        config = make_advanced_config(advanced_options={"include_attachments": False})
        with make_component_for_validate_config(config) as comp:
            result = comp.validate_config()

        assert result.type.name == "SUCCESS"
        comp.test_smtp_server_connection_.assert_called_once()
        comp.validate_subject_.assert_called_once()
        comp.validate_plaintext_template_.assert_called_once()
        assert result.message == "\n\n".join(["✅ OK", "✅ OK", "✅ OK"])

    def test_full_validation_one_sub_validator_fails(self):
        """One sub-validator returns DANGER → aggregated result is DANGER."""
        config = make_advanced_config(advanced_options={"include_attachments": False})
        with make_component_for_validate_config(config) as comp:
            comp.validate_subject_ = MagicMock(return_value=_make_danger("❌ Subject column is not specified"))
            result = comp.validate_config()

        assert result.type.name == "ERROR"
        assert "❌ Subject column is not specified" in result.message

    def test_html_template_enabled_adds_html_validator(self):
        """use_html_template=True → validate_html_template_ included in chain (4 validators total)."""
        config = make_advanced_config(
            advanced_options={
                "include_attachments": False,
                "message_body_config": {
                    "message_body_source": "from_template_definition",
                    "use_html_template": True,
                    "plaintext_template_definition": "Hello",
                    "html_template_definition": "Hello HTML",
                },
            }
        )
        with make_component_for_validate_config(config) as comp:
            result = comp.validate_config()

        assert result.type.name == "SUCCESS"
        comp.validate_html_template_.assert_called_once()
        assert result.message.count("✅ OK") == 4

    def test_html_template_disabled_skips_html_validator(self):
        """use_html_template=False → validate_html_template_ not called."""
        config = make_advanced_config(advanced_options={"include_attachments": False})
        with make_component_for_validate_config(config) as comp:
            result = comp.validate_config()

        comp.validate_html_template_.assert_not_called()
        assert result.type.name == "SUCCESS"

    def test_unknown_attachments_source_early_return(self):
        """attachments_source is unrecognized → early return DANGER before validators run."""
        config = make_advanced_config(
            advanced_options={
                "include_attachments": True,
                "attachments_config": {"attachments_source": "data_preview"},
            }
        )
        with make_component_for_validate_config(config) as comp:
            result = comp.validate_config()

        assert result.type.name == "ERROR"
        assert "❌ Unknown attachment source: 'data_preview'" in result.message
        comp.validate_attachments_.assert_not_called()
        comp.validate_single_table_.assert_not_called()

    @pytest.mark.parametrize(
        "advanced_options, image_parameters, expect_attachments_called, expect_single_table_called",
        [
            pytest.param(
                {"include_attachments": False},
                None,
                False,
                False,
                id="include_attachments=False",
            ),
            pytest.param(
                {
                    "include_attachments": True,
                    "attachments_config": {"attachments_source": "from_table", "attachments_column": "attachments"},
                },
                {"disable_attachments": True},
                False,
                False,
                id="disable_attachments_stack_override",
            ),
            pytest.param(
                {
                    "include_attachments": True,
                    "attachments_config": {
                        "attachments_source": "single_table",
                        "source_table": "email_basis.csv",
                        "include_csv_sample": True,
                    },
                },
                None,
                False,
                True,
                id="source=single_table",
            ),
            pytest.param(
                {
                    "include_attachments": True,
                    "attachments_config": {"attachments_source": "from_table", "attachments_column": "attachments"},
                },
                None,
                True,
                False,
                id="source=from_table",
            ),
            pytest.param(
                {
                    "include_attachments": True,
                    "attachments_config": {"attachments_source": "all_input_files"},
                },
                None,
                False,
                False,
                id="source=all_input_files",
            ),
        ],
    )
    def test_attachment_validator_routing(
        self, advanced_options, image_parameters, expect_attachments_called, expect_single_table_called
    ):
        """Correct attachment validators included/excluded based on config and stack overrides."""
        config = make_advanced_config(advanced_options=advanced_options)
        with make_component_for_validate_config(config, image_parameters=image_parameters) as comp:
            comp.validate_config()

        if expect_attachments_called:
            comp.validate_attachments_.assert_called_once()
        else:
            comp.validate_attachments_.assert_not_called()

        if expect_single_table_called:
            comp.validate_single_table_.assert_called_once()
        else:
            comp.validate_single_table_.assert_not_called()
