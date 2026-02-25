"""
Comprehensive test suite for configuration validation in Component.

Tests cover:
- _validate_run_configuration() method (to be implemented)
- _resolve_data_source_table_path() method
- load_email_data_table_path() method
- _load_attachment_tables() method
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration

# ==================== Fixtures ====================


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
    with patch("component.ComponentBase.__init__", return_value=None):
        comp = Component()

        # Set up valid default configuration
        comp.cfg = Configuration.load_from_dict(make_advanced_config())

        # Mock configuration property (read-only, needs PropertyMock)
        mock_config = MagicMock()
        mock_config.tables_input_mapping = mock_tables_mapping
        mock_config.parameters = make_advanced_config()

        with patch.object(type(comp), "configuration", new_callable=PropertyMock) as mock_conf_prop:
            mock_conf_prop.return_value = mock_config
            comp._configuration_mock = mock_conf_prop  # Keep reference

            # Mock environment variables
            comp.environment_variables = mock_environment

            # Mock get_input_tables_definitions()
            comp.get_input_tables_definitions = MagicMock(return_value=mock_input_tables)

            # Mock other required methods
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
            "include_custom_link": False,
            "custom_link_text": "Storage Link",
            "custom_link_url": "",
            "custom_link_table": None,
            "include_attachments": True,
            "attachments_config": {
                "attachments_source": "all_input_files",
            },
        },
        "continue_on_error": True,
        "dry_run": False,
    }

    # Apply overrides
    _deep_update(config, overrides)
    return config


def _deep_update(base_dict, update_dict):
    """Recursively update nested dictionary."""
    for key, value in update_dict.items():
        if isinstance(value, dict) and key in base_dict and isinstance(base_dict[key], dict):
            _deep_update(base_dict[key], value)
        else:
            base_dict[key] = value


# ==================== Tests for _validate_run_configuration() ====================


class TestValidateRunConfiguration:
    """Tests for the _validate_run_configuration() method."""

    def test_basic_mode_skips_validation(self, component):
        """Basic mode should skip all validation."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                configuration_type="basic",
            )
        )
        # Should not raise
        component._validate_run_configuration()

    # Data Preview Validation

    def test_data_preview_missing_table(self, component):
        """Data preview mode without table specified should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "data_preview",
                        "data_preview_table": None,
                    }
                }
            )
        )
        with pytest.raises(UserException, match="Data preview table must be specified"):
            component._validate_run_configuration()

    def test_data_preview_table_not_in_input_tables(self, component):
        """Data preview table not found in input tables should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "data_preview",
                        "data_preview_table": "nonexistent.csv",
                    }
                }
            )
        )
        with pytest.raises(UserException, match="not found in input tables"):
            component._validate_run_configuration()

    def test_data_preview_valid(self, component):
        """Valid data preview configuration should not raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "attachments_config": {
                        "attachments_source": "data_preview",
                        "data_preview_table": "email_export.csv",
                    }
                }
            )
        )
        # Should not raise
        component._validate_run_configuration()

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
        # Should not raise even without data_preview_table
        component._validate_run_configuration()

    # Custom Link Validation

    def test_custom_link_disabled_skips_validation(self, component):
        """Custom link disabled should skip validation."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(advanced_options={"include_custom_link": False})
        )
        # Should not raise
        component._validate_run_configuration()

    def test_custom_link_table_id_placeholder_without_table(self, component):
        """Custom link with {table_id} but no table specified should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}/table/{table_id}",
                    "custom_link_table": None,
                }
            )
        )
        with pytest.raises(UserException, match="Custom link table must be specified.*{table_id}"):
            component._validate_run_configuration()

    def test_custom_link_table_not_in_mapping(self, component):
        """Custom link table not found in input mapping should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}/table/{table_id}",
                    "custom_link_table": "nonexistent.csv",
                }
            )
        )
        with pytest.raises(UserException, match="not found in input mapping"):
            component._validate_run_configuration()

    @pytest.mark.parametrize("missing_var", ["stack_id", "project_id", "run_id"])
    def test_custom_link_missing_env_var(self, component, missing_var):
        """Custom link with missing environment variable should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}",
                }
            )
        )
        setattr(component.environment_variables, missing_var, "")
        with pytest.raises(UserException, match=f"{missing_var.replace('_', ' ').title()}.*not available"):
            component._validate_run_configuration()

    def test_custom_link_valid_with_table_id(self, component):
        """Valid custom link with {table_id} placeholder should not raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}/table/{table_id}",
                    "custom_link_table": "email_export.csv",
                }
            )
        )
        # Should not raise
        component._validate_run_configuration()

    def test_custom_link_valid_without_table_id(self, component):
        """Valid custom link without {table_id} placeholder should not raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}/overview",
                }
            )
        )
        # Should not raise (no custom_link_table needed)
        component._validate_run_configuration()

    def test_custom_link_static_url(self, component):
        """Custom link with static URL (no placeholders) should not raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://example.com/static-page",
                }
            )
        )
        # Should not raise
        component._validate_run_configuration()

    # Combined Scenarios

    def test_both_features_valid(self, component):
        """Both data preview and custom link enabled with valid config should not raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}",
                    "attachments_config": {
                        "attachments_source": "data_preview",
                        "data_preview_table": "email_export.csv",
                    },
                }
            )
        )
        # Should not raise
        component._validate_run_configuration()

    def test_data_preview_valid_custom_link_invalid(self, component):
        """Data preview valid but custom link invalid should raise."""
        component.cfg = Configuration.load_from_dict(
            make_advanced_config(
                advanced_options={
                    "include_custom_link": True,
                    "custom_link_url": "https://{stack}/admin/projects/{project_id}/table/{table_id}",
                    "custom_link_table": "nonexistent.csv",  # Invalid
                    "attachments_config": {
                        "attachments_source": "data_preview",
                        "data_preview_table": "email_export.csv",  # Valid
                    },
                }
            )
        )
        with pytest.raises(UserException, match="not found in input mapping"):
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
        # Verify the values are the full paths
        assert result["email_basis.csv"] == "/data/in/tables/email_basis.csv"
