import csv
import json
import logging
import os
import re
import time
from io import StringIO
from pathlib import Path
from typing import Dict, List, Set, Tuple, Union

from jinja2 import Template
from kbcstorage.client import Client as StorageClient
from kbcstorage.tables import Tables as StorageTables
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import FileDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult

from client import SMTPClient
from configuration import Configuration, ConnectionConfig
from stack_overrides import StackOverridesParameters

KEY_ALLOWED_SENDER_EMAIL_ADDRESSES = "allowed_sender_email_addresses"

KEY_PLAINTEXT_TEMPLATE_COLUMN = "plaintext_template_column"
KEY_HTML_TEMPLATE_COLUMN = "html_template_column"
KEY_PLAINTEXT_TEMPLATE_FILENAME = "plaintext_template_filename"
KEY_HTML_TEMPLATE_FILENAME = "html_template_filename"
KEY_PLAINTEXT_TEMPLATE_DEFINITION = "plaintext_template_definition"
KEY_HTML_TEMPLATE_DEFINITION = "html_template_definition"

# STACK OVERRIDES
KEY_ALLOWED_HOSTS = "allowed_hosts"
KEY_ADDRESS_WHITELIST = "address_whitelist"
KEY_DISABLE_ATTACHMENTS = "disable_attachments"

SLEEP_INTERVAL = 0.1

RESULT_TABLE_COLUMNS = (
    "status",
    "recipient_email_address",
    "sender_email_address",
    "subject",
    "plaintext_message_body",
    "html_message_body",
    "attachment_filenames",
    "error_message",
)

VALID_CONNECTION_CONFIG_MESSAGE = "✅ Connection configuration is valid"
VALID_SUBJECT_MESSAGE = "✅ All subject placeholders are present in the input table"
VALID_PLAINTEXT_TEMPLATE_MESSAGE = "✅ All plaintext template placeholders are present in the input table"
VALID_HTML_TEMPLATE_MESSAGE = "✅ All HTML template placeholders are present in the input table"
VALID_ATTACHMENTS_MESSAGE = "✅ All attachments are present"

general_error_row = {
    "status": "ERROR",
    "recipient_email_address": "",
    "sender_email_address": "",
    "subject": "",
    "plaintext_message_body": "",
    "html_message_body": "",
    "attachment_filenames": "",
}


class Component(ComponentBase):
    """Component for sending emails"""

    def __init__(self):
        super().__init__()
        self._init_configuration()
        self._client: SMTPClient = None
        self._results_writer = None
        self.plaintext_template_path = None
        self.html_template_path = None

    def run(self):
        self._validate_run_configuration()
        self.init_client()

        if self.cfg.configuration_type == "advanced":
            validation_results = self.validate_config()
            if validation_results.type == MessageType.DANGER:
                raise UserException(validation_results.message)

        in_tables = self.get_input_tables_definitions()
        in_files_by_name = self.get_input_file_definitions_grouped_by_name()
        email_data_table_name = self.cfg.advanced_options.email_data_table_name
        email_data_table_path = self.load_email_data_table_path(in_tables, email_data_table_name)
        self.plaintext_template_path, self.html_template_path = self._extract_template_files_full_paths(
            in_files_by_name
        )

        try:
            attachments_paths_by_filename = self.load_attachment_paths_by_filename(
                in_tables, email_data_table_name, in_files_by_name
            )
        except Exception as e:
            raise UserException(f"Error loading attachments: {str(e)}")

        # Handle single table mode: CSV sample and/or snapshot link
        sample_metadata = None
        snapshot_link = None
        if (
            self.cfg.configuration_type == "advanced"
            and self.cfg.advanced_options.include_attachments
            and self.cfg.advanced_options.attachments_config.attachments_source == "single_table"
        ):
            attachments_config = self.cfg.advanced_options.attachments_config
            source_table = attachments_config.source_table

            # Resolve table path and table_id from input mapping (validation already done in _validate_run_configuration)
            table_path = self._resolve_data_source_table_path(source_table)
            table_id = next(
                table.source for table in self.configuration.tables_input_mapping if table.destination == source_table
            )

            # Initialize storage client once for both CSV sample and snapshot link
            storage_client = self._init_storage_client()

            # Handle CSV sample generation
            if attachments_config.include_csv_sample:
                # Get total row count - prefer Storage API, fallback to local CSV
                try:
                    table_detail = storage_client.tables.detail(table_id)
                    total_row_count = table_detail["rowsCount"]
                    logging.info(f"Table sample: using Storage API row count ({total_row_count} rows)")
                except Exception as e:
                    logging.warning(
                        f"Table sample: Storage API unavailable ({str(e)}), "
                        f"falling back to local CSV row count (may be inaccurate if input mapping uses limit/filters)"
                    )
                    total_row_count = self._count_csv_rows(table_path)

                # Generate sample CSV
                sample_file_path, actual_row_count = self._generate_table_sample(
                    table_path=table_path,
                    row_limit=attachments_config.sample_row_limit,
                    filename_template=attachments_config.sample_attachment_filename,
                    table_name=source_table,
                    sort_enabled=attachments_config.sample_sort_enabled,
                    sort_column=attachments_config.sample_sort_column,
                    sort_order=attachments_config.sample_sort_order,
                )

                # Override attachments with sample file only
                attachments_paths_by_filename = {os.path.basename(sample_file_path): sample_file_path}

                # Store sample metadata for send_emails
                sample_metadata = {
                    "info_text": attachments_config.sample_info_text,
                    "row_count": actual_row_count,
                    "total_count": total_row_count,
                }

            # Handle snapshot link - upload table to File Storage
            if attachments_config.include_snapshot_link:
                try:
                    run_id = self.environment_variables.run_id or "unknown"

                    file_id = storage_client.files.upload_file(
                        file_path=table_path,
                        tags=["data-snapshot", f"table:{table_id}", f"runId:{run_id}"],
                        is_permanent=False,
                        compress=True,
                    )

                    stack_id = self.environment_variables.stack_id or "STACK_ID"
                    project_id = self.environment_variables.project_id or "PROJECT_ID"
                    resolved_url = f"https://{stack_id}/admin/projects/{project_id}/storage/files?q=id%3A{file_id}"
                    snapshot_link = {"url": resolved_url, "text": "View table data snapshot in Storage"}

                    logging.info(f"Uploaded data snapshot to File Storage (file_id={file_id}, table={table_id})")
                except Exception as e:
                    error_msg = f"Failed to upload data snapshot to File Storage: {e}"
                    if self.cfg.continue_on_error:
                        logging.warning(f"{error_msg}. Snapshot link will not be included.")
                    else:
                        raise UserException(error_msg)

        results_table = self.create_out_table_definition("results.csv", write_always=True)
        with open(results_table.full_path, "w", newline="") as output_file:
            self._results_writer = csv.DictWriter(output_file, fieldnames=RESULT_TABLE_COLUMNS)
            self._results_writer.writeheader()
            self._results_writer.errors = False
            self.send_emails(
                email_data_table_path=email_data_table_path,
                attachments_paths_by_filename=attachments_paths_by_filename,
                sample_metadata=sample_metadata,
                snapshot_link=snapshot_link,
            )
        self.write_manifest(results_table)

        if self._results_writer.errors:
            raise UserException("Some emails couldn't be sent - check results.csv for more details.")

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self.cfg: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def _validate_run_configuration(self) -> None:
        """
        Validate configuration for single table mode (CSV sample and snapshot link features).
        Should be called after configuration is loaded and before actual work begins.
        Skips validation for basic mode.
        """
        # Skip validation in basic mode
        if self.cfg.configuration_type == "basic":
            return

        # Validate attachment source is a recognized value (early check)
        if self.cfg.advanced_options.include_attachments:
            attachments_source = self.cfg.advanced_options.attachments_config.attachments_source
            if attachments_source not in ("from_table", "single_table", "all_input_files"):
                raise UserException(
                    f"Unknown attachment source: '{attachments_source}'. "
                    f"Valid options: 'from_table', 'single_table', 'all_input_files'"
                )

        # Validate single table mode configuration
        if (
            self.cfg.advanced_options.include_attachments
            and self.cfg.advanced_options.attachments_config.attachments_source == "single_table"
        ):
            attachments_config = self.cfg.advanced_options.attachments_config
            source_table = attachments_config.source_table

            # Source table must be specified
            if not source_table:
                raise UserException("Source table must be specified for single table mode")

            # Verify table exists in input tables
            try:
                self._resolve_data_source_table_path(source_table)
            except UserException:
                raise  # Re-raise with original message

            # At least one feature (CSV sample or snapshot link) must be enabled
            if not attachments_config.include_csv_sample and not attachments_config.include_snapshot_link:
                raise UserException(
                    "At least one option must be enabled: 'Include CSV Sample' or 'Include Link to Table Snapshot'"
                )

    def _load_stack_overrides(self) -> StackOverridesParameters:
        image_parameters = self.configuration.image_parameters or {}

        allowed_hosts = image_parameters.get(KEY_ALLOWED_HOSTS, [])
        allowed_sender_email_addresses = image_parameters.get(KEY_ALLOWED_SENDER_EMAIL_ADDRESSES, [])
        address_whitelist = image_parameters.get(KEY_ADDRESS_WHITELIST, [])
        disable_attachments = image_parameters.get(KEY_DISABLE_ATTACHMENTS, False)

        return StackOverridesParameters(
            allowed_hosts=allowed_hosts,
            address_whitelist=address_whitelist,
            disable_attachments=disable_attachments,
            allowed_sender_email_addresses=allowed_sender_email_addresses,
        )

    def init_client(self, connection_config: Union[ConnectionConfig, None] = None) -> None:
        if connection_config is None:
            connection_config = self.cfg.connection_config

        proxy_server_config = connection_config.creds_config.proxy_server_config
        oauth_config = connection_config.oauth_config
        creds_config = connection_config.creds_config

        overrides: StackOverridesParameters = self._load_stack_overrides()
        self.validate_allowed_hosts(overrides, creds_config)
        self.validate_allowed_sender_email_addresses(overrides, creds_config)

        self._client = SMTPClient(
            use_oauth=connection_config.use_oauth,
            sender_email_address=creds_config.sender_email_address or oauth_config.sender_email_address,
            password=creds_config.pswd_sender_password,
            server_host=creds_config.server_host,
            server_port=creds_config.server_port,
            connection_protocol=creds_config.connection_protocol,
            proxy_server_host=proxy_server_config.proxy_server_host,
            proxy_server_port=proxy_server_config.proxy_server_port,
            proxy_server_username=proxy_server_config.proxy_server_username,
            proxy_server_password=proxy_server_config.pswd_proxy_server_password,
            tenant_id=oauth_config.tenant_id,
            client_id=oauth_config.client_id,
            client_secret=oauth_config.pswd_client_secret,
            address_whitelist=overrides.address_whitelist,
            disable_attachments=overrides.disable_attachments,
            without_login=creds_config.without_login,
        )

        self._client.init_smtp_server()

    @staticmethod
    def validate_allowed_sender_email_addresses(overrides, creds_config):
        if overrides.allowed_sender_email_addresses:
            if not creds_config.sender_email_address:
                raise UserException("Sender email address is not set in the configuration")
            if creds_config.sender_email_address not in overrides.allowed_sender_email_addresses:
                raise UserException(
                    f"Sender email address {creds_config.sender_email_address} is not allowed for your stack"
                )

    @staticmethod
    def validate_allowed_hosts(overrides: StackOverridesParameters, creds_config) -> None:
        if overrides.allowed_hosts:
            match = False
            for item in overrides.allowed_hosts:
                if item.get("host") == creds_config.server_host and item.get("port") == creds_config.server_port:
                    match = True

            if not match:
                raise UserException(f"Host {creds_config.server_host}:{creds_config.server_port} is not allowed")

    @staticmethod
    def load_email_data_table_path(in_tables, email_data_table_name):
        try:
            table_path = next(
                in_table.full_path for in_table in in_tables if Path(in_table.full_path).name == email_data_table_name
            )
        except StopIteration:
            table_path = None
        return table_path

    @staticmethod
    def _load_attachment_tables(in_tables, table_to_exclude):
        tables = {
            Path(in_table.full_path).name: in_table.full_path
            for in_table in in_tables
            if Path(in_table.full_path).name != table_to_exclude
        }
        return tables

    def _load_attachment_files(self, in_files_by_name):
        attachment_files = {}
        for name, files in in_files_by_name.items():
            file = files[0]
            original_path = file.full_path
            if original_path not in [self.plaintext_template_path, self.html_template_path]:
                directory = os.path.split(original_path)[0]
                new_path = os.path.join(directory, file.name)
                Path.rename(original_path, new_path)
                attachment_files[file.name] = new_path
        return attachment_files

    def load_attachment_paths_by_filename(self, in_tables, email_data_table_name, in_files_by_name):
        if self.cfg.configuration_type == "basic" and not self.cfg.basic_options.include_attachments:
            return {}
        table_attachments_paths_by_filename = self._load_attachment_tables(in_tables, email_data_table_name)
        file_attachments_paths_by_filename = self._load_attachment_files(in_files_by_name)
        return {**table_attachments_paths_by_filename, **file_attachments_paths_by_filename}

    def send_emails(
        self,
        attachments_paths_by_filename: Dict[str, str],
        email_data_table_path: Union[str, None] = None,
        sample_metadata: Union[Dict, None] = None,
        snapshot_link: Union[Dict, None] = None,
    ) -> None:
        continue_on_error = self.cfg.continue_on_error
        dry_run = self.cfg.dry_run
        use_advanced_options = self.cfg.configuration_type == "advanced"
        basic_options = self.cfg.basic_options
        advanced_options = self.cfg.advanced_options
        subject_config = advanced_options.subject_config
        message_body_config = advanced_options.message_body_config
        attachments_config = advanced_options.attachments_config
        use_html_template = message_body_config.use_html_template
        subject_column = None
        plaintext_template_column = None
        html_template_column = None
        attachments_column = attachments_config.attachments_column

        if email_data_table_path:
            in_table = open(email_data_table_path)
            reader = csv.DictReader(in_table)
            columns = set(reader.fieldnames)

            if subject_config.subject_source == "from_table":
                subject_column = subject_config.subject_column
            else:
                subject_template_text = subject_config.subject_template_definition
                self._validate_template_text(subject_template_text, columns)

            if message_body_config.message_body_source == "from_table":
                plaintext_template_column = message_body_config.plaintext_template_column
                if use_html_template:
                    html_template_column = message_body_config.html_template_column
            else:
                plaintext_template_text = self._read_template_text()
                self._validate_template_text(plaintext_template_text, columns)
                if use_html_template:
                    html_template_text = self._read_template_text(plaintext=False)
                    self._validate_template_text(html_template_text, columns)
        else:
            try:
                reader = iter(basic_options.recipient_email_addresses.split(","))
            except AttributeError:
                raise UserException("No input table found with specified name or no recipient email addresses provided")

        for row in reader:
            try:
                recipient_email_address = row
                if isinstance(reader, csv.DictReader):
                    recipient_email_address = row[advanced_options.recipient_email_address_column]

                if not use_advanced_options:
                    rendered_subject = basic_options.subject
                    rendered_plaintext_message = basic_options.message_body
                    rendered_html_message = None
                    custom_attachments_paths_by_filename = attachments_paths_by_filename
                else:
                    if subject_column is not None:
                        subject_template_text = row[subject_column]
                        self._validate_template_text(subject_template_text, columns)

                    try:
                        rendered_subject = Template(subject_template_text).render(row)
                    except Exception:
                        rendered_subject = subject_template_text

                    if plaintext_template_column is not None:
                        plaintext_template_text = row[plaintext_template_column]
                        self._validate_template_text(plaintext_template_text, columns)

                        if html_template_column is not None:
                            html_template_text = row[html_template_column]
                            self._validate_template_text(html_template_text, columns)

                    rendered_plaintext_message = Template(plaintext_template_text).render(row)
                    rendered_html_message = None
                    if use_html_template:
                        rendered_html_message = Template(html_template_text).render(row)

                    custom_attachments_paths_by_filename = attachments_paths_by_filename
                    if self.cfg.advanced_options.include_attachments and not self._client.disable_attachments:
                        # Only "from_table" mode needs per-row attachment loading from CSV column;
                        # other modes (all_input_files, single_table) use the same attachments for all recipients
                        if attachments_config.attachments_source == "from_table":
                            custom_attachments_paths_by_filename = {
                                attachment_filename: attachments_paths_by_filename[attachment_filename]
                                for attachment_filename in json.loads(row[attachments_column])
                            }

                # Append sample info text to email body if present
                if sample_metadata and not self._client.disable_attachments:
                    info_text_rendered = sample_metadata["info_text"].format(
                        n=sample_metadata["row_count"],
                        total=sample_metadata["total_count"],
                    )

                    # Append to plaintext
                    rendered_plaintext_message = f"{rendered_plaintext_message}\n{info_text_rendered}"

                    # Append to HTML if present
                    if rendered_html_message:
                        rendered_html_message = f"{rendered_html_message}\n<p>{info_text_rendered}</p>"

                # Append custom link to email body if present
                if snapshot_link:
                    link_text = snapshot_link["text"]
                    link_url = snapshot_link["url"]
                    expiry_note = "(snapshot expires after 15 days)"

                    # Append to plaintext (with colon and expiry note)
                    rendered_plaintext_message = (
                        f"{rendered_plaintext_message}\n{link_text}:\n{link_url}\n{expiry_note}"
                    )

                    # Append to HTML if present (with expiry note)
                    if rendered_html_message:
                        rendered_html_message = (
                            f'{rendered_html_message}\n<p>{link_text} <a href="{link_url}">{link_url}</a><br>'
                            f"<small>{expiry_note}</small></p>"
                        )

                email_ = self._client.build_email(
                    recipient_email_address=recipient_email_address,
                    subject=rendered_subject,
                    attachments_paths_by_filename=custom_attachments_paths_by_filename,
                    rendered_plaintext_message=rendered_plaintext_message,
                    rendered_html_message=rendered_html_message,
                )

                status = "OK"
                error_message = ""
                if not dry_run:
                    try:
                        logging.info(
                            f"Sending email with subject: `{email_['Subject']}`"
                            f" from `{email_['From']}` to `{email_['To']}`"
                        )

                        if not self.cfg.advanced_options.include_attachments or self._client.disable_attachments:
                            attachment_paths = []
                        else:
                            attachment_paths = custom_attachments_paths_by_filename.values()

                        self._client.send_email(
                            email_,
                            message_body=rendered_plaintext_message,
                            html_message_body=rendered_html_message,
                            attachments_paths=attachment_paths,
                        )

                    except Exception as e:
                        error_message = str(e)
                        status = "ERROR"
                        self._results_writer.errors = True

                rendered_html_message_writable = ""
                if rendered_html_message:
                    rendered_html_message_writable = rendered_html_message

                attachments_to_log = (
                    json.dumps(list(custom_attachments_paths_by_filename))
                    if custom_attachments_paths_by_filename
                    else []
                )

                self._results_writer.writerow(
                    dict(
                        status=status,
                        recipient_email_address=email_["To"],
                        sender_email_address=email_["From"],
                        subject=email_["Subject"],
                        plaintext_message_body=rendered_plaintext_message,
                        html_message_body=rendered_html_message_writable,
                        attachment_filenames=attachments_to_log,
                        error_message=error_message,
                    )
                )
                if error_message and not continue_on_error:
                    break
                time.sleep(SLEEP_INTERVAL)

            except Exception as e:
                self._results_writer.writerow(
                    {
                        **general_error_row,
                        "sender_email_address": self._client.sender_email_address,
                        "recipient_email_address": recipient_email_address,
                        "error_message": str(e),
                    }
                )
                self._results_writer.errors = True
                if not continue_on_error:
                    break

        try:
            in_table.close()
        except NameError:
            pass

    def _extract_template_files_full_paths(
        self, in_files_by_name: Dict[str, List[FileDefinition]]
    ) -> Tuple[Union[str, None], Union[str, None]]:
        """Extracts full paths for template files if they are provided"""
        msg_body_config = self.cfg.advanced_options.message_body_config
        plaintext_template_path = None
        html_template_path = None
        if msg_body_config.message_body_source == "from_template_file":
            plaintext_template_filename = msg_body_config.plaintext_template_filename
            plaintext_template_path = next(
                files[0].full_path
                for name, files in in_files_by_name.items()
                if files[0].name.endswith(plaintext_template_filename)
            )
            if msg_body_config.use_html_template:
                html_template_filename = msg_body_config.html_template_filename
                html_template_path = next(
                    files[0].full_path
                    for name, files in in_files_by_name.items()
                    if files[0].name.endswith(html_template_filename)
                )
        return plaintext_template_path, html_template_path

    @staticmethod
    def _read_template_file(template_path: str) -> str:
        with open(template_path) as file:
            return file.read()

    @staticmethod
    def _parse_template_placeholders(template_text: str) -> Set[str]:
        placeholders = re.findall(r"\{\{.*?\}\}", template_text)
        placeholders = set([placeholder.strip("{}") for placeholder in placeholders])
        return placeholders

    def _validate_template_text(self, template_text: str, columns: set, continue_on_error: bool = False) -> None:
        template_placeholders = self._parse_template_placeholders(template_text)
        missing_columns = set(template_placeholders) - set(columns)
        if missing_columns:
            if not continue_on_error:
                raise UserException("❌ Missing columns: " + ", ".join(missing_columns))

    def _get_attachments_filenames_from_table(self, in_table_path: str) -> Set[str]:
        attachments_filenames = set()
        try:
            with open(in_table_path) as in_table:
                reader = csv.DictReader(in_table)
                attachments_column = self.cfg.advanced_options.attachments_config.attachments_column
                for row in reader:
                    for attachment_filename in json.loads(row[attachments_column]):
                        attachments_filenames.add(attachment_filename)
        except Exception as e:
            raise UserException(
                f"Couldn't read attachments from table {in_table_path} column {attachments_column}: {str(e)}"
            )
        return attachments_filenames

    def _resolve_data_source_table_path(self, table_destination: str) -> str:
        """
        Resolves data source table destination name to local file path.

        Args:
            table_destination: Table destination name from config (e.g., "email_export.csv")

        Returns:
            Local CSV file path

        Raises:
            UserException: If table not found in input tables
        """
        in_tables = self.get_input_tables_definitions()
        try:
            return next(
                in_table.full_path for in_table in in_tables if Path(in_table.full_path).name == table_destination
            )
        except StopIteration:
            available = [Path(t.full_path).name for t in in_tables]
            raise UserException(
                f"Data source table '{table_destination}' not found in input tables. Available: {available}"
            )

    def _count_csv_rows(self, csv_path: str) -> int:
        """
        Counts data rows in CSV file (excluding header).

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of data rows (header not counted)
        """
        with open(csv_path, encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)

    def _generate_table_sample(
        self,
        table_path: str,
        row_limit: int,
        filename_template: str,
        table_name: str,
        sort_enabled: bool = False,
        sort_column: Union[str, None] = None,
        sort_order: str = "asc",
    ) -> Tuple[str, int]:
        """
        Generates a CSV sample file with first N rows, optionally sorted.

        Args:
            table_path: Path to source CSV file
            row_limit: Maximum number of rows to include
            filename_template: Filename template with {table_name} placeholder
            table_name: Table name for filename substitution
            sort_enabled: Whether sorting is enabled
            sort_column: Column name to sort by (optional)
            sort_order: Sort order - "asc" or "desc" (default: "asc")

        Returns:
            Tuple of (sample_file_path, actual_row_count)
        """
        filename = filename_template.replace("{table_name}", table_name)
        sample_path = os.path.join(self.data_folder_path, filename)

        with open(table_path, encoding="utf-8") as source:
            reader = csv.DictReader(source)
            header = reader.fieldnames

            # Read rows up to limit
            rows = []
            for i, row in enumerate(reader):
                if i >= row_limit:
                    break
                rows.append(row)

            # Sort if enabled and column specified and exists in header
            if sort_enabled and sort_column and sort_column in header:
                reverse = sort_order == "desc"
                rows.sort(key=lambda r: r.get(sort_column, ""), reverse=reverse)

            # Write to sample file
            with open(sample_path, "w", encoding="utf-8", newline="") as dest:
                writer = csv.DictWriter(dest, fieldnames=header)
                writer.writeheader()
                writer.writerows(rows)

        return sample_path, len(rows)

    def _get_missing_columns_from_table(self, reader: csv.DictReader, column: str) -> Set[str]:
        unique_placeholders = set()
        for row in reader:
            row_placeholders = self._parse_template_placeholders(template_text=row[column])
            unique_placeholders = unique_placeholders.union(row_placeholders)
        missing_columns = set(unique_placeholders) - set(reader.fieldnames)
        return missing_columns

    def _validate_templates_from_table(self, reader: csv.DictReader, plaintext: bool) -> ValidationResult:
        message = VALID_PLAINTEXT_TEMPLATE_MESSAGE if plaintext else VALID_HTML_TEMPLATE_MESSAGE
        key_template_column = KEY_PLAINTEXT_TEMPLATE_COLUMN if plaintext else KEY_HTML_TEMPLATE_COLUMN
        message_type = MessageType.SUCCESS
        template_column = self.cfg.advanced_options.message_body_config[key_template_column]
        missing_columns = self._get_missing_columns_from_table(reader, template_column)
        if missing_columns:
            message = "❌ Missing columns: " + ", ".join(missing_columns)
            message_type = MessageType.DANGER
        return ValidationResult(message, message_type)

    def _read_template_text(self, plaintext: bool = True) -> str:
        """Reads in template either from file, or from config"""
        message_body_config = self.cfg.advanced_options.message_body_config
        message_body_source = message_body_config.message_body_source

        if message_body_source == "from_template_file":
            key_template_filename = KEY_PLAINTEXT_TEMPLATE_FILENAME if plaintext else KEY_HTML_TEMPLATE_FILENAME
            template_filename = message_body_config[key_template_filename]
            files = self._list_files_in_sync_actions()
            if not files:
                raise UserException(
                    "No files found in the storage. Please use tags to select your files instead of query."
                )
            template_file_id = next(file["id"] for file in files if file["name"] == template_filename)
            template_path = self._download_file_from_storage_api(template_file_id)
            template_text = self._read_template_file(template_path)
        elif message_body_source == "from_template_definition":
            key_template_text = KEY_PLAINTEXT_TEMPLATE_DEFINITION if plaintext else KEY_HTML_TEMPLATE_DEFINITION
            template_text = message_body_config[key_template_text]
        else:
            raise UserException("Invalid message body source")
        return template_text

    def _init_storage_client(self) -> StorageClient:
        storage_token = self.environment_variables.token
        storage_client = StorageClient(self.environment_variables.url, storage_token)
        return storage_client

    def _return_table_path(self, table_name: str) -> str:
        table_path = None
        if self.configuration.action == "run":
            all_tables = self.get_input_tables_definitions()
            for table in all_tables:
                if table_name == table.name:
                    table_path = table.full_path
                    break

        # download via storage api for sync actions
        else:
            table_path = self._download_table_from_storage_api(table_name)

        return table_path

    def _download_table_from_storage_api(self, table_name) -> str:
        try:
            storage_client = self._init_storage_client()
            table_id = next(
                table.source for table in self.configuration.tables_input_mapping if table.destination == table_name
            )
            table_path = storage_client.tables.export_to_file(table_id=table_id, path_name=self.files_in_path)
        except Exception as e:
            raise UserException(f"Failed to access table {table_name} in storage: {str(e)}")
        return table_path

    def _download_file_from_storage_api(self, file_id) -> str:
        storage_client = self._init_storage_client()
        file_path = storage_client.files.download(file_id=file_id, local_path=self.files_in_path)
        return file_path

    def _list_files_in_sync_actions(self) -> List[Dict]:
        storage_client = self._init_storage_client()
        all_input_files = []
        try:
            for file_input in self.configuration.config_data["storage"]["input"]["files"]:
                tags = [tag["name"] for tag in file_input["source"]["tags"]]
                input_files = storage_client.files.list(tags=tags)
                all_input_files.extend(input_files)
            return all_input_files
        except KeyError:
            return []

    def _validate_template(self, plaintext: bool = True) -> ValidationResult:
        valid_message = VALID_PLAINTEXT_TEMPLATE_MESSAGE if plaintext else VALID_HTML_TEMPLATE_MESSAGE
        if self.cfg.advanced_options.message_body_config.message_body_source == "from_table":
            table_name = self.cfg.advanced_options.email_data_table_name
            in_table_path = self._return_table_path(table_name)
            with open(in_table_path) as in_table:
                reader = csv.DictReader(in_table)
                return self._validate_templates_from_table(reader, plaintext)
        else:
            template_text = self._read_template_text(plaintext)

            # Parse placeholders first
            template_placeholders = self._parse_template_placeholders(template_text)

            # If no placeholders, validation passes immediately
            if not template_placeholders:
                return ValidationResult(valid_message, MessageType.SUCCESS)

            # Load columns only if we have placeholders to validate
            columns = self.load_input_table_columns_()

            # If loading failed, we can't validate - return error (strict)
            if isinstance(columns, ValidationResult):
                return ValidationResult(
                    "❌ Cannot validate placeholders: email data table is not accessible", MessageType.DANGER
                )

            # Validate placeholders against columns
            try:
                self._validate_template_text(template_text, columns)
                return ValidationResult(valid_message, MessageType.SUCCESS)
            except UserException as e:
                return ValidationResult(str(e), MessageType.DANGER)

    def __exit__(self):
        self._client.smtp_server.close()

    def test_smtp_server_connection_(self) -> ValidationResult:
        connection_config = ConnectionConfig.load_from_dict(self.configuration.parameters["connection_config"])
        try:
            self.init_client(connection_config=connection_config)
            return ValidationResult("✅ Connection established successfully", MessageType.SUCCESS)
        except Exception as e:
            return ValidationResult(f"❌ Connection couldn't be established. Error: {e}", MessageType.DANGER)

    @sync_action("testConnection")
    def test_smtp_server_connection(self) -> ValidationResult:
        return self.test_smtp_server_connection_()

    @sync_action("load_input_table_selection")
    def load_input_table_selection(self) -> List[SelectElement]:
        return [SelectElement(table.destination) for table in self.configuration.tables_input_mapping]

    def _load_table_columns(self, table_name: str | None, field_label: str) -> list[str] | ValidationResult:
        """Fetch columns from a table via Storage API. Used by sync actions."""
        if table_name is None:
            return ValidationResult(f"You must specify `{field_label}` before loading columns", MessageType.DANGER)
        try:
            table_id = next(
                table.source for table in self.configuration.tables_input_mapping if table.destination == table_name
            )
            storage_url = (
                f"https://{self.environment_variables.stack_id}"
                if self.environment_variables.stack_id
                else "https://connection.keboola.com"
            )
            tables = StorageTables(storage_url, self.environment_variables.token)
            preview = tables.preview(table_id)
            reader = csv.DictReader(StringIO(preview))
            return reader.fieldnames
        except Exception:
            return ValidationResult("Couldn't fetch columns", MessageType.DANGER)

    @sync_action("load_input_table_columns")
    def load_input_table_columns(self) -> list[SelectElement]:
        columns = self._load_table_columns(self.cfg.advanced_options.email_data_table_name, "Email Data Table Name")
        if isinstance(columns, ValidationResult):
            return columns
        return [SelectElement(column) for column in columns]

    @sync_action("load_source_table_columns")
    def load_source_table_columns(self) -> list[SelectElement]:
        columns = self._load_table_columns(self.cfg.advanced_options.attachments_config.source_table, "Source Table")
        if isinstance(columns, ValidationResult):
            return columns
        return [SelectElement(column) for column in columns]

    def validate_subject_(self) -> ValidationResult:
        subject_config = self.cfg.advanced_options.subject_config
        if subject_config.subject_source == "from_table":
            subject_column = subject_config.subject_column
            table_name = self.cfg.advanced_options.email_data_table_name
            in_table_path = self._return_table_path(table_name)
            with open(in_table_path) as in_table:
                reader = csv.DictReader(in_table)
                missing_columns = self._get_missing_columns_from_table(reader, subject_column)
                if missing_columns:
                    message = "❌ Missing columns: " + ", ".join(missing_columns)
                    return ValidationResult(message, MessageType.DANGER)
                return ValidationResult(VALID_SUBJECT_MESSAGE, MessageType.SUCCESS)
        else:
            subject_template_text = subject_config.subject_template_definition

            # Parse placeholders first
            template_placeholders = self._parse_template_placeholders(subject_template_text)

            # If no placeholders, validation passes immediately
            if not template_placeholders:
                return ValidationResult(VALID_SUBJECT_MESSAGE, MessageType.SUCCESS)

            # Load columns only if we have placeholders to validate
            columns = self.load_input_table_columns_()

            # If loading failed, we can't validate - return error (strict)
            if isinstance(columns, ValidationResult):
                return ValidationResult(
                    "❌ Cannot validate placeholders: email data table is not accessible", MessageType.DANGER
                )

            # Validate placeholders against columns
            try:
                self._validate_template_text(subject_template_text, columns)
                return ValidationResult(VALID_SUBJECT_MESSAGE, MessageType.SUCCESS)
            except UserException as e:
                return ValidationResult(str(e), MessageType.DANGER)

    @sync_action("validate_subject")
    def validate_subject(self) -> ValidationResult:
        return self.validate_subject_()

    def validate_plaintext_template_(self) -> ValidationResult:
        return self._validate_template(plaintext=True)

    @sync_action("validate_plaintext_template")
    def validate_plaintext_template(self) -> ValidationResult:
        return self.validate_plaintext_template_()

    def validate_html_template_(self) -> ValidationResult:
        return self._validate_template(plaintext=False)

    @sync_action("validate_html_template")
    def validate_html_template(self) -> ValidationResult:
        return self.validate_html_template_()

    def validate_attachments_(self) -> ValidationResult:
        message = VALID_ATTACHMENTS_MESSAGE
        try:
            if self.cfg.advanced_options.attachments_config.attachments_source == "from_table":
                table_name = self.cfg.advanced_options.email_data_table_name
                in_table_path = self._return_table_path(table_name)
                expected_input_filenames = self._get_attachments_filenames_from_table(in_table_path)
                input_filenames = set([file["name"] for file in self._list_files_in_sync_actions()])
                input_tables = set([table.destination for table in self.configuration.tables_input_mapping])
                missing_attachments = expected_input_filenames - input_filenames - input_tables
                if missing_attachments:
                    message = "❌ Missing attachments: " + ", ".join(missing_attachments)
        except Exception as e:
            message = f"❌ Couldn't validate attachments. Error: {e}"
        message_type = MessageType.SUCCESS if message == VALID_ATTACHMENTS_MESSAGE else MessageType.DANGER
        return ValidationResult(message, message_type)

    @sync_action("validate_attachments")
    def validate_attachments(self) -> ValidationResult:
        return self.validate_attachments_()

    def validate_single_table_(self) -> ValidationResult:
        """
        Validate single table mode configuration (CSV sample and snapshot link features).

        Note: This is a helper method (trailing underscore) called from validate_config() sync action.
        The underscore suffix prevents the @sync_action decorator from interfering when called from
        another sync action - decorated methods would redirect stdout, write JSON, and potentially exit(),
        which would hijack the caller's output handling.

        Returns ValidationResult with aggregated error messages (all errors at once, not fail-fast).
        """
        errors = []

        attachments_config = self.cfg.advanced_options.attachments_config
        source_table = attachments_config.source_table

        # Check 1: Source table must be specified
        if not source_table:
            errors.append("❌ Source table must be specified for single table mode")
        else:
            # Check 2: Source table must exist in input tables (use tables_input_mapping for sync actions)
            available_tables = [table.destination for table in self.configuration.tables_input_mapping]
            if source_table not in available_tables:
                errors.append(
                    f"❌ Source table '{source_table}' not found in input tables. Available: {available_tables}"
                )

        # Check 3: At least one toggle must be enabled
        if not attachments_config.include_csv_sample and not attachments_config.include_snapshot_link:
            errors.append(
                "❌ At least one option must be enabled: 'Include CSV Sample' or 'Include Link to Table Snapshot'"
            )

        if errors:
            message = "\n\n".join(errors)
            message_type = MessageType.ERROR
        else:
            message = "✅ Single table configuration is valid"
            message_type = MessageType.SUCCESS

        return ValidationResult(message, message_type)

    @sync_action("validate_single_table")
    def validate_single_table(self) -> ValidationResult:
        return self.validate_single_table_()

    @sync_action("validate_config")
    def validate_config(self) -> ValidationResult:
        # TODO: once sys.stdout is None handling is released, remove helper methods and use other sync actions directly
        validation_methods = [
            self.test_smtp_server_connection_,
            self.validate_subject_,
            self.validate_plaintext_template_,
        ]
        if self.cfg.advanced_options.message_body_config.use_html_template:
            validation_methods.insert(3, self.validate_html_template_)

        image_parameters = self.configuration.image_parameters or {}
        disable_attachments = image_parameters.get(KEY_DISABLE_ATTACHMENTS, False)
        if self.cfg.advanced_options.include_attachments and not disable_attachments:
            attachments_source = self.cfg.advanced_options.attachments_config.attachments_source

            # Early return for unrecognized attachment source values (e.g., legacy configs)
            if attachments_source not in ("from_table", "single_table", "all_input_files"):
                return ValidationResult(
                    f"❌ Unknown attachment source: '{attachments_source}'. "
                    f"Valid options: 'from_table', 'single_table', 'all_input_files'",
                    MessageType.DANGER,
                )

            if attachments_source == "single_table":
                validation_methods.append(self.validate_single_table_)
            elif attachments_source == "from_table":
                validation_methods.append(self.validate_attachments_)

        messages = [validation_method().message for validation_method in validation_methods]

        if any(message.startswith("❌") for message in messages):
            message_type = MessageType.DANGER
        else:
            message_type = MessageType.SUCCESS

        message = "\n\n".join(messages)
        return ValidationResult(message, message_type)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
