# SMTP Sender Application

- Component enabling users to send emails with custom subject, message body and attachments from keboola platform

## Config fields

### Connection Config

**o365 outlook via oauth**

 - **Sender Email Address**
 - **Client ID** - app needs to be registered in `Microsoft Entra ID` in Azure portal under `App registrations` (under API permissions you need to add application type permissins: `Mail.Send`, `Mail.ReadWrite` and `User.Read.All`)
 - **Client Secret** - needs to be generated under `Certificates & secrets` tab
 - **Tenant ID** - can be found in the overview tab of your app

**SSL**

 - **Sender Email Address** - Sender Email Address
 - **Sender App Password** - certain SMTP server providers require you to generate an app password instead of the account one (typically when MFA is enabled)
 - **SMTP Server Host** - SMTP Server Host
 - **SMTP Server Port** - SMTP Server Port
 - **Connection Protocol** - specifies, whether to connect via SSL or TLS
 - **Use Proxy Server** - Use Proxy Server
 - **Proxy Server Config**
   - **Proxy Server Host** - Proxy Server Host
   - **Proxy Server Port** - Proxy Server Host
   - **Proxy Server Username** - Proxy Server Username
   - **Proxy Server Password** - Proxy Server Password

### Configuration types:

### Basic

It allows user to send specific subject and message body with or without attachments to a list of recipients

- **Recipient Email Addresses** - comma delimited list of email addresses
- **Subject** - subject literal
- **Message Body** - message body literal
- **Include Attachments** - checkbox indicating, whether to attach files and table in input mapping

### Advanced

As opposed to the basic configuration option you need to provide table with **Recipient Email Address Column** and potentially other columns corresponding to column names in `From Table` source options and columns containing values for placeholders in your subject and message body templates
It lets you choose from multiple sourcing options for subject, message body and attachments. It enables user to include html message body version with the plaintext version as a backup

- **Email Data Table Name** - dynamically loaded selection of the table containing recipient email addresses, subject and message body template placeholder values and custom attachment filenames (if selected)
- **Recipient Email Address Column** - Recipient email address column name

- **Subject Config**
 - **Subject Source** - `From Table`, `From Template Definition`
 - **Subject Column** - Subject column name (Subject Source = `From Table`)
 - **Plaintext Subject Template** - Jinja2 formatted subject (Subject Source = `From Template Definition`)

**Message Body Config**

- **Message Body Source** - `From Table`,`From Template File`, `From Template Definition`
- **Use HTML Alternative** - Checkbox indicating, whether you want to provide HTML version of message body
- **Plaintext Message Body Column** - Plaintext message body column name (Message Body Source = `From Table`)
- **HTML Message Body Column** - HTML message body column name (Message Body Source = `From Table`)
- **Plaintext Template File** - Plaintext message body template filename (Message Body Source = `From Template File`)
- **HTML Template File** - HTML message body template filename (Message Body Source = `From Template File`)
- **Plaintext Message Body Template** - Jinja2 formatted plaintext message body (Message Body Source = `From Template Definition`)
- **HTML Message Body Template** - Jinja2 formatted html message body (Message Body Source = `From Template Definition`)

**Include Attachments** - (boolean) default True

**Attachments Config**

- **Attachments Source** - `From Table`, `All Input Files`
- **Attachments Column** - Attachments column name - json list containing input file names and table names, so that each recipient can receive a specific subset of attachments (Attachments Source = `From Table`)
- **Shared attachments** - if checked, all non-template files in the files input mapping and tables from table input mapping will be attached to the email for all recipients (Attachments Source = `All Input Files`)
 - arbitrary number of attachment files - attachments can be of any file type or simply tables in input mapping (certain SMTP server providers forbid certain file types since they are considered potentially dangerous)


- **_NOTE 1_** - The maximum attachment size that the standard backend can process is 25MB. When running on a large backend, this limit increases to 150MB. However, the SMTP server may have its own limit.
- **_NOTE 2_** - Tables in the storage uses efficient compression, so the size of a CSV export file can be significantly larger than the table size displayed in the UI.

**Dry Run** - if checked - emails are built, but not sent
**Continue On Error** - if not checked - first unsendable email will crash the component - results table will still be populated with sent emails detail

## Required Input Tables

 - `arbitrary_table_name` - should be selected in `Email Data Table Name` field

 **columns:**
 
 - recipient_email_address
 - subject_column (depends on config)
 - plaintext_template_column (depends on config)
 - html_template_column (depends on config)
 - attachments_column (depends on config)
 - columns with names corresponding to placeholder names in your template(s)

## Output Table

 - `results` - gives you detail on each attempted recipient

 **columns:**
 
 - `status` - `OK` or `ERROR`
 - `recipient_email_address` - recipient_email_address
 - `sender_email_address` - sender_email_address
 - `subject` - subject
 - `plaintext_message_body` - plaintext_message_body
 - `html_message_body` - html_message_body
 - `attachment_filenames` - attachment_filenames
 - `error_message` - error_message

## Sync Actions

 - `TEST SMTP SERVER CONNECTION` - tests, whether connection to the SMTP server can be established
 - `VALIDATE SUBJECT` - validates, that all placeholders in the provided subject template are present in the input table
 - `VALIDATE PLAINTEXT TEMPLATE` - validates, that all placeholders in the provided message body plaintext template are present in the input table
 - `VALIDATE HTML TEMPLATE` - validates, that all placeholders in the provided message body HTML template are present in the input table
 - `VALIDATE ATTACHMENTS` - validates, that all attachment files are present in the file input mapping
 - `VALIDATE CONFIG` - runs all tests and validations above and provides detail
