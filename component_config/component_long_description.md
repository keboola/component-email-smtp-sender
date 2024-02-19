# SMTP Sender Application
- Component enabling users to send emails with custom subject, message body and attachments from keboola platfrom

## Config fields

**Connection Config**
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

**Recipient Email Address Column** - Recipient email address column name

**Subject Config**
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

**Attachments Config**
- **Attachments Source** - `From Table`, `All Input Files`
- **Attachments Column** - Attachments column name - json list containing input filenames, so that each recipient can recieve a specific subset of attachments (Attachments Source = `From Table`)
- **Shared attachments** - if checked, all non-template files in the files input mapping will be attached to the email for all recipients (Attachments Source = `All Input Files`)

**Dry Run** - if checked - emails are built, but not sent

 - arbitrary number of attachment files - attachment can be of any file type (certain SMTP server providers forbid some types since they are considered potentially dangerous)
## Required Input Tables
 - `arbitrary_table_name`

 **columns:**
 - recipient_email_address
 - subject_column (depends on config)
 - plaintext_template_column (depends on config)
 - html_template_column (depends on config)
 - attachments_column (depends on config)
 - columns with names corresponding to placeholder names in your template(s)

## Output Table
 - `results`

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
 - `TEST SMTP SERVER CONNECTION` - tests, whether connection can be established
 - `VALIDATE SUBJECT` - validates, that all placeholders in the provided subject template are present in the input table
 - `VALIDATE PLAINTEXT TEMPLATE` - validates, that all placeholders in the provided message body plaintext template are present in the input table
 - `VALIDATE HTML TEMPLATE` - validates, that all placeholders in the provided message body HTML template are present in the input table
 - `VALIDATE ATTACHMENTS` - validates, that all attachment files are present in the file input mapping
 - `VALIDATE CONFIG` - runs all tests and validations above
