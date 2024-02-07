# SMTP Sender Application

## Config fields

**Connection Config**
 - **Sender Email Address** - Sender Email Address
 - **Sender App Password** - certain SMTP server providers require you to generate an app password instead of the account one (typically when MFA is enabled)
 - **SMTP Server Host** - SMTP Server Host
 - **SMTP Server Port** - SMTP Server Port
 - **Proxy Server Host** - Proxy Server Host
 - **Proxy Server Port** - Proxy Server Host
 - **Proxy Server Username** - Proxy Server Username
 - **Proxy Server Password** - Proxy Server Password
 - 
 - **Connection Protocol** - specifies, whether to connect via SSL or TLS

**Recipient Email Address Column** - Recipient email address column name

**Subject Config**
- **Subject Source** - `From Table`, `Using Template`
- **Subject Column** - Subject column name (Subject Source = `From Table`)
- **Subject Template** - Jinja2 formatted subject (Subject Source = `Using Template`)

**Message Body Config**
- **Message Body Source** - `From Table`,`From Template File`, `From Template Definition`
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

 - arbitrary number of attachment files - supported extensions are: `txt`, `json`, `csv`, `xlsx`, `xls`, `jpg`, `jpeg`, `png`, `pdf` - different extensions will be ignored

## Required Input Tables
 - `arbitrary_table_name`
 - columns with names corresponding to placeholder names in your template(s)

