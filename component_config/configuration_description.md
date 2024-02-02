# SMTP Sender Application

## Config fields

 - **Sender Email Address** - Sender Email Address
 - **Sender App Password** - certain SMTP server providers require you to generate an app pappword instead of the account one usually when MFA is enabled
 - **SMTP Server Host** - SMTP Server Host
 - **SMTP Server Port** - SMTP Server Port
 - **Use SSL** - specifies, whether to connect via SSL or TLS
 - **Shared attachments** - if checked, all non-template files in the files input mapping will be attached to the email for all recipients, otherwise input table expects column `attachments`, which contains semicolon delimited filenames, so that each recipient could recieve a specific subset of attachments


## Required Input Files
 - `template.txt` - contains plaintext template with placeholders in jinja2 format

## Optional Input Files
 - `template.html` - contains html template with placeholders in jinja2 format
 - arbitrary number of attachment files - supported extensions are: `txt`, `json`, `csv`, `xlsx`, `xls`, `jpg`, `jpeg`, `png`, `pdf` - different extensions will be ignored

## Required Input Tables
 - `arbitrary_table_name`

columns:
 - `recipient_email_address` - required
 - `subject` - required
 - `attachments` - only required if **Shared attachments** config field is not checked - semicolon delimited 
 - columns with names corresponding to placeholder names in your template(s)

