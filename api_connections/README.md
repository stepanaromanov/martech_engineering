### crm_etl.py

The script receives token for authorization in custom CRM. And after that, the script gets the data about leads status, transform it and then loads it into local PostgreSQL database. If the same information about lead already exists, script skips it.

### facebook_ads_etl.py

The script extracts data from Facebook ads manager. Then this script loads data into local PostgreSQL database. If information about campaign already exists, script updates it.

### google_sheets_etl.py

The script checks if google sheets token expired or doesn't exist and receives a new one if needed. And after that, the script gets the data from google sheet cells, transform it and then loads it into local PostgreSQL database.