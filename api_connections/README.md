### crm_etl.py

The script receives token for authorization in custom CRM. And after that, the script gets the data about leads status, transform it and then loads it into local PostgreSQL database. If the same information about lead already exists, script skips it.

### facebook_ads_etl.py

The script extracts data from Facebook ads manager. Then this script loads data into local PostgreSQL database. If information about campaign already exists, script updates it.

### google_sheets_etl.py

The script checks if google sheets token expired or doesn't exist and receives a new one if needed. And after that, the script gets the data from google sheet cells, transform it with pandas and then loads it into local PostgreSQL database.

### google_analytics_4_etl.py

The script connects to google analytics 4 API and gets four types of custom reports: sessions, devices, page paths, landing pages. Data is being transformed with pandas and NumPy. Then, script loads it into local PostgreSQL database. If time columns in the new dataframe intersect with existing data, old rows are updated. 
