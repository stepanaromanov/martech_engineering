# Create Google Cloud project, enable Api, and created credentials for oauth
# pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip install psycopg2-binary

import os
import psycopg2
from psycopg2 import OperationalError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = XXXXXXXXXX

#try to request token for authorization
try:
    credentials = None
    if os.path.exists("./tokens/google_sheets.json"):
        credentials = Credentials.from_authorized_user_file("./tokens/google_sheets.json", SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("./credentials/google_sheets.json", SCOPES)
            credentials = flow.run_local_server(port=0)
        with open("./tokens/google_sheets.json", "w") as token:
            token.write(credentials.to_json())

    #try to get values from google sheets file
    try:
        service = build("sheets", "v4", credentials=credentials)
        sheets = service.spreadsheets()

        result = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range="'Sheet first'!A1:D2").execute()

        values = result.get("values", [])

        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database=XXXXXXXXXX,
            user=XXXXXXXXXX,
            password=XXXXXXXXXX,
        )

        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS google_sheets_costs (
                    id int not null,
                    category text not null,
                    amount int not null,
                    description text not null
                );
            """
        )

        #then we insert values in postgreSQL database
        for i in range(1, len(values)):
            cur.execute(
                """
                    INSERT INTO google_sheets_costs (id, category, amount, description) VALUES (%s, %s, %s, %s);
                """,
                (
                    values[i][0], 
                    values[i][1], 
                    values[i][2], 
                    values[i][3]
                )
            )

        conn.commit()
        cur.close()
        conn.close()

    except OperationalError as error:
        print(f"The error '{error}' occurred")

except HttpError as error:
    print(f"The error '{error}' occurred")