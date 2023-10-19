# Create Google Cloud project, enable Api, and created credentials for oauth
# pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip install psycopg2-binary
# pip install pandas

import os
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "XXXXXXXXXX"

# get a new token or refresh existing one if it is expired
credentials = None
if os.path.exists("token.json"):
    credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
if not credentials or not credentials.valid:
    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file("./credentials/google_sheets.json",
                                                         SCOPES)
        credentials = flow.run_local_server(port=0)
    with open("token.json", "w") as token:
        token.write(credentials.to_json())

# create column names and dataframe for data that will be imported
df_columns = [
    'Yili',
    'Lead id',
    'Telefon raqami',
    '0',
    '',
    '№',
    'F.I.SH',
    'Guruh',
    'Kirim orderi',
    'Sana',
    'Kurs narhi',
    'Jami\ntulovi',
    'Tulangan',
    'Chegirma summasi',
    'Agentlik',
    'Ortiqcha \ntulov summasi',
    'Kutulayotgan tushum',
    'Oqituvchi'
]

output = pd.DataFrame(
    data=[],
    columns=df_columns
)

# connect to our spreadsheet file
try:
    service = build("sheets", "v4", credentials=credentials)
    sheets = service.spreadsheets()
    # define names for existing sheets (sub datasets); every sheet consists of monthly financial details on revenue
    for year in range(2023, 2024):
        for month in range(1, 12):
            sheet = str(month) + "." + str(year)
            # try to parse every sheet and create dataframe from it
            try:
                result = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{sheet}!A:R").execute()
                values = result.get("values", [])
                sheet_df = pd.DataFrame(
                    data=values,
                    columns=df_columns
                )
                # skip rows where year or phone number is missing
                filtered_sheet_df = sheet_df[(sheet_df['Yili'] != '') &
                                             (sheet_df['Telefon raqami'] != '')].dropna().tail(-1)
                filtered_sheet_df['month'] = sheet

                # concat monthly sub dataframe with the main dataframe
                output = pd.concat([output, filtered_sheet_df], ignore_index=True)
            except:
                print(f"{sheet} could not be extracted")
                print(filtered_sheet_df)

except HttpError as error:
    print(error)

# delete unnecessary columns
output.drop(['0', '', '№', 'Kirim orderi'], axis=1, inplace=True)

# rename existing columns
output.rename(columns={'Yili': 'year',
                       'Lead id': 'lead_id',
                       'Telefon raqami': 'phone_number',
                       'F.I.SH': 'full_name',
                       'Guruh': 'group_name',
                       'Sana': 'date',
                       'Kurs narhi': 'course_price',
                       'Jami\ntulovi': 'total_amount',
                       'Tulangan': 'paid_amount',
                       'Chegirma summasi': 'discount_amount',
                       'Agentlik': 'agency',
                       'Ortiqcha \ntulov summasi': 'overpaid_amount',
                       'Kutulayotgan tushum': 'expected_income',
                       'Oqituvchi': 'teacher'
                       }, inplace=True)

# clean multiple spaces '   ' from every empty cell in dataframe
for col in output:
    output[col] = output[col].str.strip()

# fill every numeric column with zeroes if cell is empty
output['year'] = output['year'].apply(lambda x: 0 if len(x) == 0 else x)
output['lead_id'] = output['lead_id'].apply(lambda x: 0 if len(x) == 0 else x)
output['course_price'] = output['course_price'].apply(lambda x: 0 if len(x) == 0 else x)
output['total_amount'] = output['total_amount'].apply(lambda x: 0 if len(x) == 0 else x)
output['paid_amount'] = output['paid_amount'].apply(lambda x: 0 if len(x) == 0 else x)
output['discount_amount'] = output['discount_amount'].apply(lambda x: 0 if len(x) == 0 else x)
output['overpaid_amount'] = output['overpaid_amount'].apply(lambda x: 0 if len(x) == 0 else x)

# delete spaces, apostrophes and commas from columns
output['phone_number'].replace(" ", "", inplace=True, regex=True)
output.replace("'", "", inplace=True, regex=True)
output['course_price'].replace(",", "", inplace=True, regex=True)
output['total_amount'].replace(",", "", inplace=True, regex=True)
output['paid_amount'].replace(",", "", inplace=True, regex=True)
output['discount_amount'].replace(",", "", inplace=True, regex=True)
output['agency'].replace(",", "", inplace=True, regex=True)
output['overpaid_amount'].replace(",", "", inplace=True, regex=True)

# try to connect to the local PostgreSQL database
try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="XXXXXXXXXX",
        user="XXXXXXXXXX",
        password="XXXXXXXXXX",
    )

    cur = conn.cursor()

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS accounting_groups (
               year  numeric,
               month text not null,
               lead_id numeric,
               phone_number text not null,
               full_name text not null,
               group_name text,
               date text,
               course_price numeric,
               total_amount numeric,
               paid_amount numeric,
               discount_amount numeric,
               agency text,
               overpaid_amount numeric,
               expected_income text,
               teacher text,
               last_updated timestamp not null
            );
        """
    )

    # if data already exists we add new rows in case financial information is updated
    for i in range(1, len(output)):
        cur.execute(
            f"""INSERT INTO accounting_groups (
                                   year,
                                   month,
                                   lead_id,
                                   phone_number,
                                   full_name,
                                   group_name,
                                   date,
                                   course_price,
                                   total_amount,
                                   paid_amount,
                                   discount_amount,
                                   agency,
                                   overpaid_amount,
                                   expected_income,
                                   teacher,
                                   last_updated)
                 SELECT
                     {int(output['year'][i])},
                     '{str(output['month'][i])}',
                     {int(output['lead_id'][i])},
                     '{str(output['phone_number'][i])}',
                     '{str(output['full_name'][i])}',
                     '{str(output['group_name'][i])}',
                     '{str(output['date'][i])}',
                     {float(output['course_price'][i])},
                     {float(output['total_amount'][i])},
                     {float(output['paid_amount'][i])},
                     {float(output['discount_amount'][i])},
                     '{str(output['agency'][i])}',
                     {float(output['overpaid_amount'][i])},
                     '{str(output['expected_income'][i])}',
                     '{str(output['teacher'][i])}',
                     NOW()::timestamp
                 WHERE NOT EXISTS ( 
                    SELECT lead_id
                    FROM accounting_groups
                    WHERE year = {int(output['year'][i])}
                            AND month = '{str(output['month'][i])}'
                            AND lead_id = {int(output['lead_id'][i])}
                            AND phone_number = '{str(output['phone_number'][i])}'
                            AND full_name = '{str(output['full_name'][i])}'
                            AND group_name = '{str(output['group_name'][i])}'
                            AND date = '{str(output['date'][i])}'
                            AND teacher = '{str(output['teacher'][i])}'
                 );""",

        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")

print('google sheets data has been successfully extracted and loaded')