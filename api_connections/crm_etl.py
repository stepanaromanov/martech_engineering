
import psycopg2
import requests
from psycopg2 import OperationalError

# login info
LOGIN = XXXXXXXXXX
PASSWORD = XXXXXXXXXX

# get token
LOGIN_URL = XXXXXXXXXX
ORIGIN_URL = XXXXXXXXXX
REFERER_LOGIN_URL = XXXXXXXXXX

# get leads
LEADS_URL = XXXXXXXXXX
REFERER_LEADS_URL = XXXXXXXXXX

# headers config
LOGIN_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Origin': ORIGIN_URL,
    'Referer': REFERER_LOGIN_URL,
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
}

PAYLOAD = {
    "phone": LOGIN,
    "password": PASSWORD,
}

# trying to get token
try:
    token_response = requests.post(LOGIN_URL, data=PAYLOAD, headers=LOGIN_HEADERS)
    if token_response.ok:
        TOKEN = token_response.json()['access_token']
except:
    raise Exception("error while getting token")

# params for lead sections on the target page
PARAMS = (
    ('branch_id', '101'),
    ('sections_id[]',
     ['1001',
      '1002',
      '1003'
      ]
     ),
)

# updated headers config for getting leads
LEADS_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Origin': ORIGIN_URL,
    'Referer': REFERER_LEADS_URL,
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Authorization': 'Bearer {}'.format(TOKEN),
}

# getting json file with leads
try:
    leads_response = requests.get(LEADS_URL, headers=LEADS_HEADERS, params=PARAMS)
    if leads_response.ok:
        data = leads_response.json()['data']
except:
    raise Exception("error while getting leads")

# data file has a nested structure which looks like:
# array of objects ->
# data fields and sections (array of objects) ->
# data fields and leads (array of objects) ->
# data fields and object linked_stuff_id

leads_data = []

for column in data:
    #adding category information
    row = [column['id'], str(column['name']).replace("'", "")]
    for section in column['sections']:
        #adding subcategory information
        row += [section['id'], str(section['name']).replace("'", "")]
        for lead in section['leads']:
            #adding lead information
            if lead['linked_stuff_id']:
                staff_name = lead['linked_stuff_id']['name']
            else:
                staff_name = None
            row += [lead['id'],
                      lead['order_number'],
                      str(lead['name']).replace("'", ""),
                      lead['phone'],
                      str(lead['comment']).replace("'", ""),
                      lead['created_by'],
                      lead['updated_by'],
                      lead['deleted_by'],
                      str(lead['deleted_at']),
                      str(lead['created_at']),
                      str(lead['updated_at']),
                      lead['course_id'],
                      lead['source_id'],
                      str(staff_name).replace("'", ""),
                      #1 means current load, active in crm
                      1]
            leads_data.append(row)
            #truncating lead information after lead row added
            row = row[:4]
        #truncating section information after all leads added
        row = row[:2]

#load transformed data to PostgreSQL database
try:
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
            CREATE TABLE IF NOT EXISTS XXXXXXXXXX (
                category_id serial,
                category_name text,
                subcategory_id serial,
                subcategory_name text,
                lead_id serial,
                order_number text,
                name text,
                phone text,
                comment text,
                created_by text,
                updated_by text,
                deleted_by text,
                deleted_at text,
                created_at text,
                updated_at text,
                course_id text,
                source_id text,
                stuff_name text,
                is_active numeric,
                custom_client_id text,
                deal_probability numeric
            );
        """
    )

    for i in range(0, len(leads_data)):
        #insert if there are no full duplicates; some duplicates is ok because customer is moving through marketing funnel stages and its data can be updated
        cur.execute(
            #firstly, let's assign current crm status; if the lead has been deleted from the crm then we will write 0; otherwise we update database with current load (1)
            f"""UPDATE XXXXXXXXXX
                SET
                    active = CASE WHEN lead_id = {leads_data[i][4]} THEN 1 ELSE 0 END
                WHERE lead_id = {leads_data[i][4]};
                INSERT INTO XXXXXXXXXX (
                                category_id,
                                category_name,
                                subcategory_id,
                                subcategory_name,
                                lead_id,
                                order_number,
                                name,
                                phone,
                                comment,
                                created_by,
                                updated_by,
                                deleted_by,
                                deleted_at,
                                created_at,
                                updated_at,
                                course_id,
                                source_id,
                                stuff_name,
                                is_active)
                SELECT {leads_data[i][0]},
                        '{leads_data[i][1]}',
                        {leads_data[i][2]},
                        '{leads_data[i][3]}',
                        {leads_data[i][4]},
                        '{leads_data[i][5]}',
                        '{leads_data[i][6]}',
                        '{leads_data[i][7]}',
                        '{leads_data[i][8]}',                               
                        '{leads_data[i][9]}',
                        '{leads_data[i][10]}',
                        '{leads_data[i][11]}',
                        '{leads_data[i][12]}',
                        '{leads_data[i][13]}',
                        '{leads_data[i][14]}',
                        '{leads_data[i][15]}',
                        '{leads_data[i][16]}',
                        '{leads_data[i][17]}',
                        {leads_data[i][18]}
                WHERE NOT EXISTS ( 
                    SELECT lead_id
                    FROM XXXXXXXXXX
                    WHERE category_id = {leads_data[i][0]}
                            AND subcategory_id = {leads_data[i][2]}
                            AND lead_id = {leads_data[i][4]}
                            AND order_number = '{leads_data[i][5]}'
                            AND name = '{leads_data[i][6]}'
                            AND phone = '{leads_data[i][7]}'
                            AND comment = '{leads_data[i][8]}'
                            AND course_id = '{leads_data[i][15]}'
                            AND source_id = '{leads_data[i][16]}'
                            AND stuff_name = '{leads_data[i][17]}'
                    );"""
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")

print('crm data has been successfully extracted and loaded')