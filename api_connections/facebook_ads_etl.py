import json
import requests
import psycopg2
from psycopg2 import OperationalError

#Long lived access tokens
#https://graph.facebook.com/{graph-api-version}/oauth/access_token?
#    grant_type=fb_exchange_token&
#    client_id={app-id}&
#    client_secret={app-secret}&
#    fb_exchange_token={your-access-token}

ACCESS_TOKEN = XXXXXXXXXX
BASEURL = "https://graph.facebook.com/v17.0/"

#target account and required fields
REQUEST = "act_XXXXXXXXXX/insights?level=adset&limit=5000&fields=campaign_id,adset_id,adset_name,campaign_name,spend&access_token="
URL = BASEURL + REQUEST + ACCESS_TOKEN

#get data
response = json.dumps(requests.get(URL).json())
data = json.loads(response)['data']

#then load it into postgreSQL
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
            CREATE TABLE IF NOT EXISTS facebook_ads_data (
                campaign_id text not null,
                adset_id text not null,
                adset_name text not null,
                campaign_name text not null,
                spend decimal not null,
                date_start text not null,
                date_stop text not null,
                last_updated timestamp not null
            );
        """
    )

    for i in range(0, len(data)):
        data[i]['adset_name'] = str(data[i]['adset_name']).replace("'", "")
        data[i]['campaign_name'] = str(data[i]['campaign_name']).replace("'", "")
        cur.execute(
            #information on spending is updated so we will delete old rows where campaign ids intersect; then we will insert new data
            f"""
            DELETE FROM facebook_ads_data
            WHERE campaign_id = '{data[i]['campaign_id']}';
            INSERT INTO facebook_ads_data (
                campaign_id, 
                adset_id, 
                adset_name, 
                campaign_name, 
                spend, 
                date_start, 
                date_stop,
                last_updated
            ) VALUES (
                '{data[i]['campaign_id']}',
                '{data[i]['adset_id']}',
                '{data[i]['adset_name']}',
                '{data[i]['campaign_name']}',
                {float(data[i]['spend'])},
                '{data[i]['date_start']}',
                '{data[i]['date_stop']}',
                NOW()::timestamp
            );"""
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")