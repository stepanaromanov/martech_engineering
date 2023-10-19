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
BASEURL = "https://graph.facebook.com/v18.0/"

#target account and required fields
REQUEST = "act_XXXXXXXXXX/insights?level=campaign&limit=5000&fields=campaign_id,campaign_name,spend,clicks,cpc,cpm,cpp,ctr,frequency,engagement_rate_ranking,impressions,quality_ranking,reach,objective&access_token="
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
            CREATE TABLE IF NOT EXISTS facebook_ads (
                campaign_id text not null,
                campaign_name text not null,
                spend decimal not null,
                date_start text not null,
                date_stop text not null,
                clicks numeric not null,
                cpc numeric not null,
                cpm numeric not null,
                cpp numeric,
                ctr numeric not null,
                frequency numeric not null,
                engagement_rate_ranking text,
                impressions numeric,
                quality_ranking text,
                reach numeric not null,
                objective text not null,
                last_updated timestamp not null
            );
        """
    )

    for i in range(0, len(data)):
        data[i]['campaign_name'] = str(data[i]['campaign_name']).replace("'", "")
        cur.execute(
            #information on spending is updated so we will delete old rows where campaign ids intersect; then we will insert new data
            f"""
            DELETE FROM facebook_ads
            WHERE campaign_id = '{data[i]['campaign_id']}';
            INSERT INTO facebook_ads (
                campaign_id, 
                campaign_name, 
                spend, 
                date_start, 
                date_stop,
                clicks,
                cpc,
                cpm,
                cpp,
                ctr,
                frequency,
                engagement_rate_ranking,
                impressions,
                quality_ranking,
                reach,
                objective,
                last_updated
            ) VALUES (
                '{data[i]['campaign_id']}',
                '{data[i]['campaign_name']}',
                {float(data[i]['spend'])},
                '{data[i]['date_start']}',
                '{data[i]['date_stop']}',
                {float(data[i]['clicks'])},
                {float(data[i]['cpc'])},
                {float(data[i]['cpm'])},
                {float(data[i]['cpp'])},
                {float(data[i]['ctr'])},
                {float(data[i]['frequency'])},
                '{data[i]['engagement_rate_ranking']}',
                {float(data[i]['impressions'])},
                '{data[i]['quality_ranking']}',
                {float(data[i]['reach'])},
                '{data[i]['objective']}',
                NOW()::timestamp
            );"""
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")

print('facebook ads data has been successfully extracted and loaded')