# pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
import os
import json
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.analytics.data_v1beta import BetaAnalyticsDataClient


def facebook_instagram():
    # Long lived access tokens
    # https://graph.facebook.com/{graph-api-version}/oauth/access_token?
    #    grant_type=fb_exchange_token&
    #    client_id={app-id}&
    #    client_secret={app-secret}& (secret from the app page > settings > basic > App Secret
    #    fb_exchange_token={your-access-token}

    with open('configuration/credentials/facebook.json') as fb:
        fb_credentials = json.load(fb)
    base_url = "https://graph.facebook.com/v18.0/"
    # target account and required fields
    request = "act_XXXXXXXXXX/insights?level=campaign&limit=5000&fields=campaign_id,adset_id,adset_name,campaign_name,spend,clicks,cpc,cpm,cpp,ctr,frequency,engagement_rate_ranking,impressions,quality_ranking,reach,conversions,objective&access_token="
    return base_url + request + fb_credentials["access_token_main"]


def google_analytics():
    # set up global variables
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './configuration/credentials/ga4.json'
    client = BetaAnalyticsDataClient()
    return client


def google_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    # get a new token or refresh existing one if it is expired
    gs_credentials = None
    if os.path.exists("configuration/tokens/google_sheets_token.json"):
        gs_credentials = Credentials.from_authorized_user_file("configuration/tokens/google_sheets_token.json", scopes)
    if not gs_credentials or not gs_credentials.valid:
        if gs_credentials and gs_credentials.expired and gs_credentials.refresh_token:
            gs_credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("configuration/credentials/google_sheets.json",
                                                             scopes)
            gs_credentials = flow.run_local_server(port=0)
        with open("configuration/tokens/google_sheets_token.json", "w") as token:
            token.write(gs_credentials.to_json())
    return gs_credentials


def crm(login_type, password_type, referer_login_url_type):
    with open("configuration/credentials/crm.json") as m:
        crm_credentials = json.load(m)

    # get credentials for token
    login = crm_credentials[login_type]
    password = crm_credentials[password_type]
    origin_url = crm_credentials["origin_url"]
    login_url = crm_credentials["login_url"]
    referer_login_url = crm_credentials[referer_login_url_type]

    # headers config
    login_headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Origin': origin_url,
        'Referer': referer_login_url,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    }

    payload = {
        "phone": login,
        "password": password,
        "relation_degree": 0,
    }

    # try to get token
    try:
        token_response = requests.post(login_url, data=payload, headers=login_headers)
        if token_response.ok:
            return token_response.json()['access_token']
    except Exception as error:
        print(f"Error '{error}' while getting token")