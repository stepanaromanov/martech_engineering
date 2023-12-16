import importlib
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import requests
import json
import re
time_functions = importlib.import_module("utils.time_functions")
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import OrderBy


def finance_costs(credentials, spreadsheet_id):
    # create column names, sheet names, and dataframes for data that will be imported
    xarajatlar_columns = [
        'sana',
        'xarajat_mazmuni',
        'summa',
        'manba'
    ]

    xarajatlar_sheets = [
        'Divident',
        'Kovorking',
        'Kreditlar',
        'Taksi',
        'Ovkatlanish',
        'Kanstovar',
        'Kofe brek',
        'Davr xarajat',
        'Reklama',
        'Investisiya',
        'Texnik inventar',
        'Paynet va aloka xizmatlari',
        'Tadbir',
        'Ukuvchiga kaytarildi',
        'HR brend',
        'Ustalar ish xaklari',
        'Arenda',
        'Xisobrakamga',
        'Daromad soligi',
        'IT park',
        'Nakd pul PK utkazildi',
        'Nakd pul PK dan olingan',
        'Bank xizmati',
        'Internet',
        'Eletor yenergiya'
    ]

    xodimlarning_xarajatlari_columns = [
        'sana',
        'xarajat_turi',
        'xarajat_mazmuni',
        'summa',
        'manba'
    ]

    xodimlarning_xarajatlari_sheets = [
        'Ukituvchi ish xaki',
        'Xodim ish xaki',
        'Ing ukituvchi oyliki'
    ]

    xarajatlar = pd.DataFrame(
        data=[],
        columns=xarajatlar_columns
    )

    xodimlarning_xarajatlari = pd.DataFrame(
        data=[],
        columns=xodimlarning_xarajatlari_columns
    )

    # connect to our spreadsheet file
    try:
        service = build("sheets", "v4", credentials=credentials)
        sheets = service.spreadsheets()
        # define names for existing sheets (sub datasets); every sheet consists of costs observation details
        for sheet in xarajatlar_sheets:
            # try to parse every sheet and create dataframe from it
            try:
                result = sheets.values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!B:E").execute()
                values = result.get("values", [])
                xarajatlar_df = pd.DataFrame(
                    data=values,
                    columns=xarajatlar_columns
                )
                # skip rows where date is missing
                filtered_sheet_df = xarajatlar_df[xarajatlar_df['sana'] != ''].dropna().tail(-4)
                filtered_sheet_df['kategoriya'] = sheet

                # concat sub dataframe with the main dataframe
                xarajatlar = pd.concat([xarajatlar, filtered_sheet_df], ignore_index=True)
            except:
                print(f"{sheet} could not be extracted")

        for sheet in xodimlarning_xarajatlari_sheets:
            # try to parse every sheet and create dataframe from it
            try:
                result = sheets.values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!B:F").execute()
                values = result.get("values", [])
                xodimlarning_xarajatlari_df = pd.DataFrame(
                    data=values,
                    columns=xodimlarning_xarajatlari_columns
                )
                # skip rows where date is missing
                filtered_sheet_df = xodimlarning_xarajatlari_df[
                    xodimlarning_xarajatlari_df['sana'] != ''].dropna().tail(-4)
                filtered_sheet_df['kategoriya'] = sheet

                # concat sub dataframe with the main dataframe
                xodimlarning_xarajatlari = pd.concat([xodimlarning_xarajatlari, filtered_sheet_df], ignore_index=True)
            except:
                print(f"{sheet} could not be extracted")

        # delete "\t" signs, apostrophes, spaces, and commas from columns
        xodimlarning_xarajatlari['xarajat_turi'].replace("'", "", inplace=True, regex=True)

        for df in (xarajatlar, xodimlarning_xarajatlari):
            df['xarajat_mazmuni'].replace("'", "", inplace=True, regex=True)
            df['xarajat_mazmuni'].replace("\t", " ", inplace=True, regex=True)
            df['summa'].replace(",00", "", inplace=True, regex=True)
            df['summa'] = df['summa'].apply(lambda x: re.sub(r"[^0-9]+", '', x))

        # create local copy of the data
        xarajatlar.to_csv(
            f"data_archive/finance_costs__{time_functions.get_current_datetime()}.csv",
            index=False)

        xodimlarning_xarajatlari.to_csv(
            f"data_archive/finance_salary_costs__{time_functions.get_current_datetime()}.csv",
            index=False)

        return xarajatlar, xodimlarning_xarajatlari

    except Exception as error:
        print(error)


def finance_client_groups(year, credentials, spreadsheet_id):
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
        for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            try:
                sheet = str(month) + "." + str(year)
                # parse every sheet and create dataframe from it
                result = sheets.values().get(spreadsheetId=spreadsheet_id, range=f"{sheet}!A:R").execute()
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

            except Exception as error:
                print(error)

    except Exception as error:
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

    # create local copy of the data
    output.to_csv(
        f"data_archive/finance_client_groups_{year}__{time_functions.get_current_datetime()}.csv",
        index=False)

    return output


def marketing_daily_leads(credentials, spreadsheet_id):
    # create column names for data that will be imported
    df_columns = [
        'Date',
        'IT Reception leads',
        'IT Telegram leads',
        'IT Instagram leads',
        'English Reception leads',
        'English Telegram leads',
        'English Instagram leads',
        'Kids Reception leads',
        'Kids Telegram leads',
        'Kids Instagram leads'
    ]

    # connect to the spreadsheet file
    try:
        service = build("sheets", "v4", credentials=credentials)
        sheets = service.spreadsheets()

        result = sheets.values().get(spreadsheetId=spreadsheet_id, range="A:J").execute()
        values = result.get("values", [])

        marketing_daily_leads = pd.DataFrame(
            data=values,
            columns=df_columns
        )
        marketing_daily_leads = marketing_daily_leads.dropna().tail(-1)
        for col in df_columns:
            if col == "Date":
                marketing_daily_leads[col] = marketing_daily_leads[col].astype(str)
            else:
                marketing_daily_leads[col] = marketing_daily_leads[col].astype(int)

        # create local copy of the data
        marketing_daily_leads.to_csv(
            f"data_archive/marketing_daily_leads__{time_functions.get_current_datetime()}.csv",
            index=False)

        return marketing_daily_leads

    except Exception as error:
        print(f"Could not extract the data from the spreadsheet: {error}")


def marketing_facebook_instagram_spending(token_url):
    # get data
    response = json.dumps(requests.get(token_url).json())
    data = json.loads(response)['data']

    # create dataframe from data
    output = pd.DataFrame(data=data,
                          columns=[
                              'campaign_id',
                              'campaign_name',
                              'spend',
                              'date_start',
                              'date_stop',
                              'clicks',
                              'cpc',
                              'cpm',
                              'cpp',
                              'ctr',
                              'frequency',
                              'engagement_rate_ranking',
                              'impressions',
                              'quality_ranking',
                              'reach',
                              'objective'
                          ])

    # fill NaN and absent values
    output['spend'].fillna(0, inplace=True)
    output['date_start'].fillna('NA', inplace=True)
    output['date_stop'].fillna('NA', inplace=True)
    output['clicks'].fillna(0, inplace=True)
    output['cpc'].fillna(0, inplace=True)
    output['cpm'].fillna(0, inplace=True)
    output['cpp'].fillna(0, inplace=True)
    output['ctr'].fillna(0, inplace=True)
    output['frequency'].fillna(0, inplace=True)
    output['engagement_rate_ranking'].fillna('NA', inplace=True)
    output['impressions'].fillna(0, inplace=True)
    output['quality_ranking'].fillna('NA', inplace=True)
    output['reach'].fillna(0, inplace=True)
    output['objective'].fillna('NA', inplace=True)
    output['campaign_name'].replace("'", "", inplace=True, regex=True)

    return output

def marketing_google_analytics_format_report(client, request):
    response = client.run_report(request)
    # create the index for rows
    row_index_names = [header.name for header in response.dimension_headers]
    row_header = []
    for i in range(len(row_index_names)):
        row_header.append([row.dimension_values[i].value for row in response.rows])

    row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names=np.array(row_index_names))

    # extract values and add them to our dataframe
    metric_names = [header.name for header in response.metric_headers]
    data_values = []
    for i in range(len(metric_names)):
        data_values.append([row.metric_values[i].value for row in response.rows])

    output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')),
                          index=row_index_named,
                          columns=metric_names)
    output.reset_index(inplace=True)
    output.replace("'", "", inplace=True, regex=True)

    # add weekly range if report contains 'year' and 'week' columns
    if 'year' in output.columns and 'week' in output.columns:
        output["dateRange"] = output.apply(lambda row: time_functions.get_date_range_from_week(row['year'], row['week']), axis=1)
    return output

def marketing_google_analytics_sessions(client, property_id):
    # set up dates
    start_date, end_date = time_functions.get_timedelta_90_days()

    # run request to get session dimensions
    sessionsRequest = RunReportRequest(
        property='properties/' + property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="sessionManualAdContent"),
                    Dimension(name="sessionManualTerm"),
                    ],
        metrics=[Metric(name="averageSessionDuration"),
                 Metric(name="activeUsers"),
                 Metric(name="bounceRate"),
                 Metric(name="eventCount"),
                 Metric(name="engagementRate"),
                 Metric(name="conversions"),
                 ],
        order_bys=[OrderBy(dimension={'dimension_name': 'year'}),
                   OrderBy(dimension={'dimension_name': 'month'}),
                   OrderBy(dimension={'dimension_name': 'week'}),
                   OrderBy(dimension={'dimension_name': 'sessionMedium'}),
                   OrderBy(dimension={'dimension_name': 'sessionSource'}),
                   OrderBy(metric={'metric_name': 'activeUsers'}, desc=True)
                   ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                               end_date=f"{end_date}")
                     ],
    )

    # format our sessions data response to the dataframe
    sessions = marketing_google_analytics_format_report(client, sessionsRequest)

    # create local copy of the data
    sessions.to_csv(
        f"data_archive/marketing_ga4_sessions__{time_functions.get_current_datetime()}.csv",
        index=False)

    return sessions


def marketing_google_analytics_devices(client, property_id):
    # set up dates
    start_date, end_date = time_functions.get_timedelta_90_days()

    # run request to get session dimensions
    devicesRequest = RunReportRequest(
        property='properties/' + property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="dateHour"),
                    Dimension(name="dayOfWeek"),
                    Dimension(name="deviceCategory"),
                    Dimension(name="deviceModel"),
                    ],
        metrics=[Metric(name="averageSessionDuration"),
                 Metric(name="activeUsers"),
                 Metric(name="bounceRate"),
                 Metric(name="eventCount"),
                 Metric(name="engagementRate"),
                 Metric(name="conversions"),
                 ],
        order_bys=[OrderBy(dimension={'dimension_name': 'year'}),
                   OrderBy(dimension={'dimension_name': 'month'}),
                   OrderBy(dimension={'dimension_name': 'week'}),
                   OrderBy(dimension={'dimension_name': 'dayOfWeek'}),
                   OrderBy(dimension={'dimension_name': 'dateHour'}),
                   OrderBy(metric={'metric_name': 'activeUsers'}, desc=True)
                   ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                               end_date=f"{end_date}")
                     ],
    )

    # format our sessions data response to the dataframe
    devices = marketing_google_analytics_format_report(client, devicesRequest)

    # create local copy of the data
    devices.to_csv(
        f"data_archive/marketing_ga4_devices__{time_functions.get_current_datetime()}.csv",
        index=False)

    return devices


def marketing_google_analytics_page_paths(client, property_id):
    # set up dates
    start_date, end_date = time_functions.get_timedelta_90_days()

    # run request to get page paths dimensions
    pagePathsRequest = RunReportRequest(
        property='properties/' + property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="pagePath"),
                    ],
        metrics=[Metric(name="activeUsers")],
        order_bys=[OrderBy(dimension={'dimension_name': 'year'}),
                   OrderBy(dimension={'dimension_name': 'month'}),
                   OrderBy(dimension={'dimension_name': 'week'}),
                   OrderBy(metric={'metric_name': 'activeUsers'}, desc=True)
                   ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                               end_date=f"{end_date}"),
                     ],
    )

    # format our sessions data response to the dataframe
    page_paths = marketing_google_analytics_format_report(client, pagePathsRequest)

    # create local copy of the data
    page_paths.to_csv(
        f"data_archive/marketing_ga4_page_paths__{time_functions.get_current_datetime()}.csv",
        index=False)

    return page_paths


def marketing_google_analytics_landing_pages(client, property_id):
    # set up dates
    start_date, end_date = time_functions.get_timedelta_90_days()

    landingPagesRequest = RunReportRequest(
        property='properties/' + property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="landingPage"),
                    ],
        metrics=[Metric(name="activeUsers")],
        order_bys=[OrderBy(dimension={'dimension_name': 'year'}),
                   OrderBy(dimension={'dimension_name': 'month'}),
                   OrderBy(dimension={'dimension_name': 'week'}),
                   OrderBy(metric={'metric_name': 'activeUsers'}, desc=True)
                   ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                               end_date=f"{end_date}"),
                     ],
    )

    # format our sessions data response to the dataframe
    landing_pages = marketing_google_analytics_format_report(client, landingPagesRequest)

    # create local copy of the data
    landing_pages.to_csv(
        f"data_archive/marketing_ga4_landing_pages__{time_functions.get_current_datetime()}.csv",
        index=False)

    return landing_pages


def crm(token, target_url, referer_url_type, params):
    with open("configuration/credentials/crm.json") as m:
        crm_credentials = json.load(m)

    # get credentials
    target_url = crm_credentials[target_url] + params
    origin_url = crm_credentials["origin_url"]
    referer_url = crm_credentials[referer_url_type]

    # headers config for getting leads
    leads_headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Origin': origin_url,
        'Referer': referer_url,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Authorization': 'Bearer {}'.format(token),
    }

    # getting json file with data
    try:
        leads_response = requests.get(target_url, headers=leads_headers)
        if leads_response.ok:
            return leads_response.json()['data']
    except Exception as error:
        print(f"Error '{error}' while getting leads")


def sales_crm_leads(token, target_url, referer_url_type, params):
    with open("configuration/credentials/crm.json") as m:
        crm_credentials = json.load(m)

    # get credentials
    target_url = crm_credentials[target_url] + params
    origin_url = crm_credentials["origin_url"]
    referer_url = crm_credentials[referer_url_type]

    # headers config for getting leads
    leads_headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Origin': origin_url,
        'Referer': referer_url,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Authorization': 'Bearer {}'.format(token),
    }

    # getting json file with leads
    try:
        leads_response = requests.get(target_url, headers=leads_headers)
        if leads_response.ok:
            return leads_response.json()['data']
    except Exception as error:
        print(f"Error '{error}' while getting leads")

