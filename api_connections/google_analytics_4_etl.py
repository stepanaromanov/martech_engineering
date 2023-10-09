
# pip install psycopg2-binary
# pip install numpy
# pip install pandas
# pip install google-analytics-data

# import all required libraries
import psycopg2
from psycopg2 import OperationalError
from datetime import date
import datetime
import numpy as np
import pandas as pd
import os

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import OrderBy

# set up global variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './credentials/ga4.json'

# property_id = 'GA4_property_id'
property_id = XXXXXXXXXX

client = BetaAnalyticsDataClient()

# set up dates
start_date = date.today() - datetime.timedelta(90)
end_date = date.today()

# define the function to find weekly date range for particular date. It will be used in graphs
def getDateRangeFromWeek(p_year, p_week):
    firstdayofweek = datetime.datetime.strptime(f'{p_year}/W{int(p_week )}/1', "%Y/W%W/%w").date()
    lastdayofweek = firstdayofweek + datetime.timedelta(days=6.9)
    return f'{firstdayofweek.strftime("%Y/%m/%d")} - {lastdayofweek.strftime("%Y/%m/%d")}'


# Format Report - run_report method
def format_report(request):
    response = client.run_report(request)
    # create the index for rows
    row_index_names = [header.name for header in response.dimension_headers]
    row_header = []
    for i in range(len(row_index_names)):
        row_header.append([row.dimension_values[i].value for row in response.rows])

    row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names = np.array(row_index_names))

    # extract values and add them to our dataframe
    metric_names = [header.name for header in response.metric_headers]
    data_values = []
    for i in range(len(metric_names)):
        data_values.append([row.metric_values[i].value for row in response.rows])

    output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')),
                          index=row_index_named,
                          columns=metric_names)
    output.reset_index(inplace=True)

    # add weekly range if report contains 'year' and 'week' columns
    if 'year' in output.columns and 'week' in output.columns:
        output["dateRange"] = output.apply(lambda row: getDateRangeFromWeek(row['year'], row['week']), axis=1)
    return output

# run request to get session dimensions
sessionsRequest = RunReportRequest(
            property='properties/'+property_id,
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
                         OrderBy(metric = {'metric_name': 'activeUsers'}, desc = True)
                        ],
            date_ranges=[DateRange(start_date=f"{start_date}",
                                   end_date=f"{end_date}")
                        ],
)

# format our sessions response to dataframe
sessions = format_report(sessionsRequest)

#connect to local PostgreSQL database
try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database=XXXXXXXXXX,
        user=XXXXXXXXXX,
        password=XXXXXXXXXX,
    )

    cur = conn.cursor()

    # Create table to store sessions data
    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS ga_datatalim_sessions (
                year smallserial not null,
                month smallserial not null,
                week smallserial not null,
                session_medium text not null,
                session_source text not null,
                session_campaign_name text not null,
                session_manual_ad_content text not null,
                session_manual_term text not null,
                average_session_duration numeric not null,
                active_users smallserial not null,
                bounce_rate numeric not null,
                event_count numeric not null,
                engagement_rate numeric not null,
                conversions numeric not null,
                date_range text not null
            );
        """
    )

    for i in range(0, len(sessions)):
        cur.execute(
            # information about visitors is updated, so we will delete old rows where data intersects; then we will insert new data
            f"""
                DELETE FROM ga_datatalim_sessions
                WHERE week = {sessions['week'][i]} and year = {sessions['year'][i]};
                INSERT INTO ga_datatalim_sessions (
                    year,
                    month,
                    week,
                    session_medium,
                    session_source,
                    session_campaign_name,
                    session_manual_ad_content,
                    session_manual_term,
                    average_session_duration,
                    active_users,
                    bounce_rate,
                    event_count,
                    engagement_rate,
                    conversions,
                    date_range
                ) VALUES (
                    {sessions['year'][i]},
                    {sessions['month'][i]},
                    {sessions['week'][i]},
                    '{sessions['sessionMedium'][i]}',
                    '{sessions['sessionSource'][i]}',
                    '{sessions['sessionCampaignName'][i]}',
                    '{sessions['sessionManualAdContent'][i]}',
                    '{sessions['sessionManualTerm'][i]}',
                    {sessions['averageSessionDuration'][i]},
                    {sessions['activeUsers'][i]},
                    {sessions['bounceRate'][i]},
                    {sessions['eventCount'][i]},
                    {sessions['engagementRate'][i]},
                    {sessions['conversions'][i]},
                    '{sessions['dateRange'][i]}'
                );
            """
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")

# run request to get device dimensions
devicesRequest = RunReportRequest(
            property='properties/'+property_id,
            dimensions=[Dimension(name="year"),
                        Dimension(name="month"),
                        Dimension(name="week"),
                        Dimension(name="dateHour"),
                        Dimension(name="dayOfWeek"),
                        Dimension(name="deviceCategory"),
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
                         OrderBy(metric = {'metric_name': 'activeUsers'}, desc = True)
                        ],
            date_ranges=[DateRange(start_date=f"{start_date}",
                                   end_date=f"{end_date}")
                        ],
)

# format request to devices dataframe
devices = format_report(devicesRequest)

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
            CREATE TABLE IF NOT EXISTS ga_datatalim_devices (
                year smallserial not null,
                month smallserial not null,
                week smallserial not null,
                date_hour text not null,
                day_of_week numeric not null,
                device_category text not null,
                average_session_duration numeric not null,
                active_users smallserial not null,
                bounce_rate numeric not null,
                event_count numeric not null,
                engagement_rate numeric not null,
                conversions numeric not null,
                date_range text not null
            );
        """
    )

    for i in range(0, len(devices)):
        cur.execute(
            #information about visitors is updated so we will delete old rows where data intersects; then we will insert new data
            f"""
                DELETE FROM ga_datatalim_devices
                WHERE week = {devices['week'][i]} and year = {devices['year'][i]};
                INSERT INTO ga_datatalim_devices (
                    year,
                    month,
                    week,
                    date_hour,
                    day_of_week,
                    device_category,
                    average_session_duration,
                    active_users,
                    bounce_rate,
                    event_count,
                    engagement_rate,
                    conversions,
                    date_range
                ) VALUES (
                    {devices['year'][i]},
                    {devices['month'][i]},
                    {devices['week'][i]},
                    '{devices['dateHour'][i]}',
                    {devices['dayOfWeek'][i]},
                    '{devices['deviceCategory'][i]}',
                    {devices['averageSessionDuration'][i]},
                    {devices['activeUsers'][i]},
                    {devices['bounceRate'][i]},
                    {devices['eventCount'][i]},
                    {devices['engagementRate'][i]},
                    {devices['conversions'][i]},
                    '{devices['dateRange'][i]}'
                );
            """
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")

# run request to get page path dimensions
pagePathsRequest = RunReportRequest(
        property='properties/'+property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="pagePath"),
                   ],
        metrics=[Metric(name="activeUsers")],
        order_bys = [OrderBy(dimension={'dimension_name': 'year'}),
                     OrderBy(dimension={'dimension_name': 'month'}),
                     OrderBy(dimension={'dimension_name': 'week'}),
                     OrderBy(metric = {'metric_name': 'activeUsers'}, desc = True)
                    ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                                end_date=f"{end_date}"),
                    ],
)

# format response to page paths dataframe
pagePaths = format_report(pagePathsRequest)

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
            CREATE TABLE IF NOT EXISTS ga_datatalim_page_paths (
                year smallserial not null,
                month smallserial not null,
                week smallserial not null,
                page_path text not null,
                active_users smallserial not null,
                date_range text not null
            );
        """
    )

    for i in range(0, len(pagePaths)):
        cur.execute(
            #information about visitors is updated so we will delete old rows where data intersects; then we will insert new data
            f"""
                DELETE FROM ga_datatalim_page_paths
                WHERE week = {pagePaths['week'][i]} and year = {pagePaths['year'][i]};
                INSERT INTO ga_datatalim_page_paths (
                    year,
                    month,
                    week,
                    page_path,
                    active_users,
                    date_range
                ) VALUES (
                    {pagePaths['year'][i]},
                    {pagePaths['month'][i]},
                    {pagePaths['week'][i]},
                    '{pagePaths['pagePath'][i]}',
                    {pagePaths['activeUsers'][i]},
                    '{pagePaths['dateRange'][i]}'
                );
            """
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")


# run request to get landing page dimensions
landingPagesRequest = RunReportRequest(
        property='properties/'+property_id,
        dimensions=[Dimension(name="year"),
                    Dimension(name="month"),
                    Dimension(name="week"),
                    Dimension(name="landingPage"),
                   ],
        metrics=[Metric(name="activeUsers")],
        order_bys = [OrderBy(dimension={'dimension_name': 'year'}),
                     OrderBy(dimension={'dimension_name': 'month'}),
                     OrderBy(dimension={'dimension_name': 'week'}),
                     OrderBy(metric = {'metric_name': 'activeUsers'}, desc = True)
                    ],
        date_ranges=[DateRange(start_date=f"{start_date}",
                                end_date=f"{end_date}"),
                    ],
)

# format response to landing pages dataframe
landingPages = format_report(landingPagesRequest)

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
            CREATE TABLE IF NOT EXISTS ga_datatalim_landing_pages (
                year smallserial not null,
                month smallserial not null,
                week smallserial not null,
                landing_page text not null,
                active_users smallserial not null,
                date_range text not null
            );
        """
    )

    for i in range(0, len(landingPages)):
        cur.execute(
            # information about visitors is updated so we will delete old rows where data intersects; then we will insert new data
            f"""
                DELETE FROM ga_datatalim_landing_pages
                WHERE week = {landingPages['week'][i]} and year = {landingPages['year'][i]};
                INSERT INTO ga_datatalim_landing_pages (
                    year,
                    month,
                    week,
                    landing_page,
                    active_users,
                    date_range
                ) VALUES (
                    {landingPages['year'][i]},
                    {landingPages['month'][i]},
                    {landingPages['week'][i]},
                    '{landingPages['landingPage'][i]}',
                    {landingPages['activeUsers'][i]},
                    '{landingPages['dateRange'][i]}'
                );
            """
        )

    conn.commit()
    cur.close()
    conn.close()

except OperationalError as error:
    print(f"The error '{error}' occurred")