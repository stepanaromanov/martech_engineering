import importlib
import logging

connect = importlib.import_module("etl.connect")
extract = importlib.import_module("etl.extract")
transform = importlib.import_module("etl.transform")
load = importlib.import_module("etl.load")

#==================================================================================================================================================================
# MARKETING DAILY LEADS
#==================================================================================================================================================================

try:
    credentials = connect.google_sheets()
    data = extract.marketing_daily_leads(
        credentials=credentials,
        spreadsheet_id="XXXXXXXXXX"
    )
    load.marketing_daily_leads(data)

    logging.info("'MARKETING DAILY LEADS' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle MARKETING DAILY LEADS data: {error}")

    logging.info(f"'MARKETING DAILY LEADS' data ETL experienced an error: {error}")

#==================================================================================================================================================================
# MARKETING FACEBOOK INSTAGRAM SPENDING
#==================================================================================================================================================================

try:
    token_url = connect.facebook_instagram()
    data = extract.marketing_facebook_instagram_spending(token_url=token_url)
    load.marketing_facebook_instagram_spending(data)

    logging.info("'MARKETING FACEBOOK INSTAGRAM SPENDING' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle MARKETING FACEBOOK INSTAGRAM SPENDING data: {error}")

    logging.info(f"'MARKETING FACEBOOK INSTAGRAM SPENDING' data ETL experienced an error: {error}")

#==================================================================================================================================================================
# MARKETING GOOGLE ANALYTICS
#==================================================================================================================================================================

# property_id = 'GA4_property_id'
property_id = 'XXXXXXXXXXX'

try:
    client = connect.google_analytics()

    sessions = extract.marketing_google_analytics_sessions(client=client, property_id=property_id)
    devices = extract.marketing_google_analytics_devices(client=client, property_id=property_id)
    page_paths = extract.marketing_google_analytics_page_paths(client=client, property_id=property_id)
    landing_pages = extract.marketing_google_analytics_landing_pages(client=client, property_id=property_id)

    load.marketing_google_analytics_sessions(sessions)
    load.marketing_google_analytics_devices(devices)
    load.marketing_google_analytics_page_paths(page_paths)
    load.marketing_google_analytics_landing_pages(landing_pages)

    logging.info("'MARKETING GOOGLE ANALYTICS' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle MARKETING FACEBOOK INSTAGRAM SPENDING data: {error}")

    logging.info(f"'MARKETING GOOGLE ANALYTICS' data ETL experienced an error: {error}")

