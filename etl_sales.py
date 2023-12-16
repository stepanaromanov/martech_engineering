import importlib
import logging

connect = importlib.import_module("etl.connect")
extract = importlib.import_module("etl.extract")
transform = importlib.import_module("etl.transform")
load = importlib.import_module("etl.load")

#==================================================================================================================================================================
# SALES CRM LEADS
#==================================================================================================================================================================

try:
    token = connect.crm(
        login_type="login_main",
        password_type="password_main",
        referer_login_url_type="main_referer_login_url"
    )

    leads_object = extract.crm(
        token=token,
        target_url="leads_url",
        referer_url_type="main_referer_leads_url",
        params='XXXXXXXXXX'
    )

    leads_data = transform.sales_crm_leads(leads_object)

    load.sales_crm_leads(leads_data)

    logging.info("'SALES CRM LEADS' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle SALES CRM LEADS data: {error}")

    logging.info(f"'SALES CRM LEADS' data ETL experienced an error: {error}")


