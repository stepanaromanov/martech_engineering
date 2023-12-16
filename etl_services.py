import importlib
import logging

connect = importlib.import_module("etl.connect")
extract = importlib.import_module("etl.extract")
transform = importlib.import_module("etl.transform")
load = importlib.import_module("etl.load")

#==================================================================================================================================================================
# SERVICES CLIENT GROUPS ROOMS CAPACITY
#==================================================================================================================================================================

try:
    token = connect.crm(
        login_type="login_main",
        password_type="password_main",
        referer_login_url_type="main_referer_login_url"
    )

    rooms_capacity_object = extract.crm(
        token=token,
        target_url="groups_url",
        referer_url_type="referer_groups_url",
        params='XXXXXXXXX'
    )

    room_data = transform.services_rooms(rooms_capacity_object)

    load.services_rooms(room_data)

    logging.info("'SERVICES CLIENT GROUPS ROOMS CAPACITY' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle SERVICES CLIENT GROUPS ROOMS CAPACITY: {error}")

    logging.info(f"'SERVICES CLIENT GROUPS ROOMS CAPACITY' data ETL experienced an error: {error}")
