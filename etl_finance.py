import importlib
import logging

connect = importlib.import_module("etl.connect")
extract = importlib.import_module("etl.extract")
transform = importlib.import_module("etl.transform")
load = importlib.import_module("etl.load")


#==================================================================================================================================================================
# FINANCE COSTS
#==================================================================================================================================================================

try:
    credentials = connect.google_sheets()
    costs, salary_costs = extract.finance_costs(
        credentials=credentials,
        spreadsheet_id="XXXXXXXXXX"
    )
    load.finance_costs(costs,
                       salary_costs,
                       month="nov",
                       year=2023
    )

    logging.info("'FINANCE COSTS' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle FINANCE COSTS data: {error}")

    logging.info(f"'FINANCE COSTS' data ETL experienced an error: {error}")


#==================================================================================================================================================================
# FINANCE CLIENT GROUPS
#==================================================================================================================================================================

try:
    credentials = connect.google_sheets()
    data = extract.finance_client_groups(
        year=2023,
        credentials=credentials,
        spreadsheet_id="XXXXXXXXXX"
    )
    load.finance_client_groups(year=2023, dataframe=data)
    
    logging.info("'FINANCE CLIENT GROUPS' data extraction, transformation, loading successful")

except Exception as error:
    print(f"Could not handle 'FINANCE CLIENT GROUPS' data: {error}")
    
    logging.info(f"'FINANCE CLIENT GROUPS' data ETL experienced an error: {error}")
