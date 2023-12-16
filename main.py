import logging
logging.basicConfig(filename='logging/etl.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
)

import etl_finance
import etl_marketing
import etl_sales
import etl_services


