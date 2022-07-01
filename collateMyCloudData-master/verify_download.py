import json
import logging
import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_props_and_dates(date_str: str, config: dict) -> dict:
    ''' Returns a dictionary of property with details of each excel file. '''

    # Setting up directories
    data_dir = os.path.join(config['download_dir'], date_str)
    data_files = [os.path.join(data_dir,  f) for f in os.listdir(data_dir) if f.endswith('.xlsx')]

    # Creating prop dict from excel files
    prop_dict = {}
    for file in data_files:
        logger.info(f"Opening File: {file}")
        df = pd.read_excel(file, engine='openpyxl')
        info = str(df.iloc[1][0])
        hotel_id = info[-5:]
        date_from = datetime.strptime(info[11:21], "%d/%m/%Y")
        date_to = datetime.strptime(info[32:42], "%d/%m/%Y")
        prop_dict[hotel_id] = prop_dict.get(hotel_id, [])
        prop_dict[hotel_id].append([date_from, date_to, os.path.basename(file)])
    
    return prop_dict


def sanity_check(prop_dict: dict, num_months: int, start_date):
    ''' Checks all excel files with start and end dates. '''

    start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    for (key, value) in prop_dict.items():
        logger.info(f"Checking property: {key}")
        value.sort()
        end_date = start_date + relativedelta(months=num_months) - relativedelta(days=1)
        assert(value[0][0] == start_date)
        assert (value[len(value)-1][1] == end_date)
        for x in range(0, len(value)-1):
            assert value[x][1] + relativedelta(days=1) == value[x+1][0], "Processing file:" + value[x][2]


def main(date_str: str):
    # Reading config file
    with open('config.json') as f:
        config = json.load(f)

    prop_dict = get_props_and_dates(date_str, config)
    # print(prop_dict)
    num_of_months = config['end_month_offset'] - config['start_month_offset'] + 1
    sanity_check(prop_dict, num_of_months, date_str)
    logger.info("Sanity check passed!")
