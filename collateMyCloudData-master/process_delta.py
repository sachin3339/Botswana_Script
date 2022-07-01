import json
import logging
import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_delta_csv(file1_str: str, file2_str: str, output_date: str, out_dir: str, config):
    df1 = pd.read_csv(file1_str)
    df2 = pd.read_csv(file2_str)

    # Creating path for output dir
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    # Creating a new column called Operation to indicate different 
    # dataframe type in single one.
    df1['Operation'] = ''
    df2['Operation'] = ''

    # To filter out data for end and start of month
    start_date = datetime.strptime(output_date, '%Y-%m-%d').replace(day=1)
    num_of_months = config['end_month_offset'] - config['start_month_offset'] + 1
    end_date = start_date + relativedelta(months=num_of_months)
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    df1 = df1[(df1['Date'] >= start_date) & (df1['Date'] < end_date)].copy()
    df2 = df2[(df2['Date'] >= start_date) & (df2['Date'] < end_date)].copy()
    df1 = df1[df1['Reservation Status'].isin(["IN-HOUSE", "CHECKED OUT", "CONFIRMED"])]
    df2 = df2[df2['Reservation Status'].isin(["IN-HOUSE", "CHECKED OUT", "CONFIRMED"])]


    # Drop the unnamed column.
    df1.rename({"Unnamed: 39":"a"}, axis="columns", inplace=True)
    df1.drop(["a"], axis=1, inplace=True)

    df2.rename({"Unnamed: 39":"a"}, axis="columns", inplace=True)
    df2.drop(["a"], axis=1, inplace=True)

    # Rename Hotel names
    # df1['Hotel Name'] = df1['Hotel Name'].replace(['Evolve Back Chikkana Halli Estate, Coorg',
    #                                                 'Evolve Back Kamalapura Palace, Hampi',
    #                                                 'Evolve Back Kuruba Safari Lodge, Kabini'],
    #                                                 ['EBH Coorg', 'EBH Hampi', 'EBH Kabini'])
    # df2['Hotel Name'] = df2['Hotel Name'].replace(['Evolve Back Chikkana Halli Estate, Coorg',
    #                                                 'Evolve Back Kamalapura Palace, Hampi',
    #                                                 'Evolve Back Kuruba Safari Lodge, Kabini'],
    #                                                 ['EBH Coorg', 'EBH Hampi', 'EBH Kabini'])

    # Creating new column of conf_key
    df1['conf_key'] = df1['Hotel Name'].astype(str) + df1['Account Id'].astype(str)
    df2['conf_key'] = df2['Hotel Name'].astype(str) + df2['Account Id'].astype(str)

    # Creating common dataframe b/w today and yesterday
    conf_common = df1.merge(df2, on=['conf_key'])

    # Creating new confirmations dataframe and saving as csv
    conf_df = df1[~df1.conf_key.isin(conf_common.conf_key)].copy()
    conf_df.drop(['conf_key'], axis=1, inplace=True)
    conf_df['Operation'] = 'Confirmation'

    # Creating cancellation dataframe and saving as csv
    cancel_df = df2[~df2.conf_key.isin(conf_common.conf_key)].copy()
    cancel_df.drop(['conf_key'], axis=1, inplace=True)
    cancel_df.loc[:, ['Report Date', 'Operation']] = [output_date, 'Cancellation']
    cancel_df.iloc[:, 34:39] = cancel_df.iloc[:, 34:39] * -1

    # Creating new column in both dataframe to check for modifications
    df1['mod_key'] = df1['conf_key'].astype(str) + df1['Account Id'].astype(str) + df1['Date'].astype(str) + \
                        df1['Revenue Head'].astype(str) + df1['Amount Payable'].astype(str) + \
                        df1['Invoice/Bill No.'].astype(str)

    df2['mod_key'] = df2['conf_key'].astype(str) + df2['Account Id'].astype(str) + df2['Date'].astype(str) + \
                        df2['Revenue Head'].astype(str) + df2['Amount Payable'].astype(str) + \
                        df2['Invoice/Bill No.'].astype(str)

    # Creating mod_common dataframe and getting positive and negative modifications.
    mod_common = df1.merge(df2, on=['mod_key'])
    mod_today = df1[~df1.mod_key.isin(mod_common.mod_key) & df1.conf_key.isin(conf_common.conf_key)].copy()
    mod_yesterday = df2[~df2.mod_key.isin(mod_common.mod_key) & df2.conf_key.isin(conf_common.conf_key)].copy()
    mod_yesterday.iloc[:, 34:39] = mod_yesterday.iloc[:, 34:39] * -1

    # Exporting dataframes as csv
    mod_yesterday.drop(['conf_key', 'mod_key'], axis=1, inplace=True)
    mod_yesterday['Operation'] = 'Modifcation'
    mod_today.drop(['conf_key', 'mod_key'], axis=1, inplace=True)
    mod_today['Operation'] = 'Modifcation'

    # Saving single dataframe as join of all 4 dataframes
    main_df = pd.concat([conf_df, cancel_df, mod_yesterday, mod_today], axis=0)
    main_df['Report Date'] = output_date
    main_df.to_csv(os.path.join(out_dir, output_date, f"{output_date}_delta_data.csv"), index=False)


def main(date_str: str):
    # Loading data path from config.json
    with open('config.json') as f:
        config = json.load(f)

    data_dir = config['download_dir']

    today = date_str
    yesterday = (datetime.strptime(date_str, '%Y-%m-%d') - relativedelta(days=1)).strftime('%Y-%m-%d')

    today_file = os.path.join(data_dir, today, f"{today}_consolidated_data.csv")
    yesterday_file = os.path.join(data_dir, yesterday, f"{yesterday}_consolidated_data.csv")
    
    # Check for yesterday's file
    if os.path.exists(yesterday_file):
        logger.info(f'Working for date: {today}')
        process_delta_csv(today_file, yesterday_file, today, data_dir, config)
        logger.info(f'Delta csv files created for: {today}')
    else:
            # Check for yesterday's files
        logger.warning("Yesterday's data not available! Skipping delta csv creation.")


def main_all():
    """ To run for all files in download data dir """
    
    # Loading data path from config.json
    with open('config.json') as f:
        config = json.load(f)

    data_dir = config['download_dir']

    file_list = [os.path.join(data_dir, i.split('_')[0], i)
                    for r, d, f in os.walk(data_dir)
                    for i in f 
                    if 'consolidated' in i]

    for x in range(len(file_list)-1):
        yesterday = file_list[x]
        today = file_list[x+1]
        logger.info(f'Working for file: {today}')

        if os.name == 'nt':
            output_date = today.split('\\')[-2]
        else:
            output_date = today.split('/')[-2]
        
        process_delta_csv(today, yesterday, output_date, data_dir)
    logger.info('Done Creating delta csv for all files!')
