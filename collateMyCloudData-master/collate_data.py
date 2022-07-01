import json
import logging
import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def excel_to_csv(date_str: str, config: dict) -> None:
    ''' Saves a csv file for all excel files in given directory. '''

    # Creating a dict with fixed datatypes for importing excel files.
    data_types = {'Hotel Name': str, 'External Refrence #': str, 'Confirmation No':  int, 'Account Id':  int, 
                    'Invoice/Bill No.': str, 'Guest Name': str, 'Reservation Date': str, 'Created By': str, 
                    'Arrival Date': str, 'Departure Date': str, 'Room Type': str, 'Room no': str, 
                    'Adult Pax':  int, 'Youth Pax':  int, 'Child Pax':  int, 'Total Pax':  int, 'Reservation Status': str, 
                    'Confirmation Date': str, 'Reservation Changed on': str, 'Guest Email': str, 
                    'Guest Contact No': str, 'Guest City': str, 'Guest State': str, 'Guest Country': str, 'Nationality': str, 
                    'Rate Type': str, 'Booked Thru/Channel': str, 'Business Source': str, 'Market Segment': str, 
                    'Guest Class': str, 'System Rate': float, 'Agreed Rate': float, 'Date': str, 'Revenue Head': str, 
                    'Amount': float, 'Discount': float, 'Taxable': float, 'Taxes': float, 'Amount Payable': float}

    # Creating dataframe from excel files
    dataDir = os.path.join(config['download_dir'], date_str)
    dataFiles = [os.path.join(dataDir, f) for f in os.listdir(dataDir) if f.endswith('.xlsx')]
    df = pd.concat([pd.read_excel(fl, skiprows=3, header=1, skipfooter=1, 
                        index_col=False, engine='openpyxl', dtype=data_types) for fl in dataFiles])
    df.drop_duplicates(keep="first", inplace=True)

    # Changing datatype for date
    date_list = ['Reservation Date', 'Arrival Date', 'Departure Date', 'Confirmation Date', 'Reservation Changed on', 'Date']
    for entry in date_list:
        df[entry] = pd.to_datetime(df[entry])

    # Creating subset of dataframe by start and end date
    start_date = datetime.strptime(date_str, '%Y-%m-%d').replace(day=1)
    num_of_months = config['end_month_offset'] - config['start_month_offset'] + 1
    end_date = start_date + relativedelta(months=num_of_months)
    df = df[df['Date'] >= start_date]
    df = df[df['Date'] < end_date]
    df['Report Date'] = date_str

    # Saving dataframe as csv file with start date as name
    filename = os.path.join(dataDir, f"{date_str}_consolidated_data.csv")
    df.to_csv(filename, index=False)
    
    return


def create_summary(field_name: str, output_file_name: str, report_date: str, config: dict) -> None:
    ''' Creates a csv file based on field name given. '''

    file_path = os.path.join(config['download_dir'], report_date, f"{report_date}_consolidated_data.csv")
    df = pd.read_csv(file_path)

    df = df[df['Reservation Status'].isin([
        'CONFIRMED', 'CHECKED OUT', 'IN-HOUSE'])]
    df = df[df['Revenue Head'].isin([
        'ROOM CHARGE', 'MEAL - DINNER', 'MEAL - BREAKFAST', 'MEAL - LUNCH', 'HONEYMOON PACKAGE'])]
    df['Hotel Name'].replace({
        'Evolve Back Kuruba Safari Lodge, Kabini': 'EB Kabini',
        'Evolve Back Chikkana Halli Estate, Coorg': 'EB Coorg',
        'Evolve Back Kamalapura Palace, Hampi': 'EB Hampi'
    }, inplace=True)
    df['Month Year'] = pd.to_datetime(df['Date']).dt.to_period('M')

    df = (df.groupby(['Hotel Name', field_name, 'Month Year'])
            .agg(Amount=("Amount Payable", 'sum')).reset_index().round(2))
    df['Report Date'] = report_date

    # Saving as csv
    out_filename = os.path.join(config['download_dir'], report_date, f'{report_date}_{output_file_name}')
    df.to_csv(out_filename, index=False)


def main(date_str: str):
    # Reading config file
    with open('config.json') as f:
        config = json.load(f)

    # To convert all excel files to one csv file
    excel_to_csv(date_str, config)
    logger.info(f"Created {date_str}_consolidated_data.csv successfully!")

    # To create department summary
    create_summary('Market Segment', 'department_summary.csv', date_str, config)
    logger.info(f"Created {date_str}_department_summary.csv successfully!")

    # To create excutive summary
    create_summary('Business Source', 'executive_summary.csv', date_str, config)
    logger.info(f"Created {date_str}_executive_summary.csv successfully!")
