# -*- coding: utf-8 -*-

import pandas as pd
import requests
import sys
import logging

logging.basicConfig(level=logging.INFO)


def get_count(table_url):
    """
    Get the number of rows for a specified GHGRP table (via url)
    from EPA Envirofacts.

    Parameters
    ----------
    table_url : str
        URL for Envirofacts API


    Returns
    -------
    row_count : int
        Count of table rows. 
    
    """

    try:  
        r = requests.get(table_url + '/count/json')

    except requests.exceptions.RequestException as e:
        logging.error(f'{e}\nTable url: {table_url}')
        sys.exit(1)

    try:
        row_count = r.json()[0]['TOTALQUERYRESULTS']

    except (IndexError, requests.exceptions.JSONDecodeError) as e:
        logging.error(f'Check API respose: {e}\n{r.status_code}')
        sys.exit(1)

    return row_count

def get_records(table_url, start_end):
    """
    Get specified rows for a specified GHGRP table (via url)
    from EPA Envirofacts.

    Parameters
    ----------
    table_url : str
        URL for Envirofacts API

    start_end : list of integers
        List indicating the starting and ending row to get
    (e.g., [0, 1000])

    Returns
    -------
    records_df : pandas.DataFrame
        DataFrame of records from Envirofacts API.
    
    """

    try:

        r_records = requests.get(
            f'{table_url}/rows/{start_end[0]}:{start_end[1]}/json'
            )

    except requests.exceptions.RequestException as e:

        logging.error(f'{e}, {table_url}')
        sys.exit(1)

    try:

        json_data = pd.DataFrame(r_records.json())

    except requests.exceptions.JSONDecodeError:

        logging.error(f'Table URL: {table_url}\n{r_records.content}')
        sys.exit(1)

    return json_data


def get_GHGRP_records(reporting_year, table, rows=None):
    """
    Return GHGRP data using EPA RESTful API based on specified reporting year
    and table. Tables of interest are C_FUEL_LEVEL_INFORMATION,
    D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and
    V_GHG_EMITTER_FACILITIES.
    Optional argument to specify number of table rows.

    Parameters
    ----------
    reporting_year : int
        Reporting year of GHGRP data

    table : str
        Name of GHGRP Envirofacts table to retrieve records from

    rows : int; default=None
        Number of table rows to retrieve, beginning at row 0.

    Returns
    -------
    ghgrp : pandas.DataFrame
        DataFrame of GHGRP Envirofacts data.
    """

    if table[0:14] == 'V_GHG_EMITTER_':

        table_url = f'https://enviro.epa.gov/enviro/efservice/{table}/YEAR/{reporting_year}'

    else:

        table_url = f'https://enviro.epa.gov/enviro/efservice/{table}/REPORTING_YEAR/{reporting_year}'

    ghgrp = pd.DataFrame()

    if rows is None:

        nrecords = get_count(table_url)

        if nrecords > 10000:

            rrange = range(0, nrecords, 10000)

            for n in range(len(rrange) - 1):

                json_data = get_records(table_url, [rrange[n], rrange[n + 1]])

                ghgrp = ghgrp.append(json_data)

            records_last = get_records(table_url, [rrange[-1], nrecords])

            ghgrp = ghgrp.append(records_last)

        else:

            json_data = get_records(table_url, [0, nrecords])

            ghgrp = ghgrp.append(json_data)

            try:
                r_records = \
                    requests.get(f'{table_url}/rows/0:{nrecords}/json')

            except requests.exceptions.RequestException as e:
                logging.error(f'{e}, {table_url}')
                sys.exit(1)

            try:

                json_data = pd.DataFrame(r_records.json())

            except requests.exceptions.JSONDecodeError:

                logging.error(f'{r_records.content}')
            
            else:

                ghgrp = ghgrp.append(json_data)

    else:

        json_data = get_records(table_url, [0, rows])

        ghgrp = ghgrp.append(json_data)

    ghgrp.drop_duplicates(inplace=True)

    ghgrp.columns = [c.upper() for c in ghgrp.columns]

    return ghgrp
