# -*- coding: utf-8 -*-

import pandas as pd
import requests
import sys
import time
import logging
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO)


def requests_retry_session(retries=3, backoff_factor=0.7, status_forcelist=[500, 502, 504], session=None):
    """


    Parameters
    ----------
    retries : int
        Number of retries to allow.

    backoff_factor : float
        Backoff factor to apply between attempts.

    status_forcelist : list
        List of HTTP status codes to force a retry on.

    session : None


    Returns
    -------
    session : 
    
    """

    session = session or requests.Session()

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)

    session.mount('https://', adapter)

    return session


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

    table_url = f'{table_url}/count/json'

    t0 = time.time()
    try:  
        r = requests_retry_session().get(table_url)
        logging.info(f'{r.status_code}')
        # r = requests.get(table_url)

    except requests.exceptions.RequestException as e:
        logging.error(f'{e}\nTable url: {table_url}')
        sys.exit(1)

    try:
        row_count = r.json()[0]['TOTALQUERYRESULTS']

    except (IndexError, requests.exceptions.JSONDecodeError) as e:
        logging.error(f'Check API respose: {e}\n{r.status_code}')
        sys.exit(1)

    t1 = time.time()
    logging.info(f"That took {t1 - t0} seconds")

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

    table_url = f'{table_url}/rows/{start_end[0]}:{start_end[1]}/json'

    t0 = time.time()
    try:
        r_records = requests_retry_session().get(table_url)
        logging.info(f'{r_records.status_code}')
        # r_records = requests.get(table_url)

    except requests.exceptions.RequestException as e:

        logging.error(f'{e}\n{table_url}')
        sys.exit(1)

    try:

        json_data = pd.DataFrame(r_records.json())

    except requests.exceptions.JSONDecodeError as e:

        logging.error(f'{e}\nTable URL: {table_url}\n{r_records.content}')
        sys.exit(1)

    t1 = time.time()
    logging.info(f"That took {t1 - t0} seconds")

    return json_data


def get_GHGRP_records(reporting_year, table, rows=None, api_row_max=1000):
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

    api_row_max : int; default={1000}
        Maximum number of table rows to return at a time. 
        Envirofacts API for the GHGRP seems to be overwhelmed by > 1000 rows.
    Returns
    -------
    ghgrp : pandas.DataFrame
        DataFrame of GHGRP Envirofacts data.
    """

    if table[0:14] == 'V_GHG_EMITTER_':

        # EPA changed their table names
        if table == 'V_GHG_EMITTER_FACILITIES':

            table = 'RLPS_GHG_EMITTER_FACILITIES'

        elif table == 'V_GHG_EMITTER_SUBPART':

            table = 'RLPS_GHG_EMITTER_SUBPART'

        table_url = f'https://enviro.epa.gov/enviro/efservice/{table}/YEAR/{reporting_year}'

    else:

        table_url = f'https://enviro.epa.gov/enviro/efservice/{table}/REPORTING_YEAR/{reporting_year}'

    ghgrp = pd.DataFrame()

    if rows is None:

        nrecords = get_count(table_url)

        # API doesn't seem to be able to handle calls for more than 1000 rows at a time. 

        if nrecords > api_row_max:

            rrange = range(0, nrecords, api_row_max)

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

        if rows > api_row_max:

            rrange = range(0, rows, api_row_max)

            for n in range(len(rrange) - 1):

                json_data = get_records(table_url, [rrange[n], rrange[n + 1]])

                ghgrp = ghgrp.append(json_data)

            records_last = get_records(table_url, [rrange[-1], nrecords])

            ghgrp = ghgrp.append(records_last)

        # json_data = get_records(table_url, [0, rows])

        # ghgrp = ghgrp.append(json_data)

    ghgrp.drop_duplicates(inplace=True)

    ghgrp.columns = [c.upper() for c in ghgrp.columns]

    return ghgrp
