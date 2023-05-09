# -*- coding: utf-8 -*-

import pandas as pd
import requests
import xml.etree.ElementTree as et
import sys
import logging

logging.basicConfig(level=logging.INFO)

def xml_to_df(xml_root, table_name, df_columns):
    """
    EPA has changed the Envirofacts API.
    Converts elements of xml string obtained from EPA envirofacts (GHGRP)
    to a DataFrame.
    """


    data = []

    if table_name in ['V_GHG_EMITTER_SUBPART', 'V_GHG_EMITTER_FACILITIES']:

        for c in df_columns:
            data.append(
                [field.find(c).text for field in xml_root.findall(table_name)]
                )

    else:
        for c in df_columns:
            data.append([k.text for k in xml_root.iter(c)])

    rpd = pd.concat(
        [pd.Series(d).T for d in data], axis=1
        )

    rpd.columns = df_columns

    for c in rpd.columns:
        try:
            rpd.loc[:, c] = rpd[c].astype(float)

        except ValueError as e:
            logging.error(f'Formatting error for {table_name}: {e}')

    return rpd


def get_GHGRP_records(reporting_year, table, rows=None):
    """
    Return GHGRP data using EPA RESTful API based on specified reporting year
    and table. Tables of interest are C_FUEL_LEVEL_INFORMATION,
    D_FUEL_LEVEL_INFORMATION, c_configuration_level_info, and
    V_GHG_EMITTER_FACILITIES.
    Optional argument to specify number of table rows.
    """

    s = ""

#    max_retries = 25

    if table[0:14] == 'V_GHG_EMITTER_':

        table_url = ('https://enviro.epa.gov/enviro/efservice/', table,
                     '/YEAR/', str(reporting_year))

        table_url = "".join(table_url)

    else:

        table_url = ('https://enviro.epa.gov/enviro/efservice/', table,
                     '/REPORTING_YEAR/', str(reporting_year))

        table_url = "".join(table_url)

    r_columns = requests.get(table_url + '/rows/0:1')
    r_columns_root = et.fromstring(r_columns.content)

    ghgrp = pd.DataFrame(columns=[child.tag for child in r_columns_root[0]])

    if rows is None:

        try:  
            r = requests.get(table_url + '/count/json')  # Original used XML formatting
            # r = requests.get(table_url + '/count')  #

        except requests.exceptions.RequestException as e:
            print(e, table_url)
            sys.exit(1)

        logging.info(f'Count of {r.url} is {r.content}\nOriginal URL: {table_url}')
        # API has changed since original code.
        # tree = et.fromstring(r.content)

        # counts = \
        #     [[c for c in tree.iter(n)] for n in ['Count', 'RequestRecordCount']]

        try:

            counts = r.json()[0]['TOTALQUERYRESULTS']
            logging.info(f'JSON tree counts: {counts}')

        except IndexError as e:
            logging.error(f'Check API respose: {e}')


        # logging.info(f'XML tree counts: {counts}')
        

        # if counts[0] == []:
    
        #     nrecords = int(counts[1][0].text)

        nrecords = counts

        if nrecords > 10000:

            rrange = range(0, nrecords, 10000)

            for n in range(len(rrange) - 1):

                try:
                    r_records = requests.get(
                        f'{table_url}/rows/{rrange[n]}:{rrange[n + 1]}'
                        )

                except requests.exceptions.RequestException as e:

                    logging.error(f'{e}, {table_url}')
                    sys.exit(1)

                else:

                    records_root = et.fromstring(r_records.content)
                    r_df = xml_to_df(records_root, table, ghgrp.columns)
                    ghgrp = ghgrp.append(r_df)

            records_last = requests.get(
                f'{table_url}/rows/{rrange[-1]}:{nrecords}'
                )

            records_lroot = et.fromstring(records_last.content)
            rl_df = xml_to_df(records_lroot, table, ghgrp.columns)
            ghgrp = ghgrp.append(rl_df)

        else:

            try:
                r_records = \
                    requests.get(f'{table_url}/rows/0:{nrecords}')

            except requests.exceptions.RequestException as e:
                logging.error(f'{e}, {table_url}')
                sys.exit(1)

            else:

                records_root = et.fromstring(r_records.content)
                r_df = xml_to_df(records_root, table, ghgrp.columns)
                ghgrp = ghgrp.append(r_df)

    else:

        try:
            r_records = requests.get(f'{table_url}/rows/0:{rows}')

        except requests.exceptions.RequestException as e:
            logging.error(f'{e}, {table_url}')
            sys.exit(1)

        else:
            records_root = et.fromstring(r_records.content)
            r_df = xml_to_df(records_root, table, ghgrp.columns)
            ghgrp = ghgrp.append(r_df)

    ghgrp.drop_duplicates(inplace=True)

    return ghgrp
