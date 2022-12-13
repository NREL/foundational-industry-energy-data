

import requests
import json
import logging
import re

logging.basicConfig(level=logging.INFO)

scc = 230302000012

def read_scc_csv(filepath):
    """
    Reads and formats existing SCC csv file and returns
    a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Filepath to scc csv file.

    Returns
    -------
    scc_data : pandas.DataFrame
        Dataframe of SCC codes.
    """

    scc_data = pd.read_csv(filepath, index_col='SCC')

    scc_data.columns = [x.replace(' ', '_') for x in scc_data.columns]

    return scc_data


def scc_query_split(scc):
    """
    Uses EPA SCC Web Services to get level information for an 8- or
    10-digit SCC.

    Parameters
    ----------
    scc : int
        Eight or 10-digit Source Classification Code.

    Returns
    -------
    scc_levels : dict
        Dictionary of all four level names
    """

    base_url = 'http://sor-scc-api.epa.gov:80/sccwebservices/v1/SCC/'

    try:
        r = requests.get(base_url+f'{scc}')
        r.raise_for_status()

    except requests.exceptions.HTTPError as err:
        logging.error(f'{err}')

    levels = [f'scc level {n}' for n in ['one', 'two', 'three', 'four']]

    scc_levels = {}

    try:
        for k in levels:
            scc_levels[k] = r.json()['attributes'][k]['text']

    except TypeError:
        logging.error(f'SCC {scc} cannot be found')
        scc_levels = None

    return scc_levels

scc_levels = scc_query_split(scc)

def pollutant_check():
    """
    """

    'CO2'
    'PM10-PRI'
    'SO2'

    ef_units = ['FT3', 'BTU', 'GAL']

    if re.search(r'(?<=E\d)\w+', 'ef denominator uom'):

        ef_denom_unit = re.search(r'(?<=E\d)\w+', 'ef denominator uom').group()

    else:
        pass

    ef_unit = f"{ef_numerator_uom}_per_{ef_denominator_uom}"

    # Unit conversions for EF numerator UOM to emissions UOM 
    numer_conver = {
        'LB_to_TON': 2000**-1,
        'MILLIGRM_to_LB': 0.0022046/1000,
        'MILLIGRM_to_TON': 0.0022046/1000/2000,
        'G_to_LB': 0.0022046,
        'TON_to_LB': 2000,
        'MEGAGRAM_to_TON': 1.102,
        'BTU_to_MJ': 1055*10**-6,
        'HP-HR_to_MJ': 2.685,
        'KW-HR_to_MJ': 3.6
        }

    # HHV from https://www.epa.gov/climateleadership/ghg-emission-factors-hub
    energy_conv = {
        'diesel': {
            'GAL_to_MJ': 0.138*1055,
            },
        'natural_gas': {
            'FT3_to_MJ': ,
            'THERM_to_MJ':
            }
        ''
    }

    if 'CO2' in ['pollutant code']:


        if 'emissions uom' == 'ef numerator uom':
            value = 'total emissions' / 'emission factor'
            units = 'ef denominator uom'

        else:
            if ('ef numerator uom' == 'LB') & ('emissions uom'=='TON'):
                value = 'total emissions'/ ('emission factor' / 2000)
                units = 'ef denominator uom'[3:]
                try:
