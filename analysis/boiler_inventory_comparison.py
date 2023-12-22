

import pandas as pd
import numpy as np
import pathlib


def fix_fips(fips):
    """
    Correct float formatting for county FIPS.

    Parameters
    ----------
    fips : str
        County FIPS in original formatting.

    Returns
    -------
    fips : str
        County FIPS, corrected and re-formatted
    """

    try:
        fips = fips.replace('.0', '')

    except AttributeError:
        fips = str(fips)
        fips = fips.replace('.0', '')

    len_missing = 5 - len(fips)

    if len_missing > 0:
        fips = '0'*len_missing+fips

    else:
        pass

    return fips

def convert_units(x, unit):
    """
    Convert energy units from MMBtu to MJ and
    power units from MMBtu/hr to MW.

    Parameters
    ----------
    x : float
        Value to be converted

    unit : {'power', 'energy'}
        Type of unit to convert.

    Returns
    -------
    y : float
        Converted unit, or None
    """

    units = {
        'power': 0.29307107,  # convert to MW
        'energy': 1054.35  # convert to MJ
        }
    
    try:
        y = x * units[unit]

    except TypeError:
        y = np.nan

    return y


def get_boiler_data(boiler_url):
    """
    Download Northwestern boiler inventory and return it as a dataframe.

    Parameters
    ----------
    boiler_url : str
        URL for Northwestern boiler inventory


    Returns
    -------
    bdb : pandas.DataFrame
        Boiler inventory as a pandas DataFrame

    """

    dtypes = {
        'state': str,
        'county': str,
        'company_name': str,
        'site_name': str,
        'naics_code': int,
        'zip_code': str,
        'eis_unit_id': float,
        'naics_sub': int,
        'fuel_type': str,
        'data_source': str,
        'REPORTING_YEAR': int,
        'UNIT_TYPE': str,
        'TIER': str,
        'FUEL_COM': float,
        'FUEL_UNIT': str,
        'ENERGY_MMBtu_hr': float,
        'Op Hours Per Year': object
        }
    
    converters = {
        'cap_mmbtuhr': lambda x: convert_units(x, 'power'),  # convert to MW
        'ENERGY_COM_MMBtu': lambda x: convert_units(x, 'energy'),  # convert to MJ
        'fips': lambda x: (fix_fips(x)),
        'ENERGY_MMBtu_hr': lambda x: convert_units(x, 'power'),  # convert to MW
        }
    
    col_rename = {
        'ENERGY_COM_MMBtu': 'energyMJ',
        'fips': 'countyFIPS'   
    }

    use_cols = [0, 1, 2, 3, 4, 7, 9, 10, 13, 15, 16, 17, 18]

    bdb = pd.read_csv(boiler_url, dtype=dtypes, sep=',',
                      converters=converters,
                      usecols=use_cols, na_values=['', 'na'])
    
    bdb.rename(columns=col_rename, inplace=True)
    
    bdb.loc[:, 'designCapacity'] = np.nan
    bdb.designCapacity.update(bdb.cap_mmbtuhr)
    bdb.designCapacity.update(bdb.ENERGY_MMBtu_hr)

    return bdb

def get_fied_boiler(fied_path):

    """
    
    """

    fied_boiler = pd.read_csv(fied_path, converters={'fips': lambda x: (fix_fips(x))})
    fied_boiler = fied_boiler.query("unitTypeStd == 'boiler'").copy(deep=True)
    fied_boiler.loc[:, 'countyFIPS'] = fied_boiler.countyFIPS.apply(lambda x: fix_fips(x))

    return fied_boiler


def compare_boilers(boiler_url, fied_path):
    """
    
    """
    bdb = get_boiler_data(boiler_url)
    fied_boiler = get_fied_boiler(fied_path)

    comparison = pd.concat(
        [bdb.groupby('countyFIPS').designCapacity.sum(),
         fied_boiler.groupby('countyFIPS').designCapacity.sum(),
         bdb.groupby('countyFIPS').energyMJ.sum(),
         fied_boiler.groupby('countyFIPS')[['energyMJ', 'energyMJq2']].sum().sum(axis=1)],
        axis=1
        )
    
    comparison.columns = ['designCapacity_bdb', 'designCapacity_fied', 'energyMJ_bdb', 'energyMJ_fied']

    return comparison

if __name__ == '__main__':

    boiler_url = 'https://raw.githubusercontent.com/carriescho/Electrification-of-Boilers/master/total_boiler_inventory.csv'
    fied_path = pathlib.Path('foundational_industry_data_2017.csv.gz')

    comparison = compare_boilers(boiler_url, fied_path)
    print(comparison.head())
