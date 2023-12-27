

import pandas as pd
import numpy as np
import pathlib
import json
from urllib.request import urlopen
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio


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
        'naics_code': float,
        'zip_code': str,
        'eis_unit_id': float,
        'naics_sub': int,
        'fuel_type': str,
        'data_source': str,
        'REPORTING_YEAR': float,
        'UNIT_TYPE': str,
        'TIER': str,
        'FUEL_COM': float,
        'FUEL_UNIT': str,
        'Op Hours Per Year': object
        }
    
    converters = {
        'fips': lambda x: (fix_fips(x)),
        }
    
    col_rename = {
        'ENERGY_COM_MMBtu': 'energyMJ',
        'fips': 'countyFIPS',
        'naics_code': 'naicsCode'   
    }

    # use_cols = [0, 1, 2, 3, 4, 7, 9, 10, 13, 15, 16, 17, 18]

    bdb = pd.read_csv(boiler_url, dtype=dtypes, sep=',',
                      converters=converters, na_values=['', 'na'])
    
    for c in ['cap_mmbtuhr', 'ENERGY_COM_MMBtu', 'ENERGY_MMBtu_hr']:

        if c =='ENERGY_COM_MMBtu':
            bdb.loc[:, c] = bdb[c].apply(lambda x: convert_units(x, 'energy'))

        else:
            bdb.loc[:, c] = bdb[c].apply(lambda x: convert_units(x, 'power'))

    bdb.rename(columns=col_rename, inplace=True)
    
    bdb.loc[:, 'designCapacity'] = np.nan
    bdb.designCapacity.update(bdb.cap_mmbtuhr)
    bdb.designCapacity.update(bdb.ENERGY_MMBtu_hr)

    return bdb

def get_fied_boiler(fied_path):

    """
    Load FIED and return information only on boilers. 

    Parameters
    ----------
    fied_path : str


    Returns
    -------
    fied_boiler : pandas.DataFrame
    """

    fied_boiler = pd.read_csv(fied_path, 
                              converters={'fips': lambda x: (fix_fips(x))},
                              low_memory=False,
                              index_col=[0])
    fied_boiler = fied_boiler.query("unitTypeStd == 'boiler'").copy(deep=True)
    fied_boiler.loc[:, 'countyFIPS'] = fied_boiler.countyFIPS.apply(lambda x: fix_fips(x))

    # Drop territories
    fied_boiler = fied_boiler.where(~fied_boiler.stateCode.isin(['VI', 'PR'])).dropna(how='all')
    
    naics_sub = pd.DataFrame(fied_boiler.naicsCode.dropna().unique())
    naics_sub.columns = ['naicsCode']

    naics_sub.loc[:, 'naics_sub'] = naics_sub.naicsCode.apply(
        lambda x: int(str(x)[0:3])
        )
    
    fied_boiler = pd.merge(
        fied_boiler, naics_sub, on='naicsCode',
        how='left'
        )

    return fied_boiler


def compare_boilers(bdb, fied_boiler, compare_type=None):
    """
    Compare either county or NAICS (3-digit) sums of 
    design capacity and energy estimates.
    
    Parameters
    ----------
    bdb : pandas.DataFrame

    fied_boiler : pandas.DataFrame

    type : str, {'county', 'naics'}
        Aggregate data sets at either county- or naics-level.


    Returns
    -------
    comparison : pandas.DataFrame
    """

    grouping_column = {'county': 'countyFIPS', 'naics': 'naics_sub'}


    comparison = pd.concat(
        [bdb.groupby(grouping_column[compare_type]).designCapacity.sum(),
         fied_boiler.groupby(grouping_column[compare_type]).designCapacity.sum(),
         bdb.groupby(grouping_column[compare_type]).energyMJ.sum(),
         fied_boiler.groupby(grouping_column[compare_type])[['energyMJ', 'energyMJq2']].sum().sum(axis=1)],
        axis=1
        )
    
    comparison.columns = ['designCapacity_bdb', 'designCapacity_fied', 'energyMJ_bdb', 'energyMJ_fied']

    return comparison

def plot_scatter_comparison(comparison, compare_type=None, write_fig=True):
    """
    Creates two subplots that compare county sums of design capacity (MW)
    and energy (MJ) for the boiler inventory and the foundational dataset.
    
    Parameters
    ----------
    comparison : pandas.DataFrame

    write_fig : Bool, default=True

    compare_type : str; {'county', 'naics'}

    Returns
    -------
    None

    """

    plot_data = comparison.copy(deep=True)
    plot_data.fillna(0, inplace=True)
    plot_data.reset_index(inplace=True)

    fig = make_subplots(rows=1, cols=2, 
                        subplot_titles=('Design Capacity (MW)', 'Energy (MJ)'))

    fig.add_trace(
        go.Scatter(
            x=plot_data.designCapacity_bdb,
            y=plot_data.designCapacity_fied,
            mode='markers'
            ),
        row=1, col=1
        )

    fig.add_trace(
        go.Scatter(
            x=plot_data.energyMJ_bdb,
            y=plot_data.energyMJ_fied,
            mode='markers'
            ),
        row=1, col=2
        )

    # Hardcoded coordinates for diagonal lines.
    line_xy = {
        'county': {
            'capacity_x': [0.001, 1E6],
            'capacity_y': [0.001, 1E6],
            'energy_x': [100, 1E12],
            'energy_y': [100, 1E12],
            },
        'naics': {
            'capacity_x': [10, 1E6],
            'capacity_y': [10, 1E6],
            'energy_x': [1E4, 1E12],
            'energy_y': [1E4, 1E12]
            }
        }

    fig.add_trace(
        go.Scatter(
            x=line_xy[compare_type]['capacity_x'],
            y=line_xy[compare_type]['capacity_y'],
            mode='lines', line=dict(color='black', width=2, dash='dash')
            ),
        row=1, col=1
        )
    
    fig.add_trace(
        go.Scatter(
            x=line_xy[compare_type]['energy_x'],
            y=line_xy[compare_type]['energy_y'],
            mode='lines', line=dict(color='black', width=2, dash='dash')
            ),
        row=1, col=2
        )
    
    for n in [1, 2]:
        fig.update_xaxes(
            title_text='NW Boiler Inventory', type='log', row=1, col=n,
            showexponent='all',
            exponentformat='power',
            showgrid=True
            )
        fig.update_yaxes(
            title_text='Foundational Data', type='log', row=1, col=n,
            showexponent='all',
            exponentformat='power',
            showgrid=True
            )

    fig.update_layout(
        height=800, width=1000,
        showlegend=False,
        template='presentation'
        )
    
    if write_fig is True:
            pio.write_image(
            fig,
            file=f'./analysis/figures/boiler_compare_scatter_{compare_type}.svg'
            )

    else:
        fig.show()

    return None

if __name__ == '__main__':

    boiler_url = 'https://raw.githubusercontent.com/carriescho/Electrification-of-Boilers/master/total_boiler_inventory.csv'
    fied_path = pathlib.Path('foundational_industry_data_2017.csv.gz')

    bdb = get_boiler_data(boiler_url)
    fied_boiler = get_fied_boiler(fied_path)

    # Note that FIED is for 2017; the boiler inventory uses multiple reporting years.
    # This is more of an issue for energy than design capacity, as it's expected that
    # energy use is more likely to change by year than design capacity.
    for t in ['county', 'naics']:

        comparison = compare_boilers(bdb, fied_boiler, compare_type=t)
        plot_scatter_comparison(comparison, compare_type=t, write_fig=True)

