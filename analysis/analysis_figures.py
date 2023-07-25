
import logging
import json
import pandas as pd
import numpy as np
import plotly.express as px
from urllib.request import urlopen

logging.basicConfig(level=logging.INFO)


def summary_unit_table(final_data):
    """
    Creates a table that summarizes by industrial sector
    (i.e., 2-digit NAICS) various aspects of the 
    dataset

    Parameters
    ----------
    final_data : pandas.DataFrame or parquet

    Returns
    -------
    summary_table : pandas.DataFrame

    """

    table_data = make_consistent_naics_column(final_data, n=2)

    sectors = pd.DataFrame(
        [['ag', 11], ['cons', 21], ['mining', 23], ['mfg', 31], 
         ['mfg', 32], ['mfg', 33]],
        columns=['sector', 'n2']
        )

    table_data = pd.merge(table_data, sectors, on='n2')

    _unit_count = table_data.groupby(
        ['sector', 'unitTypeStd']
        ).registryID.count()

    _unit_count.name = 'Count of Units'

    _capacity_sum = table_data.query('designCapacityUOM == "MW"').groupby(
            ['sector', 'unitTypeStd']
            ).designCapacity.sum()

    _fac_count = table_data.groupby(
        ['sector']
        ).registryID.unique().apply(lambda x: len(x))

    _fac_count.name = 'Count of Facilities'

    _energy_sum = table_data.groupby(
            ['sector', 'unitTypeStd']
            )['energyMJ', 'energyMJq0'].sum()

    _throughput_sum = table_data.groupby(
            ['sector', 'unitTypeStd']
            ).throughputTonneQ0.sum()

    _throughput_sum.name = 'Throughput in Metric Tons'

    summary_table = pd.concat(
        [_unit_count, _capacity_sum, _energy_sum, _throughput_sum],
        axis=1,
        )

    summary_table = pd.DataFrame(_fac_count).join(summary_table)

    return summary_table


def unit_bubble_map(final_data, unit_type, measure, max_size=45):
    """
    Plot locations of a single standard unit type by
    either energy (MJ) or design capacity.

    Parameters
    ----------
    final_data : pandas.DataFrame

    unit_type : str
        Standard unit type: 'other combustion', 'kiln', 'dryer', 
        'boiler', 'heater', 'turbine', 'oven', 'engine', 'furnace', 
        'thermal oxidizer', 'incinerator',
       'other', 'generator', 'flare', 'stove', 'compressor', 'pump',
       'building heat', 'distillation'

    measure : str
        Either 'energy' (results in MJ) or 'power' (results in MW)

    Returns
    -------
    fig :
 
    """

    plot_data = final_data.query("unitTypeStd == @unit_type")

    plot_args = {
        'lat': 'latitude',
        'lon': 'longitude',
        'scope': 'usa',
        'color': 'unitTypeStd',
        'title': f'Location of {unit_type.title()} Units Reporting {measure.title()} Data',
        'size_max': max_size
        }

    if measure == 'energy':
        plot_data = plot_data.query('energyMJ>0')
        plot_args['size'] = plot_data.energyMJ.to_list()
        plot_args['labels'] = {
            'energyMJ': 'Unit Energy Use (MJ)'
            }

    elif measure == 'power':
        plot_data = plot_data.query(
            "designCapacityUOM=='MW' & designCapacity>0"
            )
        plot_args['size'] = plot_data.designCapacity.to_list()
        plot_args['labels'] = {
            'designCapacity': 'Unit Capacity (MW)'
            }
        plot_args['title'] = f'Location of {unit_type.title()} Units Reporting Capacity Data'

    # sizeref = plot_args['size'].max()/max_size**2

    fig = px.scatter_geo(plot_data, **plot_args)
    fig.update_layout(showlegend=True)
    fig.show()




def stacked_bar_missing(final_data, naics_level=2):
    """"
    Creates stacked bar showing counts of facilities
    with and without unit-level data.

    Parameters
    ----------
    df : final 
    """

    plot_data = make_consistent_naics_column(final_data, naics_level)

    plot_data = pd.concat(
        [plot_data.query("unitTypeStd.isnull()", engine="python").groupby(
            f'n{naics_level}'
            ).apply(lambda x: np.size(x.registryID.unique())),
         plot_data.query("unitTypeStd.notnull()", engine="python").groupby(
            f'n{naics_level}'
            ).apply(lambda x: np.size(x.registryID.unique()))], 
        axis=1,
        ignore_index=False
        )

    plot_data.reset_index(inplace=True)

    plot_data.columns = ['NAICS Code', 'Without Unit Characterization', 'With Unit Characterization']

    plot_data.update(plot_data['NAICS Code'].astype(str))
    fig = px.bar(plot_data,
                 x='NAICS Code',
                 y=['Without Unit Characterization',
                    'With Unit Characterization'],
                 labels={
                    'value': 'Facility Count',
                    'variable': 'Facilities'
                    },
                 template='simple_white',
                 color_discrete_map={
                    'Without Unit Characterization': "#bcbddc",
                    'With Unit Characterization': "#756bb1"
                    })
    
    fig.show()


def plot_ut_by_naics(final_data, naics_level=None, variable='count'):
    """
    Creates a table that summarizes by industrial sector
    (i.e., 2-digit NAICS) various aspects of the 
    dataset

    Parameters
    ----------
    final_data : pandas.DataFrame or parquet

    naics_level :
        Specified NAICS level (None or 2 - 6)

    Returns
    -------
    summary_table : pandas.DataFrame
    """
    formatting = {
        'x': 'unitTypeStd',
        'template': 'simple_white',
        'labels': {
            'unitTypeStd': 'Standardized Unit Type'
            }
        }

    if not naics_level:

        grouper = 'unitTypeStd'

        plot_data = final_data.copy(deep=True)

    else:

        grouper = ['unitTypeStd', f'n{naics_level}']

        plot_data = make_consistent_naics_column(final_data, naics_level)
        plot_data.loc[:, f'n{naics_level}'] = plot_data[f'n{naics_level}'].astype(str)

        len_naics = len(plot_data[f'n{naics_level}'].unique())

        formatting['labels'][f'n{naics_level}'] = 'NAICS Code'
        formatting['color'] = f'n{naics_level}'
        # formatting['color_discrete_sequence'] = px.colors.sequential.Plasma_r
        formatting['color_discrete_sequence'] = \
            px.colors.sample_colorscale("plasma_r", [n/(len_naics-1) for n in range(len_naics)])

    if variable == 'count':
        plot_data = plot_data.query(
            "unitTypeStd.notnull()", engine="python"
            ).groupby(grouper).registryID.count()

        formatting['labels']['registryID'] = 'Unit Count'
        formatting['y'] = 'registryID'

    elif variable == 'energy':
        plot_data = plot_data.query(
            "unitTypeStd.notnull()", engine="python"
            ).groupby(grouper)['energyMJ', 'energyMJq0'].sum().sum(axis=1)

        plot_data.name = 'energy'
        formatting['y'] = 'energy' 
        formatting['labels']['energy'] = 'Energy (MJ)'

    elif variable == 'capacity':
        plot_data = plot_data.query(
            "unitTypeStd.notnull() & designCapacityUOM=='MW'", engine="python"
            ).groupby(grouper).designCapacity.sum()

        formatting['y'] = 'designCapacity'
        formatting['labels']['designCapacity'] = 'Unit Design Capacity (MW)'

    else:
        logging.error('No variable specified')
        raise ValueError

    if type(plot_data) == pd.core.series.Series:
        plot_data = pd.DataFrame(plot_data).reset_index()

    else:
        plot_data.reset_index(inplace=True)

    plot_data.loc[:, 'unitTypeStd'] = [x.capitalize() for x in plot_data.unitTypeStd]
    plot_data = plot_data.sort_values(by=grouper, ascending=True)

    fig = px.bar(plot_data, **formatting)

    fig.update_layout(
        yaxis=dict(
            showexponent='all',
            exponentformat='e'
            )
        )

    fig.show()

def make_consistent_naics_column(final_data, n):
    """
    Creates a column of consisently aggregated NAICS codes
    (i.e., same number of digits) when a column of 
    NAICS codes contains different levels of aggregation.
    Will only include 

    Parameters
    ----------
    final_data : pandas.DataFrame or parquet

    n : int; 2 to 6
        Specified NAICS level

    Returns
    -------
    analysis_data : pandas.DataFrame
        Returns original DataFrame with
        new column named f'n{n}'.
    """

    df_naics = pd.DataFrame(final_data['naicsCode'].drop_duplicates(),
                            dtype=int)

    df_naics_consistent = df_naics[
        df_naics.naicsCode.apply(lambda x: len(str(x)) >= n)
        ]

    if len(df_naics_consistent) < len(df_naics):
        logging.info(f"Caution: not all original NAICS codes are at this aggregation")

    else:
        pass

    df_naics_consistent.loc[:, f'n{n}'] = df_naics_consistent.naicsCode.apply(
        lambda x: int(str(x)[0:n])
        )

    analysis_data = final_data.copy()
    analysis_data = pd.merge(
        analysis_data, df_naics_consistent, on='naicsCode'
        )

    return analysis_data

def compare_energy_est_figure(ghgrp_est, nei_est, grouping): 
    """
    
    """

def unit_capacity_naics(df, naics='all'):
    """
    
    Parameters
    ----------

    df : pandas.DataFrame
        Final foundational dataset

    naics : str ['all', 'non-mfg', 'intensive', 'other-mfg'] or list of ints; default='all'
        Group output by NAICS code group (str) or by list of integer(s). Integers must
        be at 4-digit NAICS level.

    Returns
    -------

    naics_plot : 
    
    """
    df_naics = pd.DataFrame(df['naicsCode'].drop_duplicates())
    df_naics.loc[:, 'n4'] = df_naics.naicsCode.apply(lambda x: int(str(x)[0:4]))

    plot_data = df.copy()
    plot_data = pd.merge(
        plot_data, df_naics, on='naicsCode'
        )
    
    # NAICS subsectors of Paper Manufacturing, Petroleum and Coal Products, 
    # Chemical Manufacturing, Glass and Glass Product Manufacturing, Cement and
    # Concrete, Lime and Gypsum, Iron & Steel, Alumina and Aluminum, 
    intensive_naics = [3221, 3241, 3251, 3253, 3272, 3273, 3274, 3311, 3313]

    # Select relevant NAICS codes
    if naics == 'all':
        pass

    elif naics == 'non-mfg':

        plot_data = pd.DataFrame(
            plot_data[~plot_data.n4.between(3000, 4000)]
            )

    elif naics == 'intensive':

        plot_data = pd.DataFrame(
            plot_data[plot_data.n4.isin(intensive_naics)]
            )
    
    elif naics == 'other-mfg':

        plot_data = pd.DataFrame(
            plot_data[~plot_data.n4.isin(intensive_naics)]
            )

    elif type(naics) is list:

        plot_data = pd.DataFrame(
            plot_data[plot_data.n4.isin(naics)]
            )
        
    else:
        raise IndexError

