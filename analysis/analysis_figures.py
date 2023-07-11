
import pandas as pd
import plotly.express as px



def summary_unit_table():
    """
    
    """
    final_data = pd.read_pickle('final_data.pkl')
    table_data = make_consistent_naics_column(final_data, n=2)

    sectors = pd.DataFrame(
        [['ag', 11], ['cons', 21], ['mining', 23], ['mfg', 31], 
         ['mfg', 32], ['mfg', 33]],
        columns=['sector', 'n2']
        )

    table_data = pd.merge(
        table_data, sectors, on='n2'
        )

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

    





def make_consistent_naics_column(final_data, n=4):
    """
    
    """

    df_naics = pd.DataFrame(final_data['naicsCode'].drop_duplicates())
    df_naics.loc[:, f'n{n}'] = df_naics.naicsCode.apply(lambda x: int(str(x)[0:n]))

    plot_data = final_data.copy()
    plot_data = pd.merge(
        plot_data, df_naics, on='naicsCode'
        )

    return plot_data

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

