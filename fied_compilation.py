# TODO create executable?

import logging
import os
import re
import sys
import pdb
import pandas as pd
import numpy as np
from tools.naics_matcher import naics_matcher
from tools.misc_tools import FRS_API
# import ghgrp.run_GHGRP as GHGRP
from scc.scc_unit_id import SCC_ID
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char
from nei.nei_EF_calculations import NEI
from frs.frs_extraction import FRS
from qpc.census_qpc import QPC
from geocoder.geopandas_tools import FiedGIS
import geocoder.geo_tools


logging.basicConfig(level=logging.INFO)


def assign_data_quality(df, dqi):
    """
    Assigns a data quality indicator (DQI) to a dataframe of energy estimates,
    using a column entitled "energyDQI".
    See Parameters for current DQIs and their definitions.


    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to assign dqi using column entitled
        energyDQI

    dqi : int, {1, 2, 3, 4}
        Subjective data quality indicator. Larger value represents a higher data quality level.
        See notes below for definition. 

    Returns
    -------
    df : pandas.DataFrame
        Original DataFrame with dqi assigned to energyDQI
        column.

    Notes
    -----
    Definitions of DQIs:

        1 : energy data derived from an industry and/or regional average.

        2 : energy data derived from criteria air pollutant emissions reported
        directly by facility for an identified unit type (i.e., energy 
        estimates derived from EPA's National Emissions Inventory).

        3 : energy data derived from greenhouse gas emissions reported directly
        by facility for an identified unit type (i.e., energy estimates
        derived from EPA's Greenhouse Gas Reporting Program).

        4 : energy data reported directly by facility for an identified unit
        type.

    """

    df.loc[:, 'energyDQI'] = dqi

    return df


def split_multiple(x, col_names):
    """"
    Takes a pandas DataFrame row slice and for
    specified columns, splits out multiple values contained
    in the selection into a new dataframe.

    Parameters
    ----------
    x : pandas.DataFrame row slice

    col_names : list of strings

    Returns
    -------
    mult : pandas.DataFrame
    """

    if type(x[col_names[1]]) is str:

        try:
            data = [int(x[col_names[1]])]

        except ValueError:

            data = x[col_names[1]].split(', ')
            data = [int(k) for k in data]

    elif type(x[col_names[1]]) is float:

        # data = [x[col_names[1]]]
        data = [int(x[col_names[1]])]

    else:
        return

    # Remove any duplicates
    data = list(set(data))

    mult = pd.DataFrame(
        data=data, columns=[col_names[1]],
        dtype=int
        )

    for c in [col_names[0], 'registryID']:
        mult.loc[:, c] = int(x[c])

    return mult

def check_registry_id(ghgrp_unit_data, frs_data):
    """
    There's a least one error in the FRS IDs provided
    in the EPA unit data spreadsheet. Therefore,
    there's a need to verify the GHGRP facility ID and 
    FRS ID with the FRS data.

    There are also FRS IDs from the GHGRP spreadsheet that
    do not appear in the FRS Query tool (e.g., 110043972083).

    Parameters
    ---------
    ghgrp_unit_data : pandas.DataFrame
        Formatted GHGRP-derived energy estimates with unit-level information.

    frs_data : pandas.DataFrame
        Formatted Facility Registry Service (FRS) facility data.

    Returns
    -------
    ghgrp_unit_data : pandas.DataFrame
        Original DataFrame with corrected registryIDs.

    """

    ghgrp_ids = ghgrp_unit_data[['registryID', 'ghgrpID']].drop_duplicates()

    frs_ids = frs_data.dropna(subset=['ghgrpID'])[['registryID', 'ghgrpID']].drop_duplicates()
    frs_id_dict = frs_ids.set_index('ghgrpID').to_dict()
    
    # ghgrpIDs with null values for frs_registryID in this DataFrame have no corresponding
    # Registry ID from the underlying FRS data. 
    ghgrp_ids.loc[:, 'frs_registryID'] = ghgrp_ids.ghgrpID.map(frs_id_dict['registryID'])

    ghgrp_ids.loc[:, 'match'] = ghgrp_ids.registryID == ghgrp_ids.frs_registryID

    # These are all the Registry Ids from the GHGRP data that don't match the underlying FRS 
    # data
    wrong_id = ghgrp_ids.where(ghgrp_ids.match==False).dropna()

    ghgrp_ids.registryID.update(wrong_id.frs_registryID)

    ghgrp_ids.loc[:, 'rematch'] =  ghgrp_ids.registryID == ghgrp_ids.frs_registryID

    assert all(ghgrp_ids.loc[wrong_id.index, 'rematch']) is True

    ghgrp_unit_data.loc[:, 'corrected_registryID'] = ghgrp_unit_data.ghgrpID.map(
        ghgrp_ids.set_index('ghgrpID').registryID.to_dict()
        )

    ghgrp_unit_data.registryID.update(ghgrp_unit_data.corrected_registryID)

    ghgrp_unit_data.drop(['corrected_registryID'], axis=1, inplace=True)

    return ghgrp_unit_data


def melt_multiple_ids(frs_data, other_data, pkle=False):
    """
    Melt FRS data for facilities, extracting multiple NEI or GHGRP IDs.

    Parameters
    ----------
    frs_data : pandas.DataFrame
        Formatted FRS data.

    other_data : pandas.DataFrame
        Either formatted NEI or GHGRP unit data


    Returns
    -------
    melted : pandas.DataFrame
        Melted DataFrame with columns of registryID and either ghgrpID or
        eisFacilityID.

    """

    col_names = []

    if 'ghgrpID' in other_data.columns:

        col_names = ['ghgrpID', 'ghgrpIDAdditional']

    else:

        col_names = ['eisFacilityID', 'eisFacilityIDAdditional']

    frs_mult = frs_data[frs_data[col_names[1]].notnull()]

    # Check if 'registryID' in frs_mult
    if 'registryID' in frs_mult.columns:
        pass
    else:
        logging.error("registryID missing")
        frs_mult.to_csv('frs_mult.csv')

    frs_mult = pd.concat(
        [split_multiple(d, col_names) for i, d in frs_mult.iterrows()],
        axis=0, ignore_index=True
        )

    frs_mult = frs_mult.melt(
        id_vars=['registryID'], value_name=col_names[0]
        ).drop('variable', axis=1)

    melted = pd.concat([
        frs_mult, frs_data[frs_data[col_names[1]].isnull()][
            ['registryID', col_names[0]]
            ]
        ],
        axis=0, ignore_index=True
        )

    melted.dropna(inplace=True)

    melted = melted.drop_duplicates(subset=[col_names[0]])

    return melted


def harmonize_unit_type(df):
    """
    Applies unit type mapping to detailed unit types
    reported under GHGRP and NEI

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing unit types to be harmonized.

    Returns
    -------
    df : pandas.DataFrame
        Original DataFrame with additional column of
        harmonized unit types

    """

    unit_types = pd.DataFrame(df['unitType'].drop_duplicates())

    unit_types.loc[:, 'unitTypeStd'] = unit_types.unitType.apply(
        lambda x: unit_regex(x)
        )

    df = pd.merge(df, unit_types, on=['unitType'],
                  how='left')

    # Try using unitName or unitDescription for any missing unitTypeStd
    std_null = df.query("unitTypeStd.isnull()", engine='python')

    try:
        std_null.update(
            std_null.unitName.apply(lambda x: unit_regex(x))
            )

    except AttributeError:
        std_null.update(
            std_null.unitDescription.apply(lambda x: unit_regex(x))
            )

    df.unitTypeStd.update(std_null.unitTypeStd)

    return df


def blend_estimates(nei_data_shared, ghgrp_data_shared):
    """"
    Select approporiate unit data where both GHGRP
    and NEI report unit data.

    Parameters
    ----------
    nei_data_shared : pandas.DataFrame
        NEI data where facility and units are shared with GHGRP.

    ghgrp_data_shared : pandas.DataFrame
        GHGRP data where facility and units are shared with NEI.

    Returns
    -------
    shared_ocs_ : pandas.DataFrame
        Energy or emissions estimates for shared GHGRP units that were labeled
        as "Other combustion source (OCS)".

    shared_nonocs_ : pandas.DataFrame
        Energy of emissions estimates for shared GHGRP units that were not labeled
        as "Other combustion source (OCS)".

    """

    ghgrp_data_shared_ocs = id_ghgrp_units(ghgrp_data_shared, ocs=True)

    nei_data_shared_ocs = id_nei_units_ocs(nei_data_shared,
                                           ghgrp_data_shared_ocs)

    ghgrp_data_shared_nonocs = id_ghgrp_units(ghgrp_data_shared, ocs=False)

    nei_data_shared_nonocs = id_nei_units_nonocs(nei_data_shared,
                                                 nei_data_shared_ocs)

    ghgrp_data_shared_ocs.to_pickle('ghgrp_data_shared_ocs.pkl')
    nei_data_shared_ocs.to_pickle('nei_data_shared_ocs.pkl')
    ghgrp_data_shared_nonocs.to_pickle('ghgrp_data_shared_nonocs.pkl')
    nei_data_shared_nonocs.to_pickle('nei_data_shared_nonocs.pkl')

    shared = {}

    for dt in ['energy', 'ghgs']:

        logging.info(f'Allocating shared OCS {dt}...')
        shared[f'shared_ocs_{dt}'] = allocate_shared_ocs(
            ghgrp_data_shared_ocs, nei_data_shared_ocs, dt=dt)

        logging.info(f"Reonciling shared non-ocs {dt}...")

        # Instead of the reconcile_shared_ocs method, just use GHGRP data.
        shared[f'shared_nonocs_{dt}'] = ghgrp_data_shared_nonocs 
        
    shared_ocs_ = shared['shared_ocs_energy'].copy(deep=True)
        
    shared_ocs_.update(
        shared['shared_ocs_ghgs'][
            ['ghgsTonneCO2e', 'ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']
            ]
        )
    
    shared_nonocs_ = shared['shared_nonocs_energy'].copy(deep=True)

    # shared_nonocs_.update(
    #     shared['shared_nonocs_ghgs'][
    #         ['ghgsTonneCO2e', 'ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']
    #         ]
    #     )

    return shared_ocs_, shared_nonocs_


def id_nei_units_nonocs(nei_data_shared, nei_data_shared_ocs):
    """
    Identify NEI data for facilites that report to both NEI and GHGRP
    and that report a unit type other than "OCS (Other combustion source)" 
    under GHGRP reporting.

    Parameters
    ----------
    nei_data_shared : pandas.DataFrame
        All NEI data for facilities that report to both NEI and GHGRP.

    nei_data_shared_ocs : pandas.DataFrame
        NEI data for facilities that report to both NEI and GHGRP
        and have an OCS (Other combustion source) unit for a given
        ('registryID', 'fuelTypeStd') in the corresponding GHGRP data.

    Returns
    -------
    nei_data_shared_nonocs : pandas.DataFrame
        The selection of data from nei_data_shared that does not correspond
        to nei_data_shared_ocs. If this is combined with nei_data_shared_ocs,
        it will result in the full nei_data_shared DataFrame.

    """

    # These entries do not have energy estimates
    nei_data_shared_na_fuelType = pd.DataFrame(
        nei_data_shared.query('fuelTypeStd.isnull()',
                              engine='python')
        )

    nei_data_shared.set_index(['registryID', 'fuelTypeStd'], inplace=True)
    nei_data_shared_ocs.set_index(['registryID', 'fuelTypeStd'], inplace=True)

    nei_data_shared_nonocs = pd.DataFrame(
        nei_data_shared.loc[
            nei_data_shared.index.dropna().difference(
                nei_data_shared_ocs.index
                )
            ]
        )

    # Reset all of these indicies
    nei_data_shared_nonocs.reset_index(inplace=True)
    nei_data_shared_ocs.reset_index(inplace=True)
    nei_data_shared.reset_index(inplace=True)

    nei_data_shared_nonocs = \
        nei_data_shared_nonocs.append(nei_data_shared_na_fuelType)

    return nei_data_shared_nonocs


def assign_estimate_source(df, source, dt):
    """
    Create a column entitled 'estimateSource' and assign a value
    based on the source of energy estimates.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to assign estimate source. 

    source : str, {'ghgrp', 'nei'}
        Source of energy estimate.

    dt : str, {'energy', 'ghgs'}
        Type of estimated data.

    Returns
    -------
    df : pandas.DataFrame
        Original dataframe with new column and values indicating
        source of energy estimates. 
    """

    df.loc[:, f'{dt}EstimateSource'] = source

    return df


def reconcile_shared_nonocs(nei_data_shared_nonocs, ghgrp_data_shared_nonocs, dt='energy'):
    """
    Reconcile cases where facilities report to both NEI and GHGRP for
    units that are not identified as OCS (other combustion source)
    for a given (registryID, fuelTypeStd).

    Parameters
    ----------
    nei_data_shared_nonocs : pandas.DataFRame
        NEI data from facilities that also report to GHGRP, but 
        do not report units as OCS (Other combustion source)

    ghgrp_data_shared_nonocs : pandas.DataFrame
        GHGRP data from facilities that also report to NEI, but do
        not report units as OCS (Other combustion source) for a given
        (registryID, fuelTypeStd).

    dt : str, default='energy'; {'energy', 'ghgs'}
        Indicate whether shares are calculated for energy or 
        greenhouse gas emissions (ghgs).

    Returns
    -------
    final_shared_nonocs : pands.DataFrame
        A compilation of unit-level estimates.

    """
    if dt == 'energy':
        nei_agg_dict = {
            'designCapacity': 'sum', 'eisUnitID': 'count',
            'energyMJq0': 'sum', 'energyMJq2': 'sum',
            'energyMJq3': 'sum'
            }

        ghgrp_agg_dict = {
            'designCapacity': 'sum', 'unitName': 'count',
            'energyMJ': 'sum'
            }

        query_dict = {
            'ghgrp_no_neiE': "energyMJq2==0 & unitName.notnull()",
            'nei_no_ghgrpE': "energyMJ==0 & energyMJq2>0",
            'no_e': "energyMJ==0 & energyMJq2==0",
            'ghgrp_and_nei': "energyMJ>0 & energyMJq2>0"
            }

    elif dt == 'ghgs':
        nei_agg_dict = {
            'designCapacity': 'sum', 'eisUnitID': 'count',
            'ghgsTonneCO2eQ0': 'sum', 'ghgsTonneCO2eQ2': 'sum',
            'ghgsTonneCO2eQ3': 'sum'
            }

        ghgrp_agg_dict = {
            'designCapacity': 'sum', 'unitName': 'count',
            'ghgsTonneCO2e': 'sum'
            }
        
        query_dict = {
            'ghgrp_no_neiE': "ghgsTonneCO2e==0 & unitName.notnull()",
            'nei_no_ghgrpE': "ghgsTonneCO2e==0 & ghgsTonneCO2eQ2>0",
            'no_e': "ghgsTonneCO2e==0 & ghgsTonneCO2eQ2==0",
            'ghgrp_and_nei': "ghgsTonneCO2e>0 & ghgsTonneCO2eQ2>0"
            }

    nei_dsno = nei_data_shared_nonocs.copy(deep=True)
    ghgrp_dsno = ghgrp_data_shared_nonocs.copy(deep=True)

    compare = pd.merge(
        nei_dsno.groupby(
            ['registryID', 'unitTypeStd', 'fuelTypeStd']
            ).agg(nei_agg_dict),
        ghgrp_dsno.groupby(
            ['registryID', 'unitTypeStd', 'fuelTypeStd']
            ).agg(ghgrp_agg_dict),
        left_index=True,
        right_index=True,
        suffixes=('_nei', '_ghgrp'),
        how='outer'
        )

    compare.to_pickle(f'{dt}_compare.pkl')

    use_nei_data = {}  # Dictionary for index selections that will use NEI data
    use_ghgrp_data = {}  # Dictionary for index selections that will use GHGRP data

    # Use GHGRP data for facilities where there is no unit information for
    # a given (registryID, unitTypeStd, fuelTypeStd)
    use_ghgrp_data['ghgrp_'] = compare.query("eisUnitID.isnull()", 
                                             engine='python')
    use_ghgrp_data['ghgrp_'].dropna(axis=1, inplace=True)
    use_ghgrp_data['ghgrp_'] = use_ghgrp_data['ghgrp_'].index

    # Use NEI data for facilities where there is no unit information for
    # a given (registryID, unitTypeStd, fuelTypeStd)
    use_nei_data['nei_'] = compare.query("unitName.isnull()", engine='python')
    use_nei_data['nei_'].dropna(axis=1, inplace=True)
    use_nei_data['nei_'] = use_nei_data['nei_'].index

    # Use GHGRP data for facilities where there are NEI units, but
    # no energy estimates. There are cases where NEI data lack energy
    # estimates, but do have design capacities and GHGRP data have energy 
    # estimates, but lack design capacities
    # #TODO a closer matching of units between NEI and GHGRP data might be 
    # possible using NEI design capacities where there are none in GHGRP data
    use_ghgrp_data['ghgrp_no_neiE'] = compare.query(
        query_dict['ghgrp_no_neiE'],
        engine='python'
        ).index

    # Use NEI data for facilities where there are GHGRP units, but
    # no energy estimates.
    use_nei_data['nei_no_ghgrpE'] = \
        compare.query(query_dict['nei_no_ghgrpE']).index

    # Use NEI data for facilities where there are GHGRP units, but
    # no energy estimates from either data set.
    use_nei_data['no_e'] = compare.query(query_dict['no_e']).index

    # Other challenge is how to select when it was possible to derive
    # energy estimates from NEI and GHGRP. Assume that GHGRP derivations 
    # are more robust. This is certainly true for GHG emissions
    use_ghgrp_data['ghgrp_and_nei'] = compare.query(query_dict['ghgrp_and_nei']).index

    nei_dsno.set_index(['registryID', 'unitTypeStd', 'fuelTypeStd'],
                                     inplace=True)

    ghgrp_dsno.set_index(['registryID', 'unitTypeStd', 'fuelTypeStd'],
                                       inplace=True)

    final_shared_nonocs_nei = pd.DataFrame()
    final_shared_nonocs_ghgrp = pd.DataFrame()

    for i in use_nei_data.keys():
        final_shared_nonocs_nei = final_shared_nonocs_nei.append(
            pd.DataFrame(nei_dsno.loc[use_nei_data[i]])
            )

    for i in use_ghgrp_data.keys():
        final_shared_nonocs_ghgrp = final_shared_nonocs_ghgrp.append(
            pd.DataFrame(ghgrp_dsno.loc[use_ghgrp_data[i]])
            )

    final_shared_nonocs_ghgrp = \
        assign_estimate_source(final_shared_nonocs_ghgrp, 'ghgrp', dt=dt)
    final_shared_nonocs_nei = \
        assign_estimate_source(final_shared_nonocs_nei, 'nei', dt=dt)

    final_shared_nonocs = pd.concat(
        [final_shared_nonocs_ghgrp.reset_index(),
         final_shared_nonocs_nei.reset_index()],
        axis=0, ignore_index=True, sort=True
        )

    return final_shared_nonocs


def id_nei_units_ocs(nei_data_shared, ghgrp_data_shared_ocs):
    """
    Identify NEI data for facilites that report to both NEI and GHGRP
    and that report a unit type as "OCS (Other combustion source)"
    under GHGRP reporting.

    Parameters
    ----------
    nei_data_shared : pandas.DataFrame
        DataFrame of NEI facilities that also report GHGRP data

    ghgrp_data_shared_ocs : pandas.DataFrame
        GHGRP data with facilities that report OCS units

    Returns
    -------
    nei_data_shared_ocs : pandas.DataFrame
        NEI data for facilities that have OCS units 
        under their GHGRP reporting.    
    """

    nei_data_shared_ocs = pd.DataFrame(nei_data_shared)

    nei_data_shared_ocs = pd.merge(
        nei_data_shared_ocs,
        ghgrp_data_shared_ocs.drop_duplicates(
            ['registryID', 'fuelTypeStd']
            )[['registryID', 'fuelTypeStd']],
        on=['registryID', 'fuelTypeStd'],
        how='inner'
        )

    nei_data_shared_ocs.reset_index(inplace=True)

    return nei_data_shared_ocs


def id_ghgrp_units(ghgrp_data, ocs=True):
    """
    Identify ghgrp data for facilities that report to both GHGRP and NEI and 
    that do or don't report ALL unit types as "OCS (Other combustion source).

    At this point, there is no attempt to address OCS units for facilities that
    also report data on non-OCS units.

    Identifies registryIDs that have ONLY OCS units with the
    ultimate purpose of allocating a facility's total energy use by fuel
    type across all of its units that use that fuel type. 

    Parameters
    ----------
    ghgrp_data : pandas.DataFrame
        DataFrame of GHGRP unit-level data of facilities
        that also report to the NEI.

    ocs : bool; default is True
        Specify whether identification is for OCS units

    Returns
    -------
    ghgrp_data_ : pandas.DataFrame
        Selection of facilities that do or don't report a
        OCS for a unit for a given fuel type.

    """

    ghgrp_data_ = ghgrp_data[['registryID', 'unitType']].drop_duplicates()
 
    ghgrp_data_ocs = pd.DataFrame(ghgrp_data_.groupby(['registryID']).unitType.count())
    ghgrp_data_ocs.columns = ['ut_count']

    # This next step should capture the facilities that only have OCS units
    ghgrp_data_ocs = ghgrp_data_ocs[ghgrp_data_ocs == 1].join(
        ghgrp_data_[ghgrp_data_.unitType == 'OCS (Other combustion source)'].set_index('registryID'),
        how='inner'
        ).dropna()
    
    if ocs is True:

        ghgrp_data_ocs = ghgrp_data.set_index('registryID').join(
            ghgrp_data_ocs[['ut_count']], how='inner'
            )
        
        ghgrp_data_ocs.reset_index(inplace=True)

        ghgrp_data_ocs.drop('ut_count', axis=1, inplace=True)
    
    else:

        ghgrp_data_ocs = ghgrp_data.set_index('registryID').join(
            ghgrp_data_ocs[['ut_count']], how='outer'
            )
        
        ghgrp_data_ocs.reset_index(inplace=True)

        ghgrp_data_ocs = ghgrp_data_ocs[ghgrp_data_ocs.ut_count.isnull()]

        ghgrp_data_ocs.drop('ut_count', axis=1, inplace=True)
        
    return ghgrp_data_ocs



# def calc_share_ocs(ghgrp_data_shared_ocs, cutoff=0.5, dt='energy'):
#     """
#     Calculate other combustion source (OCS) energy or emissions portion
#     for GHGRP reporters by registryID, fuelTypeStd, and unitTypeStd.
#     Returns unit based on cutoff value (
#     e.g., records where OCS share >=0.5 are returned with
#     cutoff=0.5)

#     Parameters
#     ----------
#     ghgrp_data_shared_ocs : pandas.DataFrame
#         DataFrame with 

#     cutoff : float, default=0.5
#         OCS share of energy by registryID, fuelTypeStd, and
#         unitTypeStd to identify unit

#     dt : str, default='energy'; {'energy', 'ghgs'}
#         Indicate whether shares are calculated for energy or 
#         greenhouse gas emissions (ghgs).

#     Returns
#     -------
#     ocs_share : pandas.DataFrame
#         Share of energy or ghgs by resgistry, fuelTypeStd, and unitTypeStd
#         that is OCS
#     """
#     if dt == 'energy':
#         col = 'energyMJ'

#     elif dt == 'ghgs':
#         col = 'ghgsTonneCO2e'

#     ocs_share = ghgrp_data_shared_ocs.groupby(
#         ['registryID', 'fuelTypeStd', 'unitTypeStd']
#         )[col].sum()

#     ocs_share = ocs_share.divide(
#         ocs_share.sum(level=[0, 1])
#         )

#     ocs_share = pd.DataFrame(ocs_share).reset_index()
    
#     ocs_share = ocs_share.query(f"unitTypeStd=='other combustion' & {col}>=@cutoff")

#     return ocs_share

def group_nei_by_unit(nei_data):
    """"
    Emissions from the NEI are attributed to processes, not individual units. So,
    there may be more than one process associated with an individual unit 
    (see the NEI data model: https://www.epa.gov/enviro/nei-model).
    This method aggregates the unit-process estimates into estimates by unit.

    Parameters
    ----------
    nei_data : pandas.DataFrame
        Formatted NEI data with energy and GHG emissions estimates

    Returns
    -------
    nei_data_unit : pandas.DataFrame
        Formatted NEI data with energy and GHG emissions estimates aggregated to unit and fuel.
    """

    nei_data_unit = nei_data.groupby(
        ['eisFacilityID', 'eisUnitID', 'unitType', 'unitDescription',
         'designCapacity', 'designCapacityUOM', 'fuelType', 'fuelTypeStd']
        )

    nei_data_unit = pd.concat(
        [nei_data_unit[c].sum() for c in ['energyMJq0', 'energyMJq2', 'energyMJq3',
              'throughputTonneQ0', 'throughputTonneQ2', 'throughputTonneQ3',
              'ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']], axis=1
        )
    
    nei_data_unit.reset_index(inplace=True)

    return nei_data_unit

def allocate_shared_ocs(ghgrp_data_shared_ocs, nei_data_shared_ocs, dt='energy'):
    """
    Selects energy or ghg emissions estimates from NEI or GHGRP when energy or emissions
    estimates are available from both sources for a facility. Also
    allocates energy estimated from GHGRP data for
    OCS (Other combustion source) based on NEI data.

    Parameters
    ----------
    ghgrp_data_shared_ocs : pandas.DataFrame
        Selected GHGRP unit data for facilities that also report
        to the NEI and report OCS (Other Combustion Source)

    nei_data_shared_ocs : pandas.DataFrame
        Selected NEI data for facilities that also report
        to the GHGRP.

    dt : str, default='energy'; {'energy', 'ghgs'}
        Indicate whether shares are calculated for energy or 
        greenhouse gas emissions (ghgs).

    Returns
    -------
    ocs_allocated : pd.DataFrame
        Energy estimates for units reported as OCS (other combustion source).
        Note that estimates may be provided from both NEI and GHGRP.
    """

    if dt == 'energy':
        ghgrp_col = 'energyMJ'
        nei_col = 'energyMJq2'
        portion_col = 'energyMJPortion'
        # nei_sum_cols = ['energyMJq0', 'energyMJq2','energyMJq3']

    elif dt == 'ghgs':
        ghgrp_col = 'ghgsTonneCO2e'
        nei_col = 'ghgsTonneCO2eQ0'
        portion_col = 'ghgsPortion'
        # nei_sum_cols = ['ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2','ghgsTonneCO2eQ3']

    nei_dso = nei_data_shared_ocs.copy(deep=True)
    ghgrp_dso = ghgrp_data_shared_ocs.copy(deep=True)

    #TODO delete after debugging
    if dt == 'ghgs':
        nei_dso.to_csv('nei_dso.csv')
        ghgrp_dso.to_csv('ghgrp_dso.csv')

    # Cleaning up the unecessary 'index' column 
    # #TODO (minor): find what merge or join is creating this column
    for df in [nei_dso, ghgrp_dso]:
        if 'index' in df.columns:
            df.drop('index', axis=1, inplace=True)

        else:
            continue

    nei_dso.set_index(
        ['registryID', 'fuelTypeStd', 'eisUnitID'], inplace=True
        )

    # nei_dso.set_index(
    #     ['registryID', 'eisFacilityID', 'fuelTypeStd', 'eisProcessID',
    #      'eisUnitID'],
    #     inplace=True
    #     )

    nei_dso.sort_index(inplace=True)

    # Use min of nei energy estimates
    # This can be changed in the future
    # nei_data_shared_portion = nei_dso[nei_col].sum(
    #     level=[0, 1, 2]
    #     )
    
    nei_dso.loc[:, portion_col] = nei_dso[nei_col].divide(
        nei_dso[nei_col].sum(level=[0, 1])
        )
    
    # nei_dso.set_index('eisUnitID', append=True, inplace=True)

    # nei_data_shared_portion = nei_data_shared_portion.where(
    #     nei_data_shared_portion > 0
    #     ).dropna(how='all')

    # nei_data_shared_portion = nei_dso[nei_col].divide(
    #         nei_data_shared_portion, fill_value=0
    #         )

    # # Order of index levels is getting mixed up after the above division
    # if nei_data_shared_portion.index.names == ['registryID', 'eisFacilityID', 'fuelTypeStd', 'eisUnitID']:
    #     nei_data_shared_portion = nei_data_shared_portion.swaplevel('eisUnitID', 'eisProcessID')
# 
    # else:
    #     pass

    # nei_data_shared_portion.dropna(inplace=True)

    # nei_data_shared_portion.name = portion_col

    # nei_dso.loc[:, portion_col] = nei_data_shared_portion

    nei_dso.reset_index(['eisUnitID'], drop=False, inplace=True)

    nei_dso.sort_index(inplace=True)

    ghgrp_dso.set_index(['registryID', 'fuelTypeStd'], inplace=True)

    ghgrp_dso.sort_index(inplace=True)

    ocs_allocated = pd.merge(
        nei_dso, ghgrp_dso[[ghgrp_col]].sum(level=[0,1]), 
        left_index=True,
        right_index=True,
        how='left'
        )
    
    ocs_allocated.loc[:, ghgrp_col] = ocs_allocated[portion_col].multiply(ocs_allocated[ghgrp_col])

    check = pd.concat(
        [ocs_allocated[ghgrp_col].sum(level=[0,1]), 
         ghgrp_dso[ghgrp_col].sum(level=[0,1])], axis=1
        )
    
    check.columns = ['allocated', 'original']

    check.loc[:, 'comparison'] = check.allocated.divide(check.original)

    missing = check[(check.allocated.isnull())|(check.comparison==0)]

    missing = ghgrp_dso.join(
        missing[['allocated']], how='inner'
        )

    ocs_allocated = pd.concat(
        [ocs_allocated, missing], axis=0, ignore_index=False
        )

    # ocs_allocated = ocs_allocated.join(
    #     check[check.allocated.notnull()][['allocated']], how='inner'
    #     )

    # Need to fill in energy values for instances where there is a GHGRP energy
    # estimate, but no NEI energy esimtate (energyMJPortion == na).
    # Use

    ocs_allocated.reset_index(inplace=True)
    ocs_allocated.drop(['allocated'], axis=1, inplace=True)
    # for df in [missing, ocs_allocated]:

    #     df.reset_index(inplace=True)

    #     df.drop(['allocated'], axis=1, inplace=True)

    ocs_allocated = assign_estimate_source(ocs_allocated, 'ghgrp', dt=dt)
    # missing = assign_estimate_source(missing, 'ghgrp', dt=dt)

    ocs_allocated.drop([portion_col], axis=1, inplace=True)

    # ocs_allocated = ocs_allocated.append(missing)

    return ocs_allocated


def unit_regex(unitType):
    """
    Use regex to standardize unit types,
    where appropriate. See unit_types variable
    for included types.

    Parameters
    ----------
    unitType : str
        Detailed unit type

    Returns
    -------
    unitTypeStd : str;
        Standardized unit type
    """

    other_boilers = ['PCWD', 'PCWW', 'PCO', 'PCT', 'OFB']

    # Combustion unit types
    unit_types = [
        'kiln', 'dryer', 'oven', 'furnace',
        'boiler', 'incinerator', 'flare',
        'heater', 'calciner', 'turbine',
        'stove', 'distillation', 'other combustion',
        'engine', 'generator', 'oxidizer', 'pump',
        'compressor', 'building heat', 'cupola',
        'PCWD', 'PCWW', 'PCO', 'PCT', 'OFB', 'broil',
        'reciprocating'
        ]

    ut_std = []

    for unit in unit_types:

        unit_pattern = re.compile(r'({})'.format(unit), flags=re.IGNORECASE)

        try:
            unit_search = unit_pattern.search(unitType)

        except TypeError:
            continue

        if unit_search:
            ut_std.append(unit)

        else:
            continue

    if any([x in ut_std for x in ['engine', 'reciprocating']]):
        ut_std = 'engine'

    elif (len(ut_std) > 1):
        ut_std = 'other combustion'

    elif (len(ut_std) == 0):
        ut_std = 'other'

    elif ut_std[0] == 'calciner':
        ut_std = 'kiln'

    elif ut_std[0] == 'oxidizer':
        ut_std = 'thermal oxidizer'

    elif ut_std[0] == 'buidling heat':
        ut_std = 'heater'

    elif ut_std[0] in ['cupola', 'broil']:
        ut_std = 'other combustion'

    elif any([x in ut_std[0] for x in other_boilers]):
        ut_std = 'boiler'

    elif ut_std[0] == 'reciprocating':
        ut_std = 'engine'

    else:
        ut_std = ut_std[0]

    return ut_std


def separate_unit_data(frs_data, nei_data, ghgrp_unit_data):
    """
    All facilities have FRS ID. Not all GHGRP facilities have EIS IDs and
    vice versa.

    Parameters
    ----------
    frs_data : pandas.DataFrame
        Formatted Facility Registry Service (FRS) data returned from 
        FRS.import_format_frs method.

    nei_data : pandas.DataFrame
        Formatted National Emissions Inventory (NEI) data returned
        from NEI.main() method.

    ghgrp_unit_data : pandas.DataFrame
        Formatted unit data from the Greenhouse Gas Reporting Program (GHGRP)
        and associated unit-level energy estimates. 
        Based on GHGRP_unit_char methods.

    Returns
    -------
    data_dict : dictionary of pandas.DataFrames
        Collection of DataFrames with keys related to whether facilities 
        report under the NEI and/or GHGRP, or neither. 
    """

    # Harmonize/standardize unit types
    nei_data = harmonize_unit_type(nei_data)
    ghgrp_unit_data = harmonize_unit_type(ghgrp_unit_data)

    # nei_data.to_pickle('nei_data.pkl')
    # ghgrp_unit_data.to_pickle('ghgrp_data_postharm.pkl')

    # Note these three DataFrames cover all registryIDs that have unit data
    # in either the NEI or GHGRP. There are other registryIDs in the 
    # frs_data DataFrame that do not have unit data.
    nei_no_ghgrp = frs_data.query(
        'eisFacilityID.notnull() & ghgrpID.isnull()', engine='python'
        )

    # Facilities that report to GHGRP, but not to NEI
    ghgrp_no_eis = frs_data.query(
        'eisFacilityID.isnull() & ghgrpID.notnull()', engine='python'
        )

    # Facilities that report to NEI AND GHGRP
    nei_and_ghgrp = frs_data.query(
        'eisFacilityID.notnull() & ghgrpID.notnull()', engine='python'
        )

    # Facilities that report to neither NEI nor GHGRP
    no_nei_or_ghgrp = frs_data.query(
        'eisFacilityID.isnull() & ghgrpID.isnull()', engine='python'
        )

    # EIS facility IDs that don't report to GHGRP
    nei_ids_noghgrp = melt_multiple_ids(nei_no_ghgrp, nei_data, pkle=True)

    nei_only_data = pd.merge(
        nei_ids_noghgrp,
        nei_data, on='eisFacilityID',
        how='inner'
        )

    for dt in ['energy', 'ghgs']:

        nei_only_data = assign_estimate_source(nei_only_data, 'nei', dt=dt)

    # GHGRP facility IDs that don't report to NEI
    ghgrp_ids_noeis = melt_multiple_ids(ghgrp_no_eis, ghgrp_unit_data)

    ghgrp_only_data = pd.merge(
        ghgrp_ids_noeis['registryID'],
        ghgrp_unit_data, on='registryID',
        how='inner'
        )
    
    for dt in ['energy', 'ghgs']:
        ghgrp_only_data = assign_estimate_source(ghgrp_only_data, 'ghgrp', dt=dt)

    ghgrp_only_data.to_pickle('ghgrp_only_data.pkl')
    # NEI and GHGRP facilities
    nei_ids_shared = melt_multiple_ids(nei_and_ghgrp, nei_data)

    nei_data_shared = pd.merge(
        nei_ids_shared,
        nei_data, on='eisFacilityID',
        how='inner'
        )

    ghgrp_ids_shared = melt_multiple_ids(nei_and_ghgrp, ghgrp_unit_data)

    ghgrp_data_shared = pd.merge(
        ghgrp_ids_shared['ghgrpID'],
        ghgrp_unit_data,
        on='ghgrpID',
        how='inner'
        )

    # Not all registryIDs with ghgrpIDs and neiIDs have ghgrp unit data
    logging.info(
        f"{len(ghgrp_data_shared.registryID.unique())} of the {len(ghgrp_ids_shared.registryID.unique())} registryIDs with ghgrpIDs and eisFacilityIDs have unit data associated with them."
        )

    data_dict = {
        'nei_shared': nei_data_shared,  # EIS data for facilities without GHGRP reporting
        'nei_only': nei_only_data,  
        'ghgrp_shared': ghgrp_data_shared,
        'ghgrp_only': ghgrp_only_data, 
        'no_nei_or_ghgrp': no_nei_or_ghgrp # All relevant facilities from this group are included in final data.
        }
    for k in data_dict.keys():
        data_dict[k].to_pickle(f'data_dict_{k}.pkl')
    return data_dict


def merge_qpc_data(final_data, qpc_data):
    """
    Original QPC data were expanded to 6-digit NAICS,
    but FRS data contain <6-digit NAICS codes. Need to
    match these FRS-provided NAICS to their 6-digit 
    equivalents before merging with QPC data.

    Paramters
    ---------
    final_data : pandas.DataFrame
        Final foundational data after merging final_energy_data 
        and frs_data.

    qpc_data : pandas.DataFrame
        Census Quarterly Survey of Plant Capacity Utilization data.

    Returns
    -------
    final_data : pandas.DataFrame
        Final data set with QPC weekly operating hours.

    """
    naics = naics_matcher(final_data.naicsCode)

    other_qpc = pd.merge(
        naics, qpc_data,
        left_on='n6', right_on='naicsCode', how='inner',
        suffixes=('', '_y')
        )

    other_qpc.drop(['n6', 'naicsCode_y'], axis=1, inplace=True)

    other_qpc = other_qpc.drop_duplicates(subset='naicsCode')

    # Merge in QPC data matched to <6-digit NAICS
    qpc_data = pd.concat([qpc_data, other_qpc], axis=0, sort=False,
                         ignore_index=True)

    final_data = pd.merge(
        final_data,
        qpc_data,
        on='naicsCode',
        how='left'
        )

    return final_data


def assemble_final_df(final_energy_data, frs_data, qpc_data, year):
    """
    Pull together FRS data, energy estimates, and weekly operating
    hour estimates into a single dataframe.

    Parameters
    ----------
    final_energy_data : pandas.DataFrame
        Estimated unit-level energy use and characterization

    frs_data : pandas.DataFrame
        Facility geographic information

    qpc_data : pandas.DataFrame 
        Weekly operating hours by quarter, including CI range

    Returns
    -------
    final_data : pandas.DataFrame

    """

    # FRS DataFrame has 1:many registryID: eisFacilityIDs, whereas final_energy_data
    # has 1:1 registryID: eisFacilityID.
    final_energy_data.drop('eisFacilityID', axis=1, inplace=True)

    # There are some discrepancies between registryIDs reported by
    # FRS and by GHGRP.
    frs_melted_ghgrp = melt_multiple_ids(frs_data,
                                         final_energy_data[['registryID', 'ghgrpID']])

    frs_melted_ghgrp = pd.merge(frs_melted_ghgrp, frs_data, on='registryID',
                                how='inner')

    frs_melted_ghgrp.set_index('ghgrpID_x', inplace=True)

    final_data = pd.merge(
        final_energy_data,
        frs_data,
        on='registryID',
        how='outer'
        )

    # Fix missing info
    energy_missing_ghgrp = pd.DataFrame(
        final_data[final_data['ghgrpID_x'].notnull() & 
                   final_data['ghgrpID_y'].isnull()]
        )

    energy_missing_ghgrp.loc[:, 'og_index'] = energy_missing_ghgrp.index.values
    # energy_missing_ghgrp.reset_index(inplace=True, drop=False)
    energy_missing_ghgrp.set_index('ghgrpID_x', inplace=True)
    energy_missing_ghgrp.update(frs_melted_ghgrp)
    energy_missing_ghgrp.reset_index(inplace=True, drop=False)
    energy_missing_ghgrp.set_index('og_index', inplace=True)
    energy_missing_ghgrp.index.name = None
    energy_missing_ghgrp = assign_estimate_source(energy_missing_ghgrp, 'ghgrp', 'energy')

    # energy_missing_ghgrp.to_pickle('energy_missing_ghgrp.pkl')
    final_data.update(energy_missing_ghgrp)

    final_data.drop(['ghgrpID_x'], inplace=True, axis=1)
    final_data.rename(columns={'ghgrpID_y': 'ghgrpID'}, inplace=True)

    final_data = merge_qpc_data(final_data, qpc_data)

    final_data = geocoder.geo_tools.fix_county_fips(final_data)

    # This doesn't result in any additional units.
    # missing_units = frs_api.find_unit_data_parallelized(final_data)

    return final_data


def save_final_data(final_data, year, fpath=None, fformat='csv', comp='gzip'):
    """
    Save final_data DataFrame.

    Parameters
    ----------
    final_data : pandas.DataFrame
        Assembled final_data DataFrame.

    year : int
        Data vintage year.

    fpath : str, default=None
        Path to save final_data. Defaults to location
        of `fied_compilation.py`.

    fformat : str, {'csv', 'parquet'}
        Format to save final_data. Defaults to 'csv'.

    comp : str, {'gzip', None}
        Compress file with gzip, or, if None, with no compression.

    """

    if comp:
        fname = f'foundational_industry_data_{year}.{fformat}.gz'

    else:
        fname = f'foundational_industry_data_{year}.{fformat}'

    if not fpath:
        save_path = fname

    else:
        save_path = os.path.join(fpath, fname)

    if fformat == 'csv':

        final_data.to_csv(save_path, compression=comp)

    elif fformat == 'parquet':

        try:
            final_data.to_parquet(
                f'foundational_industry_data_{year}.parquet.gz',
                engine='pyarrow',
                compression='gzip'
                )

        except ValueError as e:
            logging.ERROR(f"{e}\n final_data.columns are {final_data.columns}")

    return


if __name__ == '__main__':
    year = 2017

    SCC_ID().main()
    fiedgis = FiedGIS()

    try:
        frs_data = pd.read_csv(
            './data/FRS/frs_data_formatted.csv', low_memory=False
            )

    except FileNotFoundError:

        sys.exit("Run frs_extraction.py or check location of frs_data_formatted.csv")

        # frs_methods = FRS()
        # frs_methods.download_unzip_frs_data(combined=True)
        # frs_data = frs_methods.import_format_frs(combined=True)
        # frs_data.to_csv('./data/FRS/frs_data_formatted.csv', index=True)

    # Exclude all facilities that have neither EIS ID or
    # GHGRP ID

    # ghgrp_energy_file = GHGRP.main(year, year)
    ghgrp_energy_file = "ghgrp_energy_20240110-1837.parquet"
    ghgrp_unit_data = GHGRP_unit_char(ghgrp_energy_file, year).main()  # format ghgrp energy calculations to fit frs_json schema

    ghgrp_unit_data = check_registry_id(ghgrp_unit_data, frs_data)

    nei_data = NEI().main()

    nei_data = group_nei_by_unit(nei_data)

    data_dict = separate_unit_data(frs_data, nei_data, ghgrp_unit_data)

    shared_ocs_, shared_nonocs_ = blend_estimates(
        data_dict['nei_shared'],
        data_dict['ghgrp_shared']
        )

    final_energy_emissions_data = pd.concat(
        [shared_ocs_, shared_nonocs_, data_dict['ghgrp_only'],
         data_dict['nei_only']],
        axis=0, ignore_index=True
        )

    qpc_data = QPC().main(year)

    final_data = assemble_final_df(final_energy_emissions_data, frs_data, qpc_data,
                                   year=year)
    
    final_data = fiedgis.merge_geom(
        final_data, year=year, ftypes=['BG', 'CD'], 
        data_source='fied'
        )

    save_final_data(final_data, year)
