# TODO create executable?

import logging
import os
import yaml
import re
import sys
import pickle
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
import geocoder.geo_tools
# from geocoder.geo_tools import fcc_block_api
# from geocoder.geo_tools import get_blocks_parallelized
# from geocoder.get_tools


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

    mult = pd.DataFrame(
        data=data, columns=[col_names[1]],
        dtype=int
        )

    for c in [col_names[0], 'registryID']:
        mult.loc[:, c] = int(x[c])

    return mult


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


def blend_energy_estimates(nei_data_shared, ghgrp_data_shared):
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
    shared_ocs_energy : pandas.DataFrame
        Energy estimates for shared GHGRP units that were labelled
        as "Other combustion source (OCS)".

    shared_nonocs_energy : pandas.DataFrame
        Energy estimates for shared GHGRP units that were not labelled
        as "Other combustion source (OCS)".

    """

    ghgrp_data_shared_ocs = id_ghgrp_units(ghgrp_data_shared, ocs=True)

    nei_data_shared_ocs = id_nei_units_ocs(nei_data_shared,
                                           ghgrp_data_shared_ocs)

    ghgrp_data_shared_nonocs = id_ghgrp_units(ghgrp_data_shared, ocs=False)

    nei_data_shared_nonocs = id_nei_units_nonocs(nei_data_shared,
                                                 nei_data_shared_ocs)

    logging.info('Allocating shared OCS energy...')
    shared_ocs_energy = allocate_shared_ocs_energy(ghgrp_data_shared_ocs,
                                                   nei_data_shared_ocs)

    logging.info("Reonciling shared non-ocs energy...")
    shared_nonocs_energy = reconcile_shared_nonocs(nei_data_shared_nonocs,
                                                   ghgrp_data_shared_nonocs)

    return shared_ocs_energy, shared_nonocs_energy


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
        ('registryID', 'fuelType') in the corresponding GHGRP data.

    Returns
    -------
    nei_data_shared_nonocs : pandas.DataFrame
        The selection of data from nei_data_shared that does not correspond
        to nei_data_shared_ocs. If this is combined with nei_data_shared_ocs,
        it will result in the full nei_data_shared DataFrame.

    """

    # These entries do not have energy estimates
    nei_data_shared_na_fuelType = pd.DataFrame(
        nei_data_shared.query('fuelType.isnull()',
                              engine='python')
        )

    nei_data_shared.set_index(['registryID', 'fuelType'], inplace=True)
    nei_data_shared_ocs.set_index(['registryID', 'fuelType'], inplace=True)

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


def assign_estimate_source(df, source):
    """
    Create a column entitled 'estimateSource' and assign a value
    based on the source of energy estimates.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to assign estimate source. 

    source : str, {'ghgrp', 'nei'}
        Source of energy estimate.

    Returns
    -------
    df : pandas.DataFrame
        Original dataframe with new column and values indicating
        source of energy estimates. 
    """

    df.loc[:, 'estimateSource'] = source

    return df


def reconcile_shared_nonocs(nei_data_shared_nonocs, ghgrp_data_shared_nonocs):
    """
    Reconcile cases where facilities report to both NEI and GHGRP for
    units that are not identified as OCS (other combustion source)
    for a given (registryID, fuelType).

    Parameters
    ----------
    nei_data_shared_nonocs : pandas.DataFRame
        NEI data from facilities that also report to GHGRP, but 
        do not report units as OCS (Other combustion source)

    ghgrp_data_shared_nonocs : pandas.DataFrame
        GHGRP data from facilities that also report to NEI, but do
        not report units as OCS (Other combustion source) for a given
        (registryID, fuelType).

    Returns
    -------
    final_shared_nonocs : pands.DataFrame
        A compilation of unit-level estimates.

    """

    compare = pd.merge(
        nei_data_shared_nonocs.groupby(
            ['registryID', 'unitTypeStd', 'fuelType']
            ).agg({'designCapacity': 'sum', 'eisUnitID': 'count',
                   'energyMJq0': 'sum', 'energyMJq2': 'sum',
                   'energyMJq2': 'sum'}),
        ghgrp_data_shared_nonocs.groupby(
            ['registryID', 'unitTypeStd', 'fuelType']
            ).agg({'designCapacity': 'sum', 'unitName': 'count',
                   'energyMJ': 'sum'}),
        left_index=True,
        right_index=True,
        suffixes=('_nei', '_ghgrp'),
        how='outer'
        )

    use_nei_data = {}  # Dictionary for index selections that will use NEI data
    use_ghgrp_data = {}  # Dictionary for index selections that will use GHGRP data

    # Use GHGRP data for facilities where there is no unit information for
    # a given (registryID, unitTypeStd, fuelType)
    use_ghgrp_data['ghgrp_'] = compare.query("eisUnitID.isnull()", 
                                             engine='python')
    use_ghgrp_data['ghgrp_'].dropna(axis=1, inplace=True)
    use_ghgrp_data['ghgrp_'] = use_ghgrp_data['ghgrp_'].index

    # Use NEI data for facilities where there is no unit information for
    # a given (registryID, unitTypeStd, fuelType)
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
        "energyMJq2==0 & unitName.notnull()",
        engine='python'
        ).index

    # Use NEI data for facilities where there are GHGRP units, but
    # no energy estimates.
    use_nei_data['nei_no_ghgrpE'] = \
        compare.query("energyMJ==0 & energyMJq2>0").index

    # Use NEI data for facilities where there are GHGRP units, but
    # no energy estimates from either data set.
    use_nei_data['no_e'] = compare.query("energyMJ==0 & energyMJq2==0").index

    # Other challenge is how to select when it was possible to derive
    # energy estimates from NEI and GHGRP. Assume that GHGRP derivations 
    # are more robust.
    use_ghgrp_data['ghgrp_and_nei'] = compare.query("energyMJ>0 & energyMJq2>0").index

    nei_data_shared_nonocs.set_index(['registryID', 'unitTypeStd', 'fuelType'],
                                     inplace=True)

    ghgrp_data_shared_nonocs.set_index(['registryID', 'unitTypeStd', 'fuelType'],
                                       inplace=True)

    final_shared_nonocs_nei = pd.DataFrame()
    final_shared_nonocs_ghgrp = pd.DataFrame()

    for i in use_nei_data.keys():
        final_shared_nonocs_nei = final_shared_nonocs_nei.append(
            pd.DataFrame(nei_data_shared_nonocs.loc[use_nei_data[i]])
            )

    for i in use_ghgrp_data.keys():
        final_shared_nonocs_ghgrp = final_shared_nonocs_ghgrp.append(
            pd.DataFrame(ghgrp_data_shared_nonocs.loc[use_ghgrp_data[i]])
            )

    final_shared_nonocs_ghgrp = \
        assign_estimate_source(final_shared_nonocs_ghgrp, 'ghgrp')
    final_shared_nonocs_nei = \
        assign_estimate_source(final_shared_nonocs_nei, 'nei')

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
            ['registryID', 'fuelType']
            )[['registryID', 'fuelType']],
        on=['registryID', 'fuelType'],
        how='inner'
        )

    nei_data_shared_ocs.reset_index(inplace=True)

    return nei_data_shared_ocs


def id_ghgrp_units(ghgrp_data, ocs=True):
    """
    Identify ghgrp data for facilities that report to both GHGRP and NEI and 
    that do or don't report a unit type as "OCS (Other combustion source).

    Identifies OCS units across registryID, ghgrpID, and fuelType with the
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
    ghgrp_data_ = pd.DataFrame()

    gd_grpd = ghgrp_data.groupby(['registryID', 'ghgrpID', 'fuelType'])

    for g in gd_grpd.groups:

        try:
            gd_grpd.get_group(g)

        except KeyError:  # One row has NaN fuelType
            continue

        else:
            if ocs:
                if 'OCS (Other combustion source)' in gd_grpd.get_group(g).unitType.values:
                    ghgrp_data_ = pd.concat(
                        [ghgrp_data_, gd_grpd.get_group(g)], axis=0
                        )

                else:
                    continue

            else:
                if 'OCS (Other combustion source)' not in gd_grpd.get_group(g).unitType.values:
                    ghgrp_data_ = pd.concat(
                        [ghgrp_data_, gd_grpd.get_group(g)], axis=0
                        )

                else:
                    continue

    # ghgrp_data_.reset_index(inplace=True)

    return ghgrp_data_


# Not currently used. #TODO delete.
# def reconcile_nonocs_energy(ghgrp_data_shared_nonocs, nei_data_shared_nonocs):
#     """
#     Select between GHGRP and NEI energy estimates when a
#     facility doesn't report an OCS unit for a fuel type.
#     Currently defaults to using ghgrp data.

#     Parameters
#     ----------
#     ghgrp_data_shared_ocs : pandas.DataFrame
#         Selected GHGRP unit data for facilities that also report
#         to the NEI and report OCS (Other Combustion Source)

#     nei_data_shared_ocs : pandas.DataFrame
#         Selected NEI data for facilities that also report
#         to the GHGRP.

#     Returns
#     -------
#     shared_nonocs_energy : pd.DataFrame
#     """

#     # logging.info('------------\nPickling shared ocs datasets...\n-------------')
#     # nei_data_shared_nonocs.to_pickle('nei_data_shared_nonocs.pkl')
#     # ghgrp_data_shared_nonocs.to_pickle('ghgrp_data_shared_nonocs.pkl')

#     fac_unit_compare = pd.concat(
#         [nei_data_shared_nonocs.groupby(
#             ['registryID', 'unitTypeStd', 'fuelType']
#             )['energyMJq0', 'energyMJq2', 'energyMJq3'].sum(),
#          ghgrp_data_shared_nonocs.groupby(
#             ['registryID', 'unitTypeStd', 'fuelType']
#             ).energyMJ.sum()], axis=1
#         )

#     shared = pd.merge(
#         nei_data_shared_nonocs.dropna(subset=['registryID', 'unitTypeStd', 'fuelType']),
#         ghgrp_data_shared_nonocs.dropna(subset=['registryID', 'unitTypeStd', 'fuelType']),
#         how='outer',
#         on=['registryID', 'unitTypeStd', 'fuelType'],
#         indicator=True,
#         suffixes=('_nei', '_ghgrp')
#         )

#     shared_na = pd.concat(
#         [nei_data_shared_nonocs.dropna(subset=['registryID', 'unitTypeStd', 'fuelType']),
#          ghgrp_data_shared_nonocs.dropna(subset=['registryID', 'unitTypeStd', 'fuelType'])],
#         axis=0,
#         ignore_index=True
#         )

#     # Use GHGRP estimates 
#     shared[shared._merge == 'both'].where(shared.energyMJq0 < shared.energyMJ).dropna(how='all')

#     return ghgrp_data_shared_nonocs


def calc_share_ocs(ghgrp_data_shared_ocs, cutoff=0.5):
    """
    Calculate other combustion source (OCS) energy portion by registryID, fuelType,
    and unitTypeStd.
    Returns unit based on cutoff value (
    e.g., records where OCS share >=0.5 are returned with
    cutoff=0.5)

    Parameters
    ----------
    ghgrp_data_shared_ocs : pandas.DataFrame
        DataFrame with 

    cutoff : float, default=0.5
        OCS share of energy by registryID, fuelType, and
        unitTypeStd to identify unit

    Returns
    -------
    ocs_share : pandas.DataFrame
        Share of energy by resgistry, fuelType, and unitTypeStd
        that is OCS
    """

    ocs_share = ghgrp_data_shared_ocs.groupby(
        ['registryID', 'fuelType', 'unitTypeStd']
        ).energyMJ.sum()

    ocs_share = ocs_share.divide(
        ocs_share.sum(level=[0, 1])
        )

    ocs_share = pd.DataFrame(ocs_share).reset_index()

    ocs_share = ocs_share.query("unitTypeStd=='other combustion' & energyMJ>=@cutoff")

    return ocs_share


def allocate_shared_ocs_energy(ghgrp_data_shared_ocs, nei_data_shared_ocs):
    """
    Selects energy estimates from NEI or GHGRP when energy estimates
    are available from both sources for a facility. Also
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

    Returns
    -------
    ocs_energy : pd.DataFrame
        Energy estimates for units reported as OCS (other combustion source).
        Note that estimates may be provided from both NEI and GHGRP.
    """

    nei_data_shared_ocs.set_index(
        ['registryID', 'eisFacilityID', 'fuelType', 'eisProcessID',
         'eisUnitID'],
        inplace=True
        )

    nei_data_shared_ocs.sort_index(inplace=True)

    # Use min of nei energy estimates
    # This can be changed in the future
    nei_data_shared_portion = nei_data_shared_ocs.energyMJq0.sum(
        level=[0, 1, 2]
        )

    nei_data_shared_portion = nei_data_shared_portion.where(
        nei_data_shared_portion > 0
        ).dropna(how='all')

    nei_data_shared_portion = nei_data_shared_ocs.energyMJq0.divide(
            nei_data_shared_portion, fill_value=0
            )

    # Order of index levels is getting mixed up after the above division
    if nei_data_shared_portion.index.names == ['registryID', 'eisFacilityID', 'fuelType', 'eisUnitID']:
        nei_data_shared_portion = nei_data_shared_portion.swaplevel('eisUnitID', 'eisProcessID')

    else:
        pass

    nei_data_shared_portion.dropna(inplace=True)

    nei_data_shared_portion.name = 'energyMJPortion'

    # nei_data_shared_portion = pd.DataFrame(nei_data_shared_portion)

    # nei_data_shared_ocs = nei_data_shared_ocs.join(
    #     nei_data_shared_portion
    #     )

    # The join (as well as merge) were causing a crash
    # Updating the DataFrame was not working (nans weren't updated)
    # nei_data_shared_ocs.loc[:, 'energyMJPortion'] = np.nan
    # nei_data_shared_ocs.update(nei_data_shared_portion)

    nei_data_shared_ocs.loc[:, 'energyMJPortion'] = nei_data_shared_portion
    # nei_data_shared_ocs.dropna(subset=['energyMJPortion'], inplace=True)

    nei_data_shared_ocs.reset_index(
        ['eisFacilityID', 'eisProcessID', 'eisUnitID'], drop=False,
        inplace=True
        )
    # nei_data_shared_portion.reset_index(inplace=True)

    # nei_data_shared_ocs.loc[:, 'energyMJPortion'] = nei_data_shared_ocs.energyMJ.divide(
    #     nei_data_shared_portion.energyMJ, fill_value=0
    #     )

    ocs_share = calc_share_ocs(ghgrp_data_shared_ocs)

    ocs_share.set_index(['registryID', 'fuelType'], inplace=True)

    # nei_data_shared_ocs.set_index(['registryID', 'fuelType'], inplace=True)
    nei_data_shared_ocs.sort_index(inplace=True)
    ghgrp_data_shared_ocs.set_index(['registryID', 'fuelType'], inplace=True)

    ghgrp_data_shared_ocs.sort_index(inplace=True)

    ocs_energy = pd.DataFrame()

    #TODO refactor this for loop.
    for i in ghgrp_data_shared_ocs.index.drop_duplicates():

        if i in ocs_share.index:

            ghgrp_sum = ghgrp_data_shared_ocs.xs(i).energyMJ.sum()

            try:

                nei_sum = nei_data_shared_ocs.xs(i)[
                    ['energyMJq0', 'energyMJq2', 'energyMJq3']
                    ].sum()

            except KeyError:
                energy_use = pd.DataFrame(ghgrp_data_shared_ocs.loc[i, :])
                energy_use = assign_estimate_source(energy_use, 'ghgrp')

            else:

                if nei_sum[0] > 0:

                    # Assume that energy estimates derived from ghgrp are
                    # more robust than NEI derivations.
                    if nei_sum[0] < ghgrp_sum:

                        energy_use = \
                            pd.DataFrame(nei_data_shared_ocs.loc[i, :])

                        energy_use = assign_estimate_source(energy_use, 'nei')

                    else:
                        energy_use = \
                            pd.DataFrame(nei_data_shared_ocs.loc[i, :])
                        energy_use.loc[:, 'energyMJ'] = \
                            energy_use.loc[:, 'energyMJPortion'] * ghgrp_sum

                        energy_use = assign_estimate_source(energy_use, 'ghgrp')

                        # Remove NEI derivations in this case
                        energy_use.loc[:, [
                            'energyMJq0', 'energyMJq2', 'energyMJq3'
                            ]] = None

                else:

                    energy_use = pd.DataFrame(ghgrp_data_shared_ocs.loc[i, :])
                    energy_use = assign_estimate_source(energy_use, 'ghgrp')

        else:
            energy_use = pd.DataFrame(ghgrp_data_shared_ocs.loc[i, :])
            energy_use = assign_estimate_source(energy_use, 'ghgrp')

        ocs_energy = pd.concat(
            [ocs_energy, energy_use.reset_index()],
            axis=0, ignore_index=True, sort=True
            )

    ocs_energy.drop(['energyMJPortion'], axis=1, inplace=True)

    return ocs_energy


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
    
    nei_only_data = assign_estimate_source(nei_only_data, 'nei')

    # GHGRP facility IDs that don't report to NEI
    ghgrp_ids_noeis = melt_multiple_ids(ghgrp_no_eis, ghgrp_unit_data)

    ghgrp_only_data = pd.merge(
        ghgrp_ids_noeis['registryID'],
        ghgrp_unit_data, on='registryID',
        how='inner'
        )
    
    ghgrp_only_data = assign_estimate_source(ghgrp_only_data, 'ghgrp')

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

    return data_dict

# def fillin_ghgrp(final_data, year):
#     """
#     There are ~660 GHGRP-reporting facilities without FRS data.
#     Use additional GHGRP data to fill in missing info, such
#     as lat, long, name, etc.

#     Parameters
#     ----------
#     final_data : pandas.DataFrame
#         DataFrame after merging final_energy_data with frs_data

#     year : int
#         Data reporting year

#     Returns
#     -------
#     final_data : pandas.DataFrame
#         Original DataFrame with missing information (e.g., 
#         location, name) filled in with GHGRP data.

#     """

#     missing = final_data.query(
#         "name.isnull() & stateCode.isnull()", engine='python'
#         ).ghgrpID.unique()

#     table = 'V_GHG_EMITTER_FACILITIES'

#     ghgrp_data = get_GHGRP_records(year, table)

#     ghgrp_data = ghgrp_data[ghgrp_data.FACILITY_ID.isin(missing)]

#     col_rename = {
#         'FACILITY_ID': 'ghgrpID',
#         'FACILITY_NAME': 'name',
#         'ADDRESS1': 'locationAddress',
#         'ZIP': 'postalCode',
#         'LATITUDE': 'latitude',
#         'LONGITUDE': 'longitude',
#         'CITY': 'cityName',
#         'COUNTY': 'countyName',
#         'COUNTY_FIPS': 'countyFIPS',
#         'PRIMARY_NAICS_CODE': 'naicsCode',
#         'ADDITIONAL_NAICS_CODES': 'naicsCodeAdditional',
#         'STATE': 'stateCode'
#         }

#     ghgrp_data.rename(columns=col_rename, inplace=True)

#     grab_additional_naics = ghgrp_data[
#         ghgrp_data.SECONDARY_NAICS_CODE.notnull() | ghgrp_data.naicsCodeAdditional.notnull()
#         ]

#     # Combine any secondary NAICS or additional NAICS as additional NAICS
#     for i, r in grab_additional_naics.iterrows():

#         add_naics = []

#         if r['naicsCodeAdditional']:
#             add_naics = [int(x) for x in r['naicsCodeAdditional'].split(',')]

#         else:
#             pass

#         if r['SECONDARY_NAICS_CODE']:
#             add_naics.append(int(r['SECONDARY_NAICS_CODE']))

#         else:
#             pass

#         try:
#             add_naics[0]

#         except IndexError:
#             grab_additional_naics.loc[i, 'naicsCodeAdditional'] = None

#         else:
#             grab_additional_naics.loc[i, 'naicsCodeAdditional'] = add_naics

#     ghgrp_data.loc[:, 'naicsCodeAdditional'] = grab_additional_naics.naicsCodeAdditional

#     ghgrp_data = pd.DataFrame(ghgrp_data[[v for v in col_rename.values()]]).set_index('ghgrpID')

#     final_data.set_index('ghgrpID', inplace=True)
#     final_data.update(ghgrp_data)

#     final_data.reset_index(inplace=True, drop=False)

#     return final_data


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

    final_energy_data.to_pickle('initial_final_energy_data.pkl')

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

    # energy_missing_ghgrp.to_pickle('energy_missing_ghgrp.pkl')
    final_data.update(energy_missing_ghgrp)

    final_data.drop(['ghgrpID_x', 'SCC'], inplace=True, axis=1)
    final_data.rename(columns={'ghgrpID_y': 'ghgrpID'}, inplace=True)

    final_data = merge_qpc_data(final_data, qpc_data)

    logging.info('Finding Census Blocks. This takes awhile...')
    final_data = geocoder.geo_tools.get_blocks_parallelized(final_data)
    final_data = geocoder.geo_tools.fix_county_fips(final_data)
    final_data = geocoder.geo_tools.find_missing_congress(final_data)

    # #TODO something is going wrong with finding these missing HUCs; 
    # the script silently crashes.
    # logging.info('Finding missig HUC Codes. This takes awhile...')
    # frs_api = FRS_API()

    # missing_huc = frs_api.find_huc_parallelized(final_data)  # This method is the source of problems.

    # try:
    #     missing_huc[0]

    # except IndexError as e:
    #     logging.error(f"{e}")
    #     missing_huc.to_pickle('missing_huc_results.pkl')

    # except NameError as e:
    #     logging.error(f"{e}\n find_huc_parallelized failed")

    # else:
    #     final_data.hucCode8.update(
    #         frs_data.registryID.map(missing_huc)
    #         )

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
    ghgrp_energy_file = "ghgrp_energy_20230508-1606.parquet"
    ghgrp_unit_data = GHGRP_unit_char(ghgrp_energy_file, year).main()  # format ghgrp energy calculations to fit frs_json schema

    nei_data = NEI().main()

    data_dict = separate_unit_data(frs_data, nei_data, ghgrp_unit_data)

    shared_ocs_energy, shared_nonocs_energy = blend_energy_estimates(
        data_dict['nei_shared'],
        data_dict['ghgrp_shared']
        )

    final_energy_data = pd.concat(
        [shared_ocs_energy, shared_nonocs_energy, data_dict['ghgrp_only'],
         data_dict['nei_only']],
        axis=0, ignore_index=True
        )

    qpc_data = QPC().main(year)

    final_data = assemble_final_df(final_energy_data, frs_data, qpc_data,
                                   year=year)

    save_final_data(final_data, year)
