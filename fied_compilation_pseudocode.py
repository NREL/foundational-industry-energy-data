
# TODO create /bin  for keeping code to run to compile data set
# TODO create executable?

import logging
import yaml
import re
import pickle
import pandas as pd
# import ghgrp.run_GHGRP as GHGRP
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char
from nei.nei_EF_calculations import NEI
from frs.frs_extraction import FRS


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

        # data = [[x[col_names[0]], x[col_names[1]]]]
        # index = [x.registryID]

    if type(x[col_names[1]]) is str:

        try:
            data = [int(x[col_names[1]])]

        except ValueError:

            data = x[col_names[1]].split(', ')
            # index = np.repeat(
            #     x.registryID, len(data)
            #     )
            # data = [[
            #     np.repeat(
            #         x[col_names[0]],
            #         len(x[col_names[1]].split(', '))),
            #     x[col_names[1]].split(', ')
            #     ]],
            # index = np.repeat(
            #     x.registryID,
            #     len(x[col_names[1]].split(', '))
            #     )

        # else:
            # data = [x[col_names[0]], x[col_names[1]]]
            # index = [x.registryID]

    elif type(x[col_names[1]]) is float:

        data = [x[col_names[1]]]

    else:
        return

    mult = pd.DataFrame(
        data=data, columns=[col_names[1]],
        dtype=int
        )

    for c in [col_names[0], 'registryID']:
        mult.loc[:, c] = int(x[c])

    return mult


def main():
    year = 2017

    frs_methods = FRS()
    frs_methods.download_unzip_frs_data(combined=True)
    frs_data = frs_methods.import_format_frs(combined=True)
    frs_data.to_csv('./data/FRS/frs_data_formatted.csv', index=True)

    # ghgrp_energy_file = GHGRP.main(year, year)
    ghgrp_energy_file = "ghgrp_energy_20230508-1606.parquet"
    ghgrp_fac_energy = GHGRP_unit_char(ghgrp_energy_file, year).main()  # format ghgrp energy calculations to fit frs_json schema

    #nei_data = NEI().main()
    nei_data = pd.read_csv('formatted_estimated_nei.csv')


def melt_multiple_ids(frs_data, other_data):
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

    # for name in ['ghgrpID', 'eisFacilityID']:

    #     if name in other_data.columns:

    #         col_names = [name, f'{name}Additional']

    #     else:
    #         continue

    # try:
    #     col_names[0]

    # except IndexError as e:
    #     logging.error(f'Check column names in other_data: {e}')

    frs_mult = frs_data[frs_data[col_names[1]].notnull()]

    frs_mult = pd.concat(
        [split_multiple(d, col_names) for i, d in frs_mult.iterrows()],
        axis=0, ignore_index=True
        )

    frs_mult = frs_mult.melt(
        id_vars=['registryID'], value_name=col_names[0]
        ).drop('variable', axis=1)

    melted = pd.concat([
        frs_mult, frs_data[frs_data[col_names[1]].isnull()][['registryID', col_names[0]]]
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
    nei_and_ghgrp : pandas.DataFrame
        Selection of frs_data where facilities report to both
        GHGRP and NEI

    ghgrp_unit_data : pandas.DataFrame
        GHGRP unit data for facilities that also report
        to NEI.

    nei_data : pandas.DataFrame
        NEI unit data for facilities that also report
        to GHGRP

    Returns
    -------
    merged_data : pandas.DataFrame

    """

    # nei_ids = melt_multiple_ids(frs_data, nei_data)
    # ghgrp_ids = melt_multiple_ids(frs_data, ghgrp_unit_data)

    # ghgrp_data_shared = pd.merge(
    #     ghgrp_ids[['ghgrpID']],
    #     ghgrp_unit_data,
    #     on='ghgrpID',
    #     how='inner'
    #     )

    ghgrp_data_shared_ocs = id_ghgrp_units(ghgrp_data_shared, ocs=True)

    nei_data_shared_ocs = id_nei_units(nei_data_shared, ghgrp_data_shared_ocs)

    # nei_data_shared_ocs = pd.DataFrame(
    #     nei_data_shared[nei_data_shared.registryID.isin(
    #         ghgrp_data_shared_ocs.registryID.unique()
    #         )]
    #     )

    #  TODO Return these dataframes, alon with the _ocs versions?
    # #TODO only return energy estimates for ghgrp_shared_nonocs (disregard nei_shared estimates), 
    # shared_ocs_energy[including ranges], and nei_only [with ranges] and ghgrp_only?
    ghgrp_data_shared_nonocs = id_ghgrp_units(ghgrp_data_shared, ocs=False)

    nei_data_shared_nonocs = id_nei_units(nei_data_shared,
                                          ghgrp_data_shared_nonocs)

    logging.info('Allocating shared OCS energy...')
    shared_ocs_energy = allocate_shared_ocs_energy(ghgrp_data_shared_ocs,
                                                   nei_data_shared_ocs)

    logging.info("Reonciling shared non-ocs energy...")
    shared_nonocs_energy = reconcile_nonocs_energy(ghgrp_data_shared_nonocs,
                                                   nei_data_shared_nonocs)

    return shared_ocs_energy, shared_nonocs_energy


def id_nei_units(nei_data_shared, ghgrp_data_shared_):
    """
    Identify GHGRP- and NEI-reporting facilities that do or don't
    report a unit type as "OCS (Other combustion source).

    Parameters
    ----------
    nei_data_shared : pandas.DataFrame
        DataFrame of NEI facilities that also report GHGRP data

    ghgrp_data_shared_ : pandas.DataFrame
        GHGRP data with facilities that do or don't report OCS
        units

    Returns
    -------
    nei_data_shared_ : pandas.DataFrame
        NEI data for facilities that either have or
        don't have OCS units under their GHGRP reporting.    
    """

    nei_data_shared_ = pd.DataFrame(nei_data_shared)

    nei_data_shared_ = pd.merge(
        nei_data_shared_,
        ghgrp_data_shared_.drop_duplicates(
            ['registryID', 'fuelType']
            )[['registryID', 'fuelType']],
        on=['registryID', 'fuelType'],
        how='inner'
        )

    return nei_data_shared_


def id_ghgrp_units(ghgrp_data, ocs=True):
    """
    Identify GHGRP- and NEI-reporting facilities that do or don't
    report a unit type as "OCS (Other combustion source).

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


def reconcile_nonocs_energy(ghgrp_data_shared_nonocs, nei_data_shared_nonocs):
    """
    Select between GHGRP and NEI energy estimates when a 
    facility doesn't report an OCS unit for a fuel type.

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
    shared_nonocs_energy : pd.DataFrame
    """

    logging.info('------------\nPickling shared ocs datasets...\n-------------')
    nei_data_shared_nonocs.to_pickle('nei_data_shared_nonocs.pkl')
    ghgrp_data_shared_nonocs.to_pickle('ghgrp_data_shared_nonocs.pkl')

    return None


def calc_share_ocs(ghgrp_data_shared_ocs, cutoff=0.5):
    """
    Calculate OCS energy portion by ghgrpID, fuelType,
    and unitTypeStd.
    Returns unit based on cutoff value (
    e.g., records where OCS share >=0.5 are returned with
    cutoff=0.5)
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

    """

    logging.info('pickling shared ocs datasets...')
    nei_data_shared_ocs.to_pickle('nei_data_shared_ocs.pkl')
    ghgrp_data_shared_ocs.to_pickle('ghgrp_data_shared_ocs.pkl')

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

    nei_data_shared_portion = nei_data_shared_ocs.energyMJq0.divide(
            nei_data_shared_portion, fill_value=0
            )

    nei_data_shared_portion.name = 'energyMJPortion'

    nei_data_shared_portion = pd.DataFrame(nei_data_shared_portion)

    nei_data_shared_ocs = nei_data_shared_ocs.join(
        nei_data_shared_portion
        )

    # nei_data_shared_ocs = pd.concat(
    #     [nei_data_shared_ocs, nei_data_shared_portion], axis=1
    #     )

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

    for i in ghgrp_data_shared_ocs.index.drop_duplicates():

        if i in ocs_energy.index:

            ghgrp_sum = ghgrp_data_shared_ocs.xs(i).energyMJ.sum()

            try:

                nei_sum = nei_data_shared_ocs.xs(i)[
                    ['energyMJq0', 'energyMJq2', 'energyMJq3']
                    ].sum()

                logging.info(f'NEI sum: {nei_sum}')

                # nei_sum = nei_data_shared_ocs.xs(i).energyMJ.sum()

            except KeyError as e:
                logging.error(f'Check fuel type standardization: {e}')

            if nei_sum[0] > 0:

                if nei_sum[0] < ghgrp_sum:

                # if nei_sum > ghgrp_sum:

                    energy_use = nei_data_shared_ocs.loc[i, 'energyMJq0']

                else:

                    energy_use = nei_data_shared_ocs.loc[i, 'energyMJPortion'] * ghgrp_sum

            else:

                energy_use = ghgrp_data_shared_ocs.loc[i, :]

        else:
            energy_use = ghgrp_data_shared_ocs.loc[i, :]

        ocs_energy = pd.concat(
            [ocs_energy, energy_use.reset_index()],
            axis=0, ignore_index=True, sort=True
            )

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

    unit_types = [
        'kiln', 'dryer', 'oven', 'furnace',
        'boiler', 'incinerator', 'flare',
        'heater', 'calciner', 'turbine',
        'stove', 'distillation', 'other combustion',
        'engine', 'generator', 'oxidizer', 'pump',
        'compressor', 'building heat', 'cupola',
        'PCWD', 'PCWW', 'PCO', 'PCT', 'OFB'
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

    if (len(ut_std) > 1):
        ut_std = 'other'

    elif (len(ut_std) == 0):
        ut_std = None

    elif ut_std[0] == 'calciner':
        ut_std = 'kiln'

    elif ut_std[0] == 'oxidizer':
        ut_std = 'thermal oxidizer'

    elif ut_std[0] == 'buidling heat':
        ut_std = 'heater'

    elif ut_std[0] == 'cupola':
        ut_std = 'other combustion'

    elif any([x in ut_std[0] for x in other_boilers]):
        ut_std = 'boiler'

    else:
        ut_std = ut_std[0]

    return ut_std


def separate_unit_data(frs_data, nei_data, ghgrp_unit_data):
    """
    All facilities have FRS ID. Not all GHGRP facilities have EIS IDs and
    vice versa.

    Parameters
    ----------

    frs_data :

    nei_data :

    ghgrp_unit_id :

    Returns
    -------
    data_dict : dictionary of pandas.DataFrames
    """

    # Harmonize/standardize unit types
    nei_data = harmonize_unit_type(nei_data)
    ghgrp_unit_data = harmonize_unit_type(ghgrp_unit_data)

    nei_data.to_pickle('nei_data.pkl')
    ghgrp_unit_data.to_pickle('ghgrp_data_postharm.pkl')

    nei_no_ghgrp = frs_data.query(
        'eisFacilityID.notnull() & ghgrpID.isnull()', engine='python'
        )

    ghgrp_no_eis = frs_data.query(
        'eisFacilityID.isnull() & ghgrpID.notnull()', engine='python'
        )

    nei_and_ghgrp = frs_data.query(
        'eisFacilityID.notnull() & ghgrpID.notnull()', engine='python'
        )

    # For cases where facility is both GHGRP and EIS:
    # if ghgrp.unitType == OCS & (nei.fuelType == ghgrp.fuelType):  # Need to harmonize fuel types (write method to Tools)
    #   if -0.2 < (sum(ghgrp.energyMJ/)sum(nei.energyMJ) - 1) < 0.2:
    #       use nei data
    #   else:
    #       method to allocate GHGRP d to NEI unit types (with same fuelType)

    # EIS facility IDs that don't report to GHGRP
    nei_ids_noghgrp = melt_multiple_ids(nei_no_ghgrp, nei_data)

    nei_only_data = pd.merge(
        nei_ids_noghgrp,
        nei_data, on='eisFacilityID',
        how='inner'
        )

    logging.info(f'NEI IDS no GHGRP len:{len(nei_ids_noghgrp)}')
    logging.info(
        f"Lens after merge (ids, merged data, nei data): ({len(nei_only_data.eisFacilityID.unique())}, {len(nei_only_data)}, {len(nei_data)}"
        )

    # GHGRP facility IDs that don't report to NEI
    ghgrp_ids_noeis = melt_multiple_ids(ghgrp_no_eis, ghgrp_unit_data)

    ghgrp_only_data = pd.merge(
        ghgrp_ids_noeis['ghgrpID'],
        ghgrp_unit_data, on='ghgrpID',
        how='inner'
        )

    logging.info(f'GHGRP IDS no NEI len:{len(ghgrp_ids_noeis)}')
    logging.info(
        f"Lens after merge (ids, merged data, ghgrp data): ({len(ghgrp_ids_noeis.ghgrpID.unique())}, {len(ghgrp_only_data)}, {len(ghgrp_unit_data.ghgrpID.unique())}"
        )

    # NEI and GHGRP facilities
    nei_ids_shared = melt_multiple_ids(nei_and_ghgrp, nei_data)

    nei_data_shared = pd.merge(
        nei_ids_shared,
        nei_data, on='eisFacilityID',
        how='inner'
        )

    logging.info(f'NEI IDS and ghgrp len:{len(nei_ids_shared)}')
    logging.info(
        f"Lens after merge (ids, merged data, nei data): ({len(nei_ids_shared.registryID.unique())}, {len(nei_data_shared)}, {len(nei_data)}"
        )

    ghgrp_ids_shared = melt_multiple_ids(nei_and_ghgrp, ghgrp_unit_data)

    ghgrp_data_shared = pd.merge(
        ghgrp_ids_shared[['ghgrpID']],
        ghgrp_unit_data,
        on='ghgrpID',
        how='inner'
        )

    logging.info(f'GHGRP IDS and NEI len:{len(ghgrp_ids_shared)}')
    logging.info(
        f"Lens after merge (ids, merged data, nei data): ({len(ghgrp_ids_shared.registryID.unique())}, {len(ghgrp_data_shared)}, {len(ghgrp_unit_data)}"
        )

    data_dict = {
        'nei_shared': nei_data_shared,  # EIS data for facilities without GHGRP reporting
        'nei_only': nei_only_data,  # #TODO report out for final data set
        'ghgrp_shared': ghgrp_data_shared,
        'ghgrp_only': ghgrp_only_data  # #TODO Report out for final data set
        }

    return data_dict


if __name__ == '__main__':
    year = 2017

    try:
        frs_data = pd.read_csv(
            './data/FRS/frs_data_formatted.csv', low_memory=False
            )

    except FileNotFoundError:

        frs_methods = FRS()
        frs_methods.download_unzip_frs_data(combined=True)
        frs_data = frs_methods.import_format_frs(combined=True)
        frs_data.to_csv('./data/FRS/frs_data_formatted.csv', index=True)

    # ghgrp_energy_file = GHGRP.main(year, year)
    ghgrp_energy_file = "ghgrp_energy_20230508-1606.parquet"
    ghgrp_unit_data = GHGRP_unit_char(ghgrp_energy_file, year).main()  # format ghgrp energy calculations to fit frs_json schema
    ghgrp_unit_data.registryID.update(ghgrp_unit_data.registryID.astype(float))

    nei_data = NEI().main()
    # nei_data = pd.read_csv(
    #     'formatted_estimated_nei_updated.csv', low_memory=False, index_col=0,
    #     )

    data_dict = separate_unit_data(frs_data, nei_data, ghgrp_unit_data)

    logging.info('Pickling energy data')
    with open('energy_data_dict.pkl', 'wb') as handle:
        pickle.dump(data_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    ocs_energy, nonocs_energy = blend_energy_estimates(
        data_dict['nei_shared'],
        data_dict['ghgrp_shared']
        )

    logging.info('Pickling ocs-related')
    ocs_energy.to_pickle('ocs_energy.pkl')
    nonocs_energy.to_pickle('nonocs_energy.pkl')

# mining_energy = calc_mining_energy(year)  # Estimate mining energy intensity by NAICS, fuel, and location

# ag_energy = calc_ag_energy(year)  # Estimate ag energy intensity by NAICS, fuel, and location

# q_hours = calc_quarterly_op_hours(year)  # Get quarterly estimates of weekly operating hours from Censusn with standard error +/- [est, +, min]

# # Need a generic method for quickly matching sub-6-digit NAICS to 6-D NAICS

# frs_json = merge_and_make_json(frs_facs, ghgrp_fac_energy)


