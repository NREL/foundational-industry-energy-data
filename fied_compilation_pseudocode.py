
# TODO create /bin  for keeping code to run to compile data set
# TODO create executable?

import logging
import yaml
import numpy as np
import pandas as pd
import ghgrp.run_GHGRP as GHGRP
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char
from nei.nei_EF_calculations import NEI
from frs.frs_tools import FRS

year = 2017

frs_methods = FRS()
frs_methods.download_unzip_frs_data(combined=True)
frs_data = frs_methods.import_format_frs(combined=True)

ghgrp_energy_file = GHGRP.main(year, year)
ghgrp_fac_energy = GHGRP_unit_char(ghgrp_energy_file, year)  # format ghgrp energy calculations to fit frs_json schema

nei_data = NEI().main()

def frs_match_program():
    """
    
    """

def split_multiple(x, col_names):
    """"
    
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

def load_fueltype_dict():

    """
    
    """
    with open('./tools/type_standardization.yml', 'r') as file:
        fuel_dict = yaml.safe_load(file)

    return fuel_dict 

def frs_melt_multiple(frs_data, other_data):
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

    for name in ['ghgrpID', 'eisFacilityID']:

        if name in other_data.columns:

            col_names = [name, f'{name}Additional']

        else:
            continue

    try:
        col_names[0]

    except IndexError as e:
        logging.error(f'Check column names in other_data: {e}')

    frs_mult = frs_data[frs_data[col_names[1]].notnull()]

    frs_mult = pd.concat(
        [split_multiple(d, col_names) for i, d in frs_mult.iterrows()],
        axis=0, ignore_index=True
        )

    frs_mult = frs_mult.melt(
        id_vars=['registryID'], value_name = col_names[0]
        ).drop('variable', axis=1)

    melted = pd.concat([
        frs_mult, frs_data[frs_data[col_names[1]].isnull()][['registryID', col_names[0]]]
        ],
        axis=0, ignore_index=True
        )

    return melted

def harmonize_unit_data(eis_and_ghgrp, ghgrp_unit_data, nei_data):
    """"
    Select approporiate unit data where both GHGRP 
    and NEI report unit data. 

    Parameters
    ----------
    eis_and_ghgrp : pandas.DataFrame
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
    harm_data : pandas.DataFrame

    """

    eis_ids = frs_melt_multiple(eis_and_ghgrp, nei_data)
    ghgrp_ids = frs_melt_multiple(eis_and_ghgrp, ghgrp_unit_data)

    ghgrp_data = pd.merge(ghgrp_ids[['ghgrpID']], ghgrp_unit_data, on='ghgrpID', how='inner')
    ghgrp_data_ocs = ghgrp_data.query("unitType=='OCS (Other combustion source)'")

    nei_data_harm = pd.merge(eis_ids, nei_data, on='eisFacilityID', how='inner')

    nei_data_harm_portion =  nei_data_harm.groupby(
        ['registryID', 'eisFacilityID', 'eisProcessID', 'eisUnitID', 'fuelType']
        ).energyMJ.sum()

    return

def layer_unit_data(frs_data, nei_data, ghgrp_unit_data):
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
    """

    fuel_dict = load_fueltype_dict()

    ghgrp_unit_data.fuelType.update(
        ghgrp_unit_data.fuelType.map(fuel_dict)
        )

    eis_no_ghgrp = frs_data.query('eisFacilityID.isnull() & ghgrpID.notnull()',
                                  engine='python')

    ghgrp_no_eis = frs_data.query('eisFacilityID.notnull() & ghgrpID.isnull()',
                                  engine='python')

    eis_and_ghgrp = frs_data.query('eisFacilityID.notnull() & ghgrpID.notnull()',
                                   engine='python')

    # For cases where facility is both GHGRP and EIS:
    # if ghgrp.unitType == OCS & (nei.fuelType == ghgrp.fuelType):  # Need to harmonize fuel types (write method to Tools)
    #   if -0.2 < (sum(ghgrp.energyMJ/)sum(nei.energyMJ) - 1) < 0.2:
    #       use nei data
    #   else:
    #       method to allocate GHGRP energy to NEI unit types (with same fuelType)

    eis_ids = frs_melt_multiple(eis_no_ghgrp, nei_data)

    ghgrp_ids = frs_melt_multiple(ghgrp_no_eis, ghgrp_unit_data)

    eis_only_data = pd.merge(
        eis_ids,
        nei_data, on='eisFacilityID',
        how='inner'
        )

    ghgrp_only_data = pd.merge(
        ghgrp_ids['ghgrpID'],
        ghgrp_unit_data, on='ghgrpID',
        how='inner'
        )
    
    return




mining_energy = calc_mining_energy(year)  # Estimate mining energy intensity by NAICS, fuel, and location

ag_energy = calc_ag_energy(year)  # Estimate ag energy intensity by NAICS, fuel, and location

q_hours = calc_quarterly_op_hours(year)  # Get quarterly estimates of weekly operating hours from Censusn with standard error +/- [est, +, min]

# Need a generic method for quickly matching sub-6-digit NAICS to 6-D NAICS

frs_json = merge_and_make_json(frs_facs, ghgrp_fac_energy)


