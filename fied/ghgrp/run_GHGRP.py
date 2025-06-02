# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 14:26:54 2019

@author: cmcmilla
"""
import os
import logging
import ghgrp_fac_unit
import datetime as dt
from calc_GHGRP_energy import GHGRP
from calc_GHGRP_AA import subpartAA

module_logger = logging.getLogger(__name__)

def main(start_year, end_year):
    """
    """
    module_logger.info("Starting GHGRP energy calculations")
    module_logger.debug(f"Start year: {start_year}, End year: {end_year}")

    # Uncertainty calculations are not fully operational
    ghgrp = GHGRP((start_year, end_year), calc_uncertainty=False, fix_county_fips=False)

    ghgrp_data = {}

    for k in ghgrp.table_dict.keys():
        module_logger.debug(f"Processing {k}")

        ghgrp_data[k] = ghgrp.import_data(k)

    energy_subC = ghgrp.calc_energy_subC(ghgrp_data['subpartC'],
                                         ghgrp_data['subpartV_fac'])

    energy_subD = ghgrp.calc_energy_subD(ghgrp_data['subpartD'],
                                         ghgrp_data['subpartV_fac'])

    energy_subAA = subpartAA(aa_ff=ghgrp_data['subpartAA_ff'],
                             aa_sl=ghgrp_data['subpartAA_liq'],
                             std_efs=ghgrp.std_efs).energy_calc()

    energy_ghgrp = ghgrp.energy_merge(energy_subC, energy_subD, energy_subAA,
                                      ghgrp_data['subpartV_fac'])

    time = dt.datetime.today().strftime("%Y%m%d-%H%M")

    ghgrp_file = Path("data/GHGRP") / f'ghgrp_energy_{time}.parquet'

    # Save results
    energy_ghgrp.to_parquet(ghgrp_file, engine='pyarrow', compression='gzip')

    return ghgrp_file


if __name__ == '__main__':

    reporting_year = 2017
    ghgrp_file = main(reporting_year, 2017)
    ghgrp_df = ghgrp_fac_unit.GHGRP_unit_char(ghgrp_file, reporting_year).main()
