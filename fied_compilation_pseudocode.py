
# TODO create /bin  for keeping code to run to compile data set
# TODO create executable?

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


mining_energy = calc_mining_energy(year)  # Estimate mining energy intensity by NAICS, fuel, and location

ag_energy = calc_ag_energy(year)  # Estimate ag energy intensity by NAICS, fuel, and location

q_hours = calc_quarterly_op_hours(year)  # Get quarterly estimates of weekly operating hours from Censusn with standard error +/- [est, +, min]

# Need a generic method for quickly matching sub-6-digit NAICS to 6-D NAICS

frs_json = merge_and_make_json(frs_facs, ghgrp_fac_energy)


