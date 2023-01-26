
year = 2017

frs_facs = get_frs_data()  # get EPA FRS data. This is the foundation of the data set

ghgrp_energy = calc_ghgrp_energy(year)  # Estimate combustion energy by 

ghgrp_fac_energy = convert_ghgrp_energy(ghgrp_energy)  # format ghgrp energy calculations to fit frs_json schema

nei_data = get_nei_data(year)  # Download and format NEI data

nei_fac_data = =cal_nei_data(nei_data)  # Calculations

mining_energy = calc_mining_energy(year)  # Estimate mining energy intensity by NAICS, fuel, and location

ag_energy = calc_ag_energy(year)  # Estimate ag energy intensity by NAICS, fuel, and location

q_hours = calc_quarterly_op_hours(year)  # Get quarterly estimates of weekly operating hours from Censusn with standard error +/- [est, +, min]

# Need a generic method for quickly matching sub-6-digit NAICS to 6-D NAICS

frs_json = merge_and_make_json(frs_facs, ghgrp_fac_energy)


