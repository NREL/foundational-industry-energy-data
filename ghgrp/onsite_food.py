

import pandas as pd
from io import BytesIO
import zipfile
import requests
from calc_GHGRP_energy import GHGRP
from calc_GHGRP_AA import subpartAA

start_year = 2011
end_year = 2022

ghgrp = GHGRP((start_year, end_year), calc_uncertainty=False)

ghgrp_data = {}

for k in ghgrp.table_dict.keys():

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

energy_ghgrp.to_pickle('all_ghgrp_energy.pkl')

n3 = pd.Series(energy_ghgrp.PRIMARY_NAICS_CODE.unique())
n3 = pd.concat([n3, n3.apply(lambda x: int(str(x)[0:3]))], axis=1)
n3.columns = ['PRIMARY_NAICS_CODE', 'n3']
n3 = n3[(n3.n3 == 311) | (n3.n3 == 312)]  # Food and beverages

food_sector = {}
energy = pd.merge(
    energy_ghgrp, n3,
    on='PRIMARY_NAICS_CODE',
    how='inner' 
    )

# GHG emissions are incomplete. Use different source, shown below.
energy = energy[
    ['PRIMARY_NAICS_CODE', 'FACILITY_ID', 'FUEL_TYPE', 'FUEL_TYPE_BLEND', 
     'FUEL_TYPE_OTHER', 'STATE', 'LATITUDE', 'LONGITUDE', 'REPORTING_YEAR', 'UNIT_NAME',
     'MMBtu_TOTAL']
    ]

for c in ['FUEL_TYPE_BLEND', 'FUEL_TYPE_OTHER']:
    energy.FUEL_TYPE.update(
        energy[energy[c].notnull()][c]
    )

    energy.drop(c, axis=1, inplace=True)

energy = energy.groupby([
    'PRIMARY_NAICS_CODE', 'FACILITY_ID', 'STATE', 'LATITUDE', 'LONGITUDE', 'REPORTING_YEAR',
    'FUEL_TYPE'], as_index=False
    )['MMBtu_TOTAL'].sum()


ghgrp_unit_url = 'https://www.epa.gov/system/files/other-files/2023-10/2022_data_summary_spreadsheets_0.zip'

r = requests.get(ghgrp_unit_url)

with zipfile.ZipFile(BytesIO(r.content)) as zf:
    with zf.open(zf.namelist()[-1]) as zd:
        ghg_data = pd.read_excel(zd, sheet_name='Direct Emitters',
                                 skiprows=3)

ghg_columns = ['Facility Id']

for y in range(2011, 2023):
    ghg_columns.append(y)
    ghg_data.rename(columns={f'{y} Total reported direct emissions': y},
                    inplace=True)
    
ghg_data = ghg_data[ghg_columns]

ghg_data = ghg_data.melt(
    'Facility Id', var_name='REPORTING_YEAR',
    value_name = 'MTCO2e'
    )

ghgs = pd.merge(
    energy.drop_duplicates(['FACILITY_ID', 'REPORTING_YEAR']), 
    ghg_data,
    left_on=['FACILITY_ID', 'REPORTING_YEAR'],
    right_on=['Facility Id', 'REPORTING_YEAR'],
    how='left'
    )

ghgs.drop(['Facility Id', 'FUEL_TYPE', 'MMBtu_TOTAL'], axis=1, inplace=True)

energy.to_csv(
    'c:/users/cmcmilla/industry-musings/OnSite/onsite_food_bevs_energy.csv', 
    index=False
    )
ghgs.to_csv(
    'c:/users/cmcmilla/industry-musings/OnSite/onsite_food_bevs_ghgs.csv',
    index=False
    )


