

import pandas as pd
from ghgrp.calc_GHGRP_energy import GHGRP
from ghgrp.calc_GHGRP_AA import subpartAA
import ghgrp.get_GHGRP_data

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

n3 = energy_ghgrp.PRIMARY_NAICS_CODE.unique()
n3 = pd.concat([n3, n3.apply(lambda x: int(str(x)[0:3]))], axis=1)
n3.columns = ['PRIMARY_NAICS_CODE', 'n3']
n3 = n3[n3.n3 == 311]

food_energy = pd.merge(
    energy_ghgrp, n3,
    on='PRIMARY_NAICS_CODE',
    how='inner' 
    )

food_energy.to_csv('onsite_food_data.csv', index=False)
