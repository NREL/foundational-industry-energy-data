# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
Created on Tue Aug 13 10:56:14 2019

@author: cmcmilla
"""

import pandas as pd
import dask.dataframe as dd
import os
import mining
import numpy as np

mining

mining_files = []

# Import the most recent run
for f in os.listdir('../results'):

    if 'mining' in f:

        mining_files.append(f)

    else:

        continue

mining_files.sort()

county_mining = pd.read_csv('../results/'+mining_files[-1])

county_mining['fipstate'] = county_mining.fipstate.astype(int)

county_mining.rename(columns={'fuel_type': 'MECS_FT'}, inplace=True)

county_mining.replace({'COAL': 'Coal', 'CRUDE OIL': 'Other',
                       'DIESEL': 'Diesel', 'MISC': 'Other',
                       'GASOLINE': 'Other',
                       'RESIDUAL FUEL OIL': 'Residual_fuel_oil',
                       'NATURAL GAS': 'Natural_gas',
                       'ELECTRICITY': 'Net_electricity'}, inplace=True)

county_mining.drop(['county'], axis=1, inplace=True)

county_total = county_mining.melt(
        id_vars=['NAICS', 'COUNTY_FIPS', 'fipstate', 'MECS_FT', 'state'],
        var_name='year', value_name='MMBtu_TOTAL'
        )

county_total['year'] = county_total.year.astype(int)

# Import GHGRP data for mining
energy_ghgrp = pd.read_parquet(
        '../results/ghgrp_energy_20191108-1416.parquet', engine='pyarrow',
        columns=['COUNTY_FIPS', 'FUEL_TYPE', 'FUEL_TYPE_BLEND',
                 'FUEL_TYPE_OTHER', 'MMBtu_TOTAL', 'PRIMARY_NAICS_CODE',
                 'REPORTING_YEAR', 'STATE_NAME']
        )

energy_ghgrp.rename(columns={'PRIMARY_NAICS_CODE': 'NAICS',
                             'REPORTING_YEAR': 'year',
                             'STATE_NAME': 'state'}, inplace=True)

energy_ghgrp = energy_ghgrp[
        energy_ghgrp.NAICS.between(210000, 219999)
        ]

fuelxwalkDict = dict(
        pd.read_csv('../calculation_data/MECS_FT_IPF.csv')[
                ['EPA_FUEL_TYPE', 'MECS_FT']
                ].values
        )

energy_ghgrp['MECS_FT'] = np.nan

for f in ['FUEL_TYPE_OTHER','FUEL_TYPE_BLEND', 'FUEL_TYPE']:

        energy_ghgrp['MECS_FT'].update(
                energy_ghgrp[f].map(fuelxwalkDict)
                )

energy_ghgrp.drop(['FUEL_TYPE_OTHER','FUEL_TYPE_BLEND', 'FUEL_TYPE'], axis=1,
                  inplace=True)

energy_ghgrp = pd.merge(
        energy_ghgrp,
        county_total[['state', 'fipstate']].drop_duplicates(),
        on='state', how='left'
        )

county_total = county_total.append(energy_ghgrp)

county_total = dd.from_pandas(
    county_total.set_index('fipstate'),
    npartitions=len(county_total.fipstate.unique())
    )

filename = mining_files[-1].split('.')[0]+'.parquet.gzip'

county_total.to_parquet('../results/'+filename, engine='pyarrow',
                        compression='gzip')
