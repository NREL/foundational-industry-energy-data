"""

Get Facility FRS IDs for NAICS whole industrial sector

"""

import pandas as pd

frs = pd.read_csv('NATIONAL_SINGLE.csv')


# remove nan in NAICS column; 
# split up list that has multiple NAICS assigned to one facility
frs.dropna(subset=['NAICS_CODES'],inplace=True)

frs.drop(frs[frs['NAICS_CODES'].str.contains('-', na=False)].index,
         inplace=True)

frs['NAICS_CODES'] = pd.to_numeric(frs['NAICS_CODES'],errors='ignore')

frs.loc[frs['NAICS_CODES'].str.contains(',',na=False),'NAICS_CODES'] = \
    frs['NAICS_CODES'].str.split(',')


# expand list of multiple NAICS per one facilitiy into new lines, same index
frs_exp = frs.explode('NAICS_CODES')

frs_exp['NAICS_CODES'] = frs_exp.NAICS_CODES.str.replace(" ","")

frs_exp.dropna(subset=['NAICS_CODES'],inplace=True)

frs_exp['NAICS_CODES'] = frs_exp['NAICS_CODES'].astype(int)


# get only industrial NAICS facilities
frs_ind = frs_exp[
    (frs_exp['NAICS_CODES']//10000).isin([11,21,23,31,32,33])].copy()


frs_ind.to_csv('EPA_REGISTRY_ID_ALL_INDUSTRY.csv',index=False)