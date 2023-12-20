"""

Get Facility FRS IDs for NAICS manufacturing

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


# expand list of multiple NAICS per single facilitiy into new lines, same index
frs_exp = frs.explode('NAICS_CODES')

frs_exp['NAICS_CODES'] = frs_exp.NAICS_CODES.str.replace(" ","")

frs_exp.dropna(subset=['NAICS_CODES'],inplace=True)

frs_exp['NAICS_CODES'] = frs_exp['NAICS_CODES'].astype(int)


# get only manufacturing NAICS facilities
frs_mfg = frs_exp[(frs_exp.NAICS_CODES>=311000)&
                  (frs_exp.NAICS_CODES<=339999)].copy()

frs_mfg.to_csv('EPA_REGISTRY_ID_NAICS_MFG.csv')

