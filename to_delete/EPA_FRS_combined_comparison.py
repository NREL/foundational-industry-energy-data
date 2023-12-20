"""

FRS COMBILED download

"""

import pandas as pd


# file downloaded from FRS COMBINED ZIP
# https://www.epa.gov/frs/epa-state-combined-csv-download-files
frsnaics = pd.read_csv('NATIONAL_NAICS_FILE.csv')


# filter for industrial sector NAICS codes
frsnaics_ind = frsnaics[frsnaics['NAICS_CODE'].astype(str).str[:2].isin(
    ['11','21','23','31','32','33'])].copy()


# create NAICS subsector column
frsnaics_ind['NAICS_CODE_3'] = \
    frsnaics_ind['NAICS_CODE'].astype(str).str[:3].astype(int)
    
   
# there can be more than one NAICS code per REGISTRY_ID; drop duplicates
frsnaics_ind.drop_duplicates(subset=['REGISTRY_ID'],inplace=True)    
   
# get facility count by NAICS subsector    
frsnaics_ind.groupby('NAICS_CODE_3')['REGISTRY_ID'].nunique()