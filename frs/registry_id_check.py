

import pandas as pd
import os
import logging
import zipfile
from itertools import combinations


logging.basicConfig(level=logging.INFO)

# Examine which FRS registry IDs
# are used by which FRS tables.

# Individual tables from combined CSV 
# files (https://www.epa.gov/frs/epa-state-combined-csv-download-files)
file_paths = {
    'pgm': os.path.abspath('./data/FRS/NATIONAL_PROGRAM_FILE.csv'),  # FRS_PROGRAM_FACILITY table in FRS data model
    'fac': os.path.abspath('./data/FRS/NATIONAL_FACILITY_FILE.csv'),  # FRS_FACILITY_SIDE table in FRS data model
    'natsing': os.path.join(os.environ['HOME'], 'Desktop', 'national_single.zip')  # Compiled data
    }

data_dict = {}

for k, v in file_paths.items():
    if k == 'natsing':
        with zipfile.ZipFile(v) as zf:
            with zf.open(zf.namelist()[0]) as f:
                data_dict[k] = pd.read_csv(f, low_memory=False)
    elif k == 'fac':
        data_dict[k] = pd.read_csv(v, low_memory=False, skiprows=[652323])
    else:
        data_dict[k] = pd.read_csv(v, low_memory=False)

# how many unique REGISTRY IDs in each data set?
for k, v in data_dict.items():
    logging.info(f'{k} has {len(v.REGISTRY_ID.unique())} unique REGISTRY_IDs')

# Which REGISTRY ISs are unique to each data set
for c in combinations(data_dict, 2):
    combo = pd.merge(
        data_dict[c[0]].drop_duplicates(subset=['REGISTRY_ID']),
        data_dict[c[1]].drop_duplicates(subset=['REGISTRY_ID']),
        on='REGISTRY_ID', how='left', indicator=True
        )

    left_count = len(combo.query('_merge == "left_only"'))
    right_count = len(combo.query('_merge == "right_only"'))
    both_count = len(combo.query('_merge == "both"'))

    logging.info(
        f'{left_count} are only in {c[0]}\n'
        f'{right_count} are only in {c[1]}\n'
        f'{both_count} are in both {c[0]} and {c[1]}'
    )
