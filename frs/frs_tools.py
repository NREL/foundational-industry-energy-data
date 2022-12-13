import requests
import re
import io
import logging
import os
import zipfile
import pandas as pd


logging.basicConfig(level=logging.INFO)


# download bulk FRS file

def download_unzip_frs_data():
    """
    Download bulk data file from EPA.
    """

    # This file is ~732 MB as of December 2022
    frs_url = "https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip"

    r = requests.get(frs_url)

    try:
        r.raise_for_status()

    except requests.exceptions.HTTPError as e:

        logging.error(f'{e}')

    if not os.path.exists(os.path.abspath('./Data/FRS')):
        os.makedirs(os.path.abspath('./Data/FRS'))
    else:
        pass

    with open("./Data/FRS/national_combined.zip", "wb") as f:
        f.write(r.content)

    if os.path.exists("./Data/FRS/national_combined.zip"):
        print("Zip file downloaded.")
    else:
        print("Zip file not downloaded")

    # Unzip with zipfile
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
         zf.extractall("./Data/FRS/")
         print("File unzipped.")


def import_format_frs(file_dir):
    """
    Import and format downloaded frs files
    """

    # NATIONAL_FACILITY_FILE: keep 'REGISTRY_ID', 'PRIMARY_NAME',
    # 'LOCATION_ADDRESS', 'CITY_NAME', 'COUNTY_NAME',
    #    'FIPS_CODE', 'STATE_CODE', 'STATE_NAME', 'POSTAL_CODE',
    #    'TRIBAL_LAND_CODE', 'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE',
    #    'HUC_CODE', 'EPA_REGION_CODE', 'SITE_TYPE_NAME', 'LOCATION_DESCRIPTION',
    #    'CREATE_DATE', 'UPDATE_DATE', 'US_MEXICO_BORDER_IND', 'PGM_SYS_ACRNMS',
    #    'LATITUDE83', 'LONGITUDE83'
    # Merge in these files with the specified info, using REGISTRY_ID:
    # ORGANIZATION FILE: PGM_SYS_ACRNM, PGM_SYS_ID, EIN, DUNS_NUMBER, ORG_NAME
    # NAICS: NAICS_CODE (need to deal with dups)
    # PROGRAM: SMALL_BUS_IND, ENV_JUSTICE_CODE

    names_columns = {
        'NATIONAL_FACILITY': [
            'REGISTRY_ID', 'PRIMARY_NAME',
            'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
            'STATE_NAME', 'POSTAL_CODE', 'TRIBAL_LAND_CODE',
            'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE', 'HUC_CODE',
            'EPA_REGION_CODE', 'SITE_TYPE_NAME', 'LOCATION_DESCRIPTION',
            'CREATE_DATE', 'UPDATE_DATE', 'US_MEXICO_BORDER_IND',
            'PGM_SYS_ACRNMS', 'LATITUDE83', 'LONGITUDE83'
            ],
        'ORGANIZATION': [
            'REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'EIN', 'DUNS_NUMBER',
            'ORG_NAME'
            ],
        'NAICS': ['REGISTRY_ID', 'NAICS_CODE', 'CODE_DESCRIPTION'],
        'PROGRAM': ['REGISTRY_ID', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE']
        }

    for k in names_columns.keys():

        file = f'NATIONAL_{k}_FILE.CSV'

        if k == 'ORGANIZATION':

            org_data = pd.read_csv(file, usecols=names_columns[k],
                                   low_memory=False)

            org_data = pd.concat(
                [org_data.query('PGM_SYS_ACRNM=="EIS"').set_index(
                    'REGISTRY_ID'
                    )['PRM_SYS_ID'],
                 ]
            )

        if k == 'NAICS':
            naics_data = pd.read_csv(f, usecols=names_columns[k],
                                     low_memory=False)

            # Duplicate NAICS codes for facililities. Keep first. 
            # No further analysis at this time.
            naics_data.drop_duplicates(subset=['REGISTRY_ID', 'NAICS_CODE'],
                                       keep='First')

            naics_data.set_index('REGISTRY_ID', inplace=True)

    files = [f'NATIONAL_{n}_FILE.CSV' for n in names_columns.keys()]

    org_data = pd.read_csv()

    frs_data = pd.concat(
        [pd.read_csv()]
    )


    
def find_eis(acrnm):
    """
    Pull out EIS ID from program system field

    Parameters
    ----------
    acrnm : str
        String of program system names and IDs

    Returns
    -------
    eis : str or None
        Returns string if EIS in program system field; None if not.
    """

    eis = re.search(r'(?<=EIS:)\w+', acrnm)

    try:
        eis = eis.group()

    except AttributeError:
        eis = None

    return eis


def remove_dups(frs_data):
    """
    FRS data file contains duplicate FRS IDs due to assignment of multiple NAICS codes.
    A company may also be listed multiple times, with different FRS IDs due to different 
    state-local-tribal program IDs (examples include construction companies). These FRS 
    IDs also have different coordinates. 
   
    """