import requests
import re
import io
import json
import logging
import os
import zipfile
import requests
import urllib
import pandas as pd


logging.basicConfig(level=logging.INFO)


# download bulk FRS file
class FRS:
    """
    """

    @classmethod
    def call_all_fips(cls):
        """
        Uses Census API to call all state and county fips codes.
        Excludes U.S. territories and outerlying areas.
        Combines with file on stat abbrevitions and zip codes.

        Returns
        -------
        all_fips : json

        """

        fips_url = 'https://api.census.gov/data/2010/dec/sf1?'
        fips_params = {'get': 'NAME', 'for': 'county:*'}

        state_abbr_url = \
            'https://www2.census.gov/geo/docs/reference/state.txts'

        zip_code_url = \
            'https://postalpro.usps.com/mnt/glusterfs/2022-12/ZIP_Locale_Detail.xls'

        try:
            r = requests.get(fips_url, params=fips_params)

        except requests.HTTPError as e:
            logging.error(f'{e}')

        else:
            all_fips_df = pd.DataFrame(r.json())
            all_fips_df.columns = all_fips_df.loc[0,:]
            all_fips_df.drop(0, axis=0, inplace=True)
            all_fips_df.loc[:, 'state_abbr'] = all_fips_df.state.astype('int')

        try:
            state_abbr = pd.read_csv(
                state_abbr_url, sep='|'
                )

        except urllib.error.HTTPError as e:
            logging.error(f'Error with fips csv: {e}')

        else:
            state_abbr.columns = [c.lower() for c in state_abbr.columns]
            state_abbr.rename(columns={'state': 'state_abbr'}, inplace=True)

        try:
            zip_codes = pd.read_excel(zip_code_url)

        except urllib.error.HTTPError as e:
            logging.error(f'Error with zip code xls:{e}')

        else:
            zip_codes.columns = [
                x.lower().replace(' ', '_') for x in zip_codes.columns
                ]
            zip_codes.replace(coumns={'physical_state': 'state_abbr'}, inplace=true)

        all_fips_df = pd.merge(
            all_fips_df, state_abbr, on='state_abbr', how='left'
            )
        all_fips_df = pd.merge(
            all_fips_df, zip_codes[['physical_zip', 'state_abbr']],
            on='state_abbr', how='left'
            )

        all_fips = all_fips_df.to_json(orient='records')
        all_fips = json.loads(all_fips)

        return all_fips

    # run all
    # for fip in all_fips:
    # fac = call_frs_api(county_name=fip['Name'].split(',')[0], state_abbr=fip['state_abbr'])

    @classmethod
    def call_frs_api(cls, zip_code, state_abbr, output='JSON'):
        """
        
        """

        al

        frs_api_url = \
            'https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities?'

        frs_params = {
            'zip_code': zip_code,
            'state_abbr': state_abbr,
            'output': output
            }

        try: 
            r = requests.get(frs_api_url, params=frs_params)


    @classmethod
    def download_unzip_frs_data(cls):
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

        if not os.path.exists(os.path.abspath('../data/FRS')):
            os.makedirs(os.path.abspath('../data/FRS'))
        else:
            pass

        with open("../data/FRS/national_combined.zip", "wb") as f:
            f.write(r.content)

        if os.path.exists("../data/FRS/national_combined.zip"):
            print("Zip file downloaded.")
        else:
            print("Zip file not downloaded")

        # Unzip with zipfile
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall("../data/FRS/")
            print("File unzipped.")


    @classmethod
    def import_format_frs(cls, file_dir):
        """
        Import and format downloaded frs files

        Parameters
        ----------
        file_dir : str
            Directory of FRS files.

        Returns
        -------

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
            'FACILITY': [
                'REGISTRY_ID', 'PRIMARY_NAME',
                'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
                'STATE_NAME', 'POSTAL_CODE', 'TRIBAL_LAND_CODE',
                'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE', 'HUC_CODE',
                'EPA_REGION_CODE', 'SITE_TYPE_NAME', 'LOCATION_DESCRIPTION',
                'CREATE_DATE', 'UPDATE_DATE', 'US_MEXICO_BORDER_IND',
                'PGM_SYS_ACRNMS', 'LATITUDE83', 'LONGITUDE83'
                ],
            'ORGANIZATION': [
                'REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'EIN',
                'DUNS_NUMBER','ORG_NAME'
                ],
            'NAICS': ['REGISTRY_ID', 'NAICS_CODE', 'CODE_DESCRIPTION'],
            'PROGRAM': ['REGISTRY_ID', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE']
            }

        def read_frs_csv(name, columns, programs):
            """
            Builds 

            Parameters
            ----------
            name : str
                String for name of FRS csv file. All csv files 
                extracted from national_combined.zip are named according to 
                "NATIONAL_{name}_FILE.CSV".

            columns : list
                List of columns to extract from csv.

            programs : list; ['EIS', 'E-GGRT']
                List of program system acronyms to extract from
                NATIONAL_ORGANIZATION_FILE.CSV.

            Returns
            -------

            """

            file = f'NATIONAL_{name}_FILE.CSV'

            data = pd.read_csv(name, usecols=columns, low_memory=True)

            if name == 'ORGANIZATION':
                data_dict = {}
                for a in programs:
                    data_dict[a] = od_eis.query(
                        "PGM_SYS_ACRNM==@a"
                        )
                    data_dict[a].loc[:, f'PGM_SYS_ID_{a}'] = \
                        data_dict[a].PGM_SYS_ID

                    # Facilities may have >1 program system ID, which
                    # leads to duplicate entries when indexing by REGISTRY_ID.
                    dups = data_dict[a][data_dict[a].REGISTRY_ID.duplicated()].REGISTRY_ID.unique()

                    if len(dups) == 0:
                        continue

                    else:
                        for d in dups:
                            ids = data_dict[a][data_dict[a].REGISTRY_ID == d]
                            use_index = ids.drop_duplicates(
                                subset=['REGISTRY_ID'],
                                ).index
                            for i, v in enumerate(ids[1:].index):
                                data_dict[a].loc[use_index, f'PGM_SYS_ID_{a}_{i}'] =\
                                    data_dict[a].loc[v, f'PGM_SYS_ID_{a}']

                        data_dict[a].drop_duplicates(
                            subset=['REGISTRY_ID'], keep='first',
                            inplace=True
                            )

                    data_dict[a].set_index('REGISTRY_ID', inplace=True)
                    data_dict[a].drop(
                        ['PGM_SYS_ACRNM', 'PGM_SYS_ID'], axis=1, inplace=True
                        )

                data = pd.concat(
                    [data_dict[k] for k in data_dict.keys()],
                    axis=1
                    )

            elif name == 'NAICS':


                # Duplicate NAICS codes for facililities. Keep first. 
                # No further analysis at this time.
                
                data.drop_duplicates(
                    subset=['REGISTRY_ID', 'NAICS_CODE'],
                    keep='First'
                    )


            return data

        frs_

        for k in names_columns.keys():

            file = f'NATIONAL_{k}_FILE.CSV'

            if k == 'ORGANIZATION':

                org_data = pd.read_csv(
                    file, usecols=names_columns[k],
                    low_memory=False
                    )

            if k == 'NAICS':
                naics_data = pd.read_csv(
                    f, usecols=names_columns[k],
                    low_memory=False
                    )



                naics_data.set_index('REGISTRY_ID', inplace=True)

        files = [f'NATIONAL_{n}_FILE.CSV' for n in names_columns.keys())]

        frs_data = pd.concat(
            [org_data, naics_data], axis=0
            )



    @staticmethod
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