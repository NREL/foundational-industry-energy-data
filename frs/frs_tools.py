import requests
import re
import io
import json
import gzip
import logging
import os
import zipfile
import requests
import urllib
import time
import itertools
import pandas as pd
from collections import OrderedDict


logging.basicConfig(level=logging.INFO)


# download bulk FRS file
class FRS:
    """
    """
    def __init__(self):

        self._frs_data_path = '../data/FRS'

        self._names_columns = OrderedDict({
            'FACILITY': [
                'REGISTRY_ID', 'PRIMARY_NAME', 'LOCATION_ADDRESS',
                'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
                'STATE_NAME', 'POSTAL_CODE', 'TRIBAL_LAND_CODE',
                'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE', 'HUC_CODE',
                'EPA_REGION_CODE', 'SITE_TYPE_NAME', 'LOCATION_DESCRIPTION',
                'LATITUDE83', 'LONGITUDE83'
                ],
            'NAICS': ['REGISTRY_ID', 'NAICS_CODE'],
            'PROGRAM': ['REGISTRY_ID', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE',
                        'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'SENSITIVE_IND']
            # 'ORGANIZATION': [
            #     'REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'EIN',
            #     'DUNS_NUMBER', 'ORG_NAME'
            #     ],
            })


        self._json_format = OrderedDict({
            'site': [
                'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
                'POSTAL_CODE', 'TRIBAL_LAND_CODE',
                'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE', 'HUC_CODE',
                'EPA_REGION_CODE', 'LOCATION_DESCRIPTION'
                ],
            'facility': [
                'PRIMARY_NAME', 'LATITUDE83', 'LONGITUDE83',
                'LOCATION_ADDRESS', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE',
                'NAICS_CODE_additional', 'NAICS_CODE', 'SITE_TYPE_NAME',
                'SENSITIVE_IND'
                ]
            })

    def call_all_fips(self):
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
            zip_codes.replace(coumns={'physical_state': 'state_abbr'},
                              inplace=true)

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


    # def call_frs_api(self, zip_code, state_abbr, output='JSON'):
    #     """
        
    #     """

    #     al

    #     frs_api_url = \
    #         'https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities?'

    #     frs_params = {
    #         'zip_code': zip_code,
    #         'state_abbr': state_abbr,
    #         'output': output
    #         }

    #     try: 
    #         r = requests.get(frs_api_url, params=frs_params)

    def download_unzip_frs_data(self):
        """
        Download bulk data file from EPA.
        """

        # This file is ~732 MB as of December 2022
        frs_url = \
            "https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip"

        zip_path = os.path.abspath(
            os.path.join(self._frs_data_path, "national_combined.zip")
            )

        if not os.path.exists(os.path.abspath(self._frs_data_path)):
            os.makedirs(os.path.abspath(self._frs_data_path))
        else:
            pass

        if os.path.exists(zip_path):
            logging.info("Zip file exists.")

        else:
            logging.info("Zip file does not exist. Downloading...")

            r = requests.get(frs_url)

            try:
                r.raise_for_status()

            except requests.exceptions.HTTPError as e:
                logging.error(f'{e}')

            with open(zip_path, "wb") as f:
                f.write(r.content)

        # Unzip with zipfile
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.extractall(os.path.abspath(self._frs_data_path))
            logging.info("File unzipped.")

    def test_naics(naics_code):

        n2d = str(naics_code)[0:2]
        try:
            n2d = int(n2d)

        except ValueError:
            print(naics_code, n2d)

    @staticmethod
    def fix_code(code):
        """
        Fix codes that should be int, not float or str
        """
        try:
            code_fixed = int(code)

        except ValueError:
            return code

        else:
            return code_fixed


    def read_frs_csv(self, name, columns, programs=['EIS', 'E-GGRT']):
        """
        Builds dataframe based on FRS datasets.

        Parameters
        ----------
        name : str
            String for name of FRS csv file. All csv files
            extracted from national_combined.zip are named according
            to "NATIONAL_{name}_FILE.CSV".

        columns : list
            List of columns to extract from csv.

        programs : list; ['EIS', 'E-GGRT']
            List of program system acronyms to extract from
            NATIONAL_ORGANIZATION_FILE.CSV.

        Returns
        -------
        data : pandas.DataFrame
            Formatted FRS data, based on FACILITY, ORGANIZATION,
            NAICS, and PROGRAM datasets.
        """

        file = f'NATIONAL_{name}_FILE.CSV'
        file_path = os.path.abspath(os.path.join(self._frs_data_path, file))

        try:
            data = pd.read_csv(
                file_path,
                usecols=columns, low_memory=False
                )

        # Error in the 'FACILITY' file.
        except pd.errors.ParserError as e:
            logging.error(f'{e}\n Due to {file_path}')

            try:
                skiprow = int(re.search(r'(?<=row )(\d+)', str(e)).group())

            except AttributeError as e2:
                logging.error(f'{e2}. Something else happening')

            else:
                data = pd.read_csv(
                    file_path,
                    usecols=columns, low_memory=False,
                    skiprows=[skiprow]
                    )

        if name == 'PROGRAM':
            data_dict = {}
            for a in programs:
                data_dict[a] = data.query("PGM_SYS_ACRNM==@a")
                data_dict[a].loc[:, f'PGM_SYS_ID_{a}'] = \
                    data_dict[a].PGM_SYS_ID

                # Facilities may have >1 program system ID, which
                # leads to duplicate entries when indexing by REGISTRY_ID.
                dups = data_dict[a][data_dict[a].REGISTRY_ID.duplicated()].REGISTRY_ID.unique()

                if len(dups) == 0:
                    continue

                else:
                    data_dict[a].loc[:, f'PGM_SYS_ID_{a}_additional'] = None

                    for d in dups:
                        ids = data_dict[a][data_dict[a].REGISTRY_ID == d]
                        use_index = ids.drop_duplicates(
                            subset=['REGISTRY_ID'], keep='first'
                            ).index
                        # logging.info(f'registry ID: {d}\nLength of IDs: {len(ids)}')
                        # for i, v in enumerate(ids[1:].index):
                        data_dict[a].at[use_index, f'PGM_SYS_ID_{a}_additional'] =\
                                ', '.join(ids[1:][f'PGM_SYS_ID_{a}'].to_list())
                                # data_dict[a].loc[v, f'PGM_SYS_ID_{a}']

                    data_dict[a].drop_duplicates(
                        subset=['REGISTRY_ID'], keep='first',
                        inplace=True
                        )

                data_dict[a].set_index('REGISTRY_ID', inplace=True)
                data_dict[a].drop(
                    ['PGM_SYS_ACRNM', 'PGM_SYS_ID'], axis=1, inplace=True
                    )

                data_dict[a].replace({'N': False, 'Y': True}, inplace=True)

            data = pd.concat(
                [data_dict[k] for k in data_dict.keys()],
                axis=1
                )

            data.reset_index(inplace=True)

        elif name == 'NAICS':

            # Duplicate NAICS codes for facililities. Keep first
            # and move remaining to NAICS_CODE_additional.
            # Assume that a facility with any industry NAICS
            # code (i.e., 11, 21, 23, 31-33) is 
            # an industrial facility

            all_naics = pd.DataFrame(
                data.NAICS_CODE.unique(), columns=['NAICS_CODE']
                )
            all_naics.loc[:, 'ind'] = all_naics.NAICS_CODE.apply(
                lambda x: int(str(x)[0:2]) in [11, 21, 23, 31, 32, 33]
                )

            data = pd.merge(data, all_naics, on='NAICS_CODE', how='left')
            data = data.query("ind==True")

            data_unique = data.drop_duplicates(
                subset=['REGISTRY_ID'], keep='first'
                )

            dups = data[~data.index.isin(data_unique.index)]
            dups = dups.groupby('REGISTRY_ID').apply(
                lambda x: x['NAICS_CODE'].values
                )
            dups.name = 'NAICS_CODE_additional'

            data = pd.merge(
                data_unique, dups, on='REGISTRY_ID',
                how='left'
                )

            data.drop(['ind'], axis=1, inplace=True)


            # # Keep only industry NAICS
            # ind_naics = pd.DataFrame(
            #     data.NAICS_CODE.unique(), columns=['NAICS_CODE']
            #     )

            # ind_naics = pd.DataFrame(
            #     ind_naics[
            #         ind_naics.apply(
            #             lambda x: int(
            #                 str(x['NAICS_CODE'])[0:2]
            #                 ) in [11, 21, 23, 31, 32, 33],
            #             axis=1
            #             )
            #         ]
            #     )

            # data = pd.merge(
            #     data, ind_naics, on='NAICS_CODE',
            #     how='inner'
            #     )

        # elif name == 'PROGRAM':

        #     data_unique = data.drop_duplicates(
        #         subset=['REGISTRY_ID'], keep='first'
        #         )
        #     data_unique.set_index('REGISTRY_ID', inplace=True)

        #     for c in ['SMALL_BUS_IND', 'ENV_JUSTICE_CODE']:
        #         c_data = data.dropna(subset=[c]).drop_duplicates(
        #             subset=['REGISTRY_ID']
        #             )
        #         c_data.set_index('REGISTRY_ID', inplace=True)
        #         data_unique.update(c_data)

        #     data_unique.replace({'N': False, 'Y': True}, inplace=True)

        #     data = data_unique.reset_index()

        elif name == 'FACILITY':
            pass

        return data

    def build_frs_json(self, frs_data_df, save_path=None, ret=False):
        """

        Parameters
        ----------
        frs_data_df : pandas.DataFrame
            Dataframe from formatted FRS csv datasets.

        ret : bool; default == False
            Returns FRS data in json format.

        save_path : str; default == None
            Directory to save FRS data in json file.
            Must specify to save.

        Returns
        -------
        """


        # for i in frs_data_df.index:
        #     for k in self._json_format.keys():
        #         if frs_data_df.at[i, 'index'] in self._json_format[k]:
        #             frs_data_df.at[i, 'cat'] = k
        #         else:
        #             continue
        
        # frs_dict = dict.fromkeys(
        #     frs_data_df.index.values,
        #     [dict.fromkeys([x]) for x in self._json_format.keys()]
        #     )
        """""
        test_dict = {'Gfg' : 4, 'is' : 5, 'best' : 9} 
        test_list = [8, 3, 2]
        res = {idx : {key: test_dict[key]} for idx, key in zip(test_list, test_dict)}
        """""

        # Fix formatting
        fix_codes = ['NAICS_CODE', 'POSTAL_CODE', 'CONGRESSIONAL_DIST_NUM',
                     'CENSUS_BLOCK_CODE', 'HUC_CODE', 'EPA_REGION_CODE',
                    ]

        for code in fix_codes:
            frs_data_df.loc[:, code] = frs_data_df[code].apply(
                lambda x: FRS.fix_code(x)
                )

        # Version to use
        # Must first transpose DF
        frs_data_df = frs_data_df.T
        frs_data_df.index.name = 'VARIABLE'
        frs_data_df.reset_index(inplace=True)
        frs_data_df.loc[:, 'CATEGORY'] = None
        for i in frs_data_df.index:
            for cat, v in self._json_format.items():
                if frs_data_df.at[i, 'VARIABLE'] in v:
                    frs_data_df.at[i, 'CATEGORY'] = cat
                else:
                    continue
        
        frs_data_df.set_index(['CATEGORY', 'VARIABLE'], inplace=True)
    
        val_dict = \
            {k: frs_data_df.xs(k).to_dict() for k in self._json_format.keys()}  # nested dict, e.g., {'site' : {1000: {NAICS_CODE: 2111}}}

        frs_dict = dict.fromkeys(
            frs_data_df.columns, [dict.fromkeys(list(val_dict.keys()))]
            )
    
        for facid in frs_dict.keys():
            for cat in val_dict.keys():
                frs_dict[facid][0][cat] = val_dict[cat][facid]

        # frs_dict = {fid : [{cat : val_dict[cat][fid]}] for fid, cat in itertools.product(
        #     frs_data_df.columns, _json_format.keys()
        #     )}

        # frs_dict = dict.fromkeys(
        #     test.columns,
        #     [dict.fromkeys([k for k in _json_format.keys()],
        #         [l_frs.xs(k).T.to_dict(orient='records') for k in _json_format.keys()]
        #         )]
        #     )

        # for i in frs_dict.keys():
        #     for j, k in enumerate(self._json_format.keys()):
        #         frs_dict[i][j] = frs_data_df.loc[i, self._json_format[k]].to_dict() 

        if save_path:
            with gzip.open(os.path.join(save_path, 'found_ind_data.json.gz'), 'wt', encoding="ascii") as f:
                json.dump(frs_dict, f)

        else:
            pass

        if ret:
            return json.dump(frs_dict)

        else:
            pass

    def add_frs_columns_json(self, frs_data_df):
        """
        Add columns that capture multiple program IDs.
        """

        logging.info(f'Starting fields:{self._json_format["facility"]}')

        for c in frs_data_df.columns:
            if 'PGM_SYS_ID' in c:
                self._json_format['facility'].append(c)
            else:
                continue

        logging.info(f'Ending fields:{self._json_format["facility"]}')

        return

    def import_format_frs(self):
        """
        Import and format downloaded frs files

        Parameters
        ----------
        file_dir : str
            Directory of FRS files.

        Returns
        -------
        frs_data_df : pandas.DataFrame

        """

        # Reminder that self._names_columns is an ordered dict
        for i, v in enumerate(self._names_columns.items()):
            if i == 0:
                frs_data_df = self.read_frs_csv(name=v[0], columns=v[1])
                frs_data_df.set_index('REGISTRY_ID', inplace=True)
                logging.info(f'File name: {v[0]}\nDF len: {len(frs_data_df)}')

            else:
                data = self.read_frs_csv(name=v[0], columns=v[1])
                data.set_index('REGISTRY_ID', inplace=True)
                logging.info(f'File name: {v[0]}\nDF len: {len(data)}')

                frs_data_df = pd.merge(
                    frs_data_df, data, left_index=True,
                    right_index=True, how='left'
                    )

        # All dataframes but the NAICS dataframe have non-industrial
        # facilities. Drop facilities that don't have NAICS codes after
        # merging.
        frs_data_df.dropna(subset=['NAICS_CODE'], inplace=True)

        return frs_data_df

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


if __name__ == '__main__':

    t_start = time.perf_counter()
    frs_methods = FRS()
    frs_methods.download_unzip_frs_data()

    frs_data_df = frs_methods.import_format_frs()
    frs_data_df.to_pickle('frs_data_df_revised2.pkl')

    frs_methods.add_frs_columns_json(frs_data_df)

    frs_methods.build_frs_json(frs_data_df, save_path='../')
    t_stop = time.perf_counter()
    logging.info(f'Program time: {t_stop - t_start:0.2f} seconds')
