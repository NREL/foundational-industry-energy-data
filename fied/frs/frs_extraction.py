import requests
import re
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
from pathlib import Path
from naics_selection import NAICS_Identification


logging.basicConfig(level=logging.INFO)


class FRS:
    """
    Class for extracting relevant facility-level
    data from EPA's Facility Registration Service (FRS) data.

    Documentation of FRS data fields:
    https://www.epa.gov/sites/default/files/2015-09/documents/frs_data_dictionary.pdf
    """

    def __init__(self):

        self._FIEDPATH = Path(__file__).parents[1]

        self._frs_data_path = Path(self._FIEDPATH, "data/FRS")

        # self._frs_data_path = os.path.abspath('./data/FRS')

        # Names of relevant FRS data files and columns.
        self._names_columns = OrderedDict({
            'PROGRAM': ['REGISTRY_ID', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE',
                        'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'SENSITIVE_IND',
                        'STD_NAME', 'STD_LOC_ADDRESS',
                        'STD_COUNTY_FIPS', 'STD_CITY_NAME', 'STD_COUNTY_NAME',
                        'STD_STATE_CODE', 'STD_POSTAL_CODE',
                        'LEGISLATIVE_DIST_NUM', 'HUC_CODE_8',
                        'SITE_TYPE_NAME'],
            'FACILITY': [
                'REGISTRY_ID',
                'EPA_REGION_CODE',
                'LATITUDE83', 'LONGITUDE83'
                ],
            'NAICS': ['REGISTRY_ID', 'NAICS_CODE', 'PGM_SYS_ACRNM'],
            'single': ['REGISTRY_ID', 'PRIMARY_NAME',
                       'LOCATION_ADDRESS', 'SUPPLEMENTAL_LOCATION',
                       'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
                       'STATE_NAME', 'POSTAL_CODE',
                       'TRIBAL_LAND_CODE', 'CONGRESSIONAL_DIST_NUM',
                       'CENSUS_BLOCK_CODE',
                       'HUC_CODE', 'SITE_TYPE_NAME',
                       'US_MEXICO_BORDER_IND',
                       'LATITUDE83', 'LONGITUDE83']
            # 'PROGRAM': ['REGISTRY_ID', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE',
            #             'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'SENSITIVE_IND']
            # 'ORGANIZATION': [
            #     'REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID', 'EIN',
            #     'DUNS_NUMBER', 'ORG_NAME', ORG_TYPE
            #     ],  #TODO CSV contains useful data, but many ORG_NAME, etc. for a given RegistryID 
            })

        # Dictionary of relevant data categories (keys) and variables (values)
        self._json_format = OrderedDict({
            'site': [
                'CITY_NAME', 'COUNTY_NAME', 'FIPS_CODE', 'STATE_CODE',
                'POSTAL_CODE', 'TRIBAL_LAND_CODE',
                'CONGRESSIONAL_DIST_NUM', 'CENSUS_BLOCK_CODE', 'HUC_CODE',
                'EPA_REGION_CODE'
                ],
            'facility': [
                'PRIMARY_NAME', 'LATITUDE83', 'LONGITUDE83',
                'LOCATION_ADDRESS', 'SMALL_BUS_IND', 'ENV_JUSTICE_CODE',
                'NAICS_CODE_additional', 'NAICS_CODE', 'SITE_TYPE_NAME',
                'SENSITIVE_IND', 'ENERGY_EST_SOURCE'
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

    def download_unzip_frs_data(self, combined=True):
        """
        Download bulk FRS data files from EPA.
        """

        if combined:
            name = 'combined'
        else:
            name = 'single'

        # Combined file is ~732 MB as of December 2022
        frs_url = \
            f"https://ordsext.epa.gov/FLA/www3/state_files/national_{name}.zip"

        zip_path = os.path.abspath(
            os.path.join(self._frs_data_path, f"national_{name}.zip")
            )

        if not os.path.exists(os.path.abspath(self._frs_data_path)):
            os.makedirs(os.path.abspath(self._frs_data_path))
        else:
            pass

        if os.path.exists(zip_path):
            logging.info(f"FRS {name.capitalize()} zip file exists.")

        else:
            logging.info(f"FRS {name.capitalize()} zip file does not exist. Downloading...")

            r = requests.get(frs_url)

            try:
                r.raise_for_status()

            except requests.exceptions.HTTPError as e:
                logging.error(f'{e}')

            with open(zip_path, "wb") as f:
                f.write(r.content)

        # Unzip with zipfile
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(os.path.abspath(self._frs_data_path))
            logging.info(f"FRS {name.capitalize()} file unzipped.")

        return

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

    def format_program_csv(self, data, programs):
        """
        Builds dataframe from FRS_PROGRAM dataset.

        Parameters
        ----------
        data : pandas.DataFrame
            Initial imported DataFrame.

        programs : list
            List of program system acronyms to extract.

        Returns
        -------
        data : pandas.DataFrame
            Formatted FRS data
        """

        data_dict = {}
        for a in programs:
            data_dict[a] = pd.DataFrame(data.query("PGM_SYS_ACRNM==@a")[
                ['REGISTRY_ID', 'PGM_SYS_ID', 'PGM_SYS_ACRNM']
                ])
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

        data.drop(['PGM_SYS_ACRNM', 'PGM_SYS_ID'], axis=1, inplace=True)
        data.drop_duplicates(subset=['REGISTRY_ID'], inplace=True)
        data.replace({'N': False, 'Y': True}, inplace=True)
        data.set_index('REGISTRY_ID', inplace=True)

        pgm_data = pd.concat(
            [data_dict[k] for k in data_dict.keys()],
            axis=1
            )

        data = data.join(pgm_data)

        # pgm_data = pgm_data.reindex(index=data.index)
        # pgm_data.update(data)

        # data = pd.DataFrame(pgm_data)
        data.reset_index(inplace=True)

        return data

    def format_naics_csv(self, data):
        """
        Builds dataframe from FRS_FACILITY dataset.

        Parameters
        ----------
        data : pandas.DataFrame
            Initial imported DataFrame.

        Returns
        -------
        data : pandas.DataFrame
            Formatted FRS data
        """
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

        data.drop(['ind'], axis=1, inplace=True)
    
        data = NAICS_Identification().assign_all_naics(data)

        data['NAICS_CODE'] = data.NAICS_CODE.astype(int)

        data.to_csv("data_naics_check_next.csv")

        return data

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
            NATIONAL_PROGRAM_FILE.CSV.

        Returns
        -------
        data : pandas.DataFrame
            Formatted FRS data, based on FACILITY, ORGANIZATION,
            NAICS, and PROGRAM datasets.
        """

        if name == 'single':
            file = f'NATIONAL_{name.upper()}.CSV'

        else:
            file = f'NATIONAL_{name}_FILE.CSV'

        file_path = os.path.abspath(os.path.join(self._frs_data_path, file))

        data = pd.read_csv(
            file_path,
            usecols=columns, low_memory=False
            )

        if name == 'PROGRAM':
            data = self.format_program_csv(data, programs)

        elif name == 'NAICS':
            data = self.format_naics_csv(data)

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
        frs_json : json, optional.
            Dictionary of facility data extracted from FRS in
            JSON format.
        """

        # Fix formatting
        fix_codes = ['NAICS_CODE', 'POSTAL_CODE', 'CONGRESSIONAL_DIST_NUM',
                     'CENSUS_BLOCK_CODE', 'HUC_CODE', 'EPA_REGION_CODE',
                     ]

        for code in fix_codes:
            frs_data_df.loc[:, code] = frs_data_df[code].apply(
                lambda x: FRS.fix_code(x)
                )

        if frs_data_df.index.name == 'REGISTRY_ID':
            pass

        else:
            frs_data_df.set_index('REGISTRY_ID', inplace=True)

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

        # Previous approach used dict.fromkeys, which didn't work for setting values from
        # val_dict
        frs_dict = {
            k: [{c: v[k]} for c, v in val_dict.items()] for k in frs_data_df.columns
            }

        if save_path:
            with gzip.open(os.path.join(save_path, 'found_ind_data.json.gz'),
                           'wt', encoding="ascii") as f:
                json.dump(frs_dict, f, sort_keys=True, indent=4)

        else:
            pass

        if ret:
            frs_json = json.dump(frs_dict)

            return frs_json

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

    def import_format_frs(self, combined=True):
        """
        Import and format downloaded frs files

        Parameters
        ----------
        file_dir : str
            Directory of FRS files.

        combined : bool; default is True
            Indicate whether the data set is constructed using
            the EPA FRS single file or combined files.

        Returns
        -------
        final_data : pandas.DataFrame
            DataFrame indexed by REGISTRY_ID, containing
            relevant site and facility data from EPA FRS. 
        """

        # Reminder that self._names_columns is an ordered dict
        pgm_data = self.read_frs_csv(
            name='PROGRAM', columns=self._names_columns['PROGRAM']
            )

        naics_data = self.read_frs_csv(
            name='NAICS', columns=self._names_columns['NAICS']
            )

        if combined:
            fac_data = self.read_frs_csv(
                name='FACILITY', columns=self._names_columns['FACILITY']
                )

            final_data = pd.merge(
                pgm_data, fac_data, on='REGISTRY_ID',
                how='right'
                )

        else:
            fac_data = self.read_frs_csv(
                name='single', columns=self._names_columns['single']
                )

            final_data = pd.merge(
                fac_data, pgm_data, on='REGISTRY_ID',
                how='left'
                )

        final_data = pd.merge(
            final_data, naics_data,
            on='REGISTRY_ID',
            how='left'
            )

        # All dataframes but the NAICS dataframe have non-industrial
        # facilities. Drop facilities that don't have NAICS codes after
        # merging.
        final_data.dropna(subset=['NAICS_CODE'], inplace=True)

        final_data.rename(columns={
            'REGISTRY_ID': 'registryID',
            'LEGISLATIVE_DIST_NUM': 'legislativeDistrictNumber',
            'HUC_CODE_8': 'hucCode8',
            'SITE_TYPE_NAME': 'siteTypeName',
            'STD_NAME': 'name',
            'STD_LOC_ADDRESS': 'locationAddress',
            'STD_POSTAL_CODE': 'postalCode',
            'STD_CITY_NAME': 'cityName',
            'STD_COUNTY_NAME': 'countyName',
            'STD_STATE_CODE': 'stateCode',
            'STD_COUNTY_FIPS': 'countyFIPS',
            'SENSITIVE_IND': 'sensitiveInd',
            'SMALL_BUS_IND': 'smallBusInd',
            'ENV_JUSTICE_CODE': 'envJusticeCode',
            'PGM_SYS_ID_EIS': 'eisFacilityID',
            'PGM_SYS_ID_EIS_additional': 'eisFacilityIDAdditional',
            'PGM_SYS_ID_E-GGRT': 'ghgrpID',
            'PGM_SYS_ID_E-GGRT_additional': 'ghgrpIDAdditional',
            'EPA_REGION_CODE': 'epaRegionCode',
            'LATITUDE83': 'latitude',
            'LONGITUDE83': 'longitude',
            'NAICS_CODE': 'naicsCode',
            'NAICS_CODE_additional': 'naicsCodeAdditional'
            }, inplace=True)


        # for i, v in enumerate(self._names_columns.items()):
        #     if i == 0:
        #         frs_data_df = self.read_frs_csv(name=v[0], columns=v[1])
        #         frs_data_df.set_index('REGISTRY_ID', inplace=True)
        #         logging.info(f'File name: {v[0]}\nDF len: {len(frs_data_df)}')

        #     elif i < 4:
        #         data = self.read_frs_csv(name=v[0], columns=v[1])
        #         data.set_index('REGISTRY_ID', inplace=True)
        #         logging.info(f'File name: {v[0]}\nDF len: {len(data)}')

        #         frs_data_df = pd.merge(
        #             frs_data_df, data, left_index=True,
        #             right_index=True, how='left'
        #             )
        #         logging.info(f'File len: {len(frs_data_df)}')

        #     else:
        #         continue


        logging.info(f'Final len: {len(final_data)}')

        final_data.set_index('registryID', inplace=True)
        return final_data

    # TODO 
    @staticmethod
    def load_foundational_json(found_json_file):
        """
        Load json file of foundational energy data.
    
        """

        with gzip.open(found_json_file, mode='rb') as gzfile:
            # with json.load(gfile) as jfile:
            json_data = pd.DataFrame.from_dict(json.load(gzfile),
                                               orient='index')

        frs_data = pd.DataFrame(index=json_data.index)

        for c in json_data.columns:

            column_data = pd.concat(
                [pd.DataFrame.from_dict(json_data.iloc[i, c]).T for i in range(0, len(json_data))],
                axis=0
                )

            column_data.index = frs_data.index

            frs_data = pd.concat(
                [frs_data, column_data], axis=1
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


if __name__ == '__main__':
    # t_start = time.perf_counter()
    combined = True

    frs_methods = FRS()
    frs_methods.download_unzip_frs_data(combined=combined)

    frs_data_df = frs_methods.import_format_frs(combined=combined)
    frs_data_df.to_csv(Path(Path(__file__).parents[1], 'data/FRS/frs_data_formatted.csv'))

    # t_stop = time.perf_counter()
    # logging.info(f'Program time: {t_stop - t_start:0.2f} seconds')
