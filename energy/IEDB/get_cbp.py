import requests
import pandas as pd
import io
import os
import zipfile
import re


class CBP:

    def __init__(self, year):

        self.data_dir = os.path.abspath('./data/CBP/')

        self.year = year

        def naics_table(year):
            """
            Get the relevant NAICS values for a CBP dataset.
            Check if NAICS codes have been downloaded to disk. Download
            from Census API if not.
            """

            base_html = 'https://api.census.gov/data/'
            naics_file = os.path.abspath('./energy/IEDB/naics_2012.csv')
            html = 'https://api.census.gov/data/2012/cbp/variables.json'

            r = requests.get(html)

            naics_df = pd.DataFrame.from_dict(
                r.json()['variables']['NAICS2012']['values']['item'],
                orient='index'
                )

            naics_df = naics_df[2:]
            naics_df.reset_index(inplace=True)
            naics_df.columns=['naics', 'desc']

            naics_df = pd.DataFrame(
                    naics_df[(naics_df.naics != '31-33') &
                             (naics_df.naics != '44-45') &
                             (naics_df.naics != '48-49')])

            naics_df['n_naics'] = naics_df.naics.apply(
                    lambda x: len(x)
                    )

            naics_df['naics'] = naics_df.naics.astype('int')

            naics_df = pd.DataFrame(
                    naics_df[naics_df.naics.between(1, 400000)]
                    )

            return naics_df

        self.naics_df = naics_table(self.year)
        self.naics_cbp = {}

        cbp_file = 'cbp' + str(self.year)[2:] + 'co'

        cbp_csv_url = \
            'https://www2.census.gov/programs-surveys/cbp/' + \
            'datasets/' + str(self.year) + '/' + cbp_file + '.zip'

        with zipfile.ZipFile(io.BytesIO(
                    requests.get(cbp_csv_url).content
                    )) as zip_cbp:

            cbp = pd.read_csv(zip_cbp.open(zip_cbp.namelist()[0]))

        # NAICS codes are a strings that include - and / characters
        def fix_naics(naics):
            """
            Regex to retain only digits in naics strings. Returns
            integer.
            """
            if re.match("\d+", naics) is None:
                naics = 0

            else:
                naics = int(re.findall("\d+", naics)[0])

            return naics
        
        cbp.columns = [x.lower() for x in cbp.columns]

        cbp.naics = cbp.naics.apply(fix_naics)

        # Create concatentated FIPS field to match GHGRP COUNTY_FIPS
        def state_fips_str(x):

            if len(str(x)) == 1:

                fips = '0' + str(x)

            else:
                fips = str(x)

            return fips

        def county_fips_str(x):

            int_len = len(str(x))

            mult = 3 - int_len

            fips = '0' * mult + str(x)

            return fips

        cbp['COUNTY_FIPS'] = \
            cbp.fipstate.apply(state_fips_str) + \
                cbp.fipscty.apply(county_fips_str)

        cbp['COUNTY_FIPS'] = cbp.COUNTY_FIPS.astype(int)

        census_regions = pd.read_csv(
                os.path.abspath('./energy/IEDB/US_FIPS_Codes.csv'),
                index_col=['COUNTY_FIPS'])

        cbp['region'] = cbp.fipstate.map(
            dict(census_regions[
                ['FIPS State', 'MECS_Region']
                ].values)
            )

        cbp.reset_index(drop=True, inplace=True)

        # Create employment size categories that match MECS
        cbp.loc[:, 'Under 50'] = cbp[
            ['n1_4', 'n5_9', 'n10_19', 'n20_49']
            ].sum(axis=1)

        self.cbp = cbp

        # Remaining lines of code further format cbp data into cbp_matching
        # for comparison against GHGRP facilities.
        self.cbp['naics_n'] = self.cbp.naics.apply(lambda x: len(str(x)))

        self.cbp['industry'] = \
            self.cbp.loc[self.cbp[self.cbp.naics != 0].index, 'naics'].apply(
                    lambda x: int(str(x)[0:2]) in [11, 21, 23, 31, 32, 33]
                    )

        self.cbp_matching = pd.DataFrame(
                self.cbp[(self.cbp.industry == True) &
                         (self.cbp.naics_n == 6)])

        self.cbp_matching['fips_matching'] = self.cbp_matching['COUNTY_FIPS']
#            self.cbp_matching.fipstate.astype(str) + \
#                self.cbp_matching.fipscty.astype(str)

        self.cbp_matching['fips_matching'] = \
            self.cbp_matching.fips_matching.astype(int)

        #Correct instances where CBP NAICS are wrong
        #Hancock County, WV has a large electroplaing and rolling facility
        #that shouldn't be classified as 331110/331111
        self.cbp_matching.drop(
            self.cbp_matching[(self.cbp_matching.fips_matching == 54029) &
                            (self.cbp_matching.naics == 331110)].index,
            inplace=True
            )


        #Create n1-49 column to match MECS reporting.
        self.cbp_matching['n1_49'] = self.cbp_matching['Under 50']

        self.cbp_matching['fips_n'] = [
            i for i in zip(self.cbp_matching.loc[:, 'fips_matching'], \
                self.cbp_matching.loc[:,'naics'])
            ]

        #Remove state-wide "999" county FIPS
        self.cbp_matching = pd.DataFrame(
            self.cbp_matching[self.cbp_matching.fipscty != 999]
            )

        self.cbp_matching.reset_index(drop=True, inplace=True)
