
import requests
import pandas as pd
import json
import logging
import os
import re
import zipfile
from io import BytesIO

class Ag:

    def __init__(self, usda_api_key, eia_api_key):
        logging.basicConfig(level=logging.INFO)

        # MMBtu/barrel
        # https://www.eia.gov/totalenergy/data/monthly/pdf/sec13.pdf
        self._heat_content = {
            'diesel': 5.772,
            'gasoline': 5.053,
            'LPG': 3.836,
            'OTHER': 6.287
            }

        self._usda_key = usda_api_key
        self._eia_key = eia_api_key
        self._census_year = 2017
        self._survey_year = 2017

        self._region_file = pd.read_csv(
            os.path.abspath('energy/IEDB/input_region.csv')
            )

        self._data_fields = {
            'fuels': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'STATE',
                'short_desc': 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $',
                'domain_desc': 'NAICS CLASSIFICATION',
                'year': self._census_year
                },
            'electricity': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'STATE',
                'short_desc': 'AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $',
                'domain_desc': 'NAICS CLASSIFICATION',
                'year': self._census_year
                },
            'farm_counts': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'FARMS & LAND & ASSETS',
                'agg_level_desc': 'COUNTY',
                'short_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS',
                'domain_desc': 'NAICS CLASSIFICATION',
                'year': self._survey_year
                },
            'gasoline': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, GASOLINE - EXPENSE, MEASURED IN $',
                'year': self._survey_year
                },
            'diesel': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $',
                'year': self._survey_year
                },
            'lp_gas': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, LP GAS - EXPENSE, MEASURED IN $',
                'year': self._survey_year
                },
            'other': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, OTHER - EXPENSE, MEASURED IN $',
                'year': self._survey_year
                }
            }

    def call_nass_api(self, data_cat, **api_params):
        """
        Automatically collect state-level total fuel expenses data by NAICS
        code from USDA NASS 2017 Census results.

        Parameters
        ----------
        api_params : dict
            Dictionary of parameters to pass to USDA API.

        Returns
        -------
        usda_data : pandas.DataFrame
            DataFrame of USDA data.

        """

        base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

        params = {k:v for k, v in api_params.items()}
        params['key'] = self._usda_key

        if params['group_desc'] == 'FARMS & LAND & ASSETS':

            data_cols = ['state_name', 'state_alpha', 'state_fips_code',
                         'county_name', 'county_ansi',
                         'domaincat_desc', 'Value']

            col_rename = {
                'state_name': 'state',
                'state_alpha': 'state_abbr',
                'state_fips_code': 'fipstate',
                'Value': 'farm_counts',
                'county_name': 'county'
                }

        else:

            if params['agg_level_desc'] == 'STATE':

                data_cols = ['state_name', 'state_alpha', 'state_fips_code',
                             'domaincat_desc', 'Value']

            else:

                data_cols = ['region_desc', 'short_desc', 'Value']

            col_rename = {
                'region_desc': 'region',
                'state_name': 'state',
                'state_alpha': 'state_abbr',
                'short_desc': 'fuel_type',
                'state_fips_code': 'fipstate',
                'Value': 'expense_$'
                }
        try:
            r = requests.get(base_url, params=params)

        except requests.HTTPError as e:
            logging.error(f'{e}')

        # data = r.content
        # datajson = json.loads(data)

        try:
            err = r.json()['error'][0]
            logging.error(f'ERROR: {err}')

        except KeyError:
            usda_data = pd.DataFrame(r.json()['data'], columns=data_cols)

        usda_data.rename(columns=col_rename, inplace=True)
        usda_data.loc[:, 'year'] = params['year']

        # Split the column of NAICS codes:

        if params['agg_level_desc'] == 'REGION : MULTI-STATE':
            usda_data[['region', 'a']] = usda_data.region.str.split(
                ',', expand=True
                )
            drop_cols = ['a']

        else:
            usda_data[['a', 'b']] = \
                usda_data.domaincat_desc.str.split("(", expand=True)

            usda_data[['NAICS', 'c']] = usda_data.b.str.split(")", expand=True)
            drop_cols = ['domaincat_desc', 'a', 'b', 'c']

        usda_data = usda_data.drop(drop_cols, axis=1)

        # Remove invalid values & Rename columns & Set index & Sort
        invalid = '                 (D)'
        usda_data = usda_data.replace(invalid, usda_data.replace([invalid], '0'))

        if params['group_desc'] == 'EXPENSES':

            usda_data['fuel_type'] = data_cat

            # Remove commas in numbers
            try:
                usda_data.loc[:, 'expense_$'] = usda_data['expense_$'].apply(
                    lambda x: x.replace(',', "")
                    ).astype(int)

            except OverflowError:
                usda_data.loc[:, 'expense_$'] = usda_data['expense_$'].apply(
                    lambda x: x.replace(',', "")
                    ).astype(float)

            if params['agg_level_desc'] == 'STATE':
                usda_data.set_index('state', inplace=True)
                usda_data = usda_data.sort_index(ascending=True)

                # Find fraction by state
                usda_data.loc[:, 'expense_frac'] = usda_data['expense_$'].divide(
                    usda_data['expense_$'].sum(level='state')
                    )

                usda_data.reset_index(inplace=True)

            else:
                pass

            # elif params['agg_level_desc'] == 'REGION : MULTI-STATE':

            #     usda_data.loc[:, 'expense_frac'] = \
            #         usda_data['expense_$'] / usda_data['expense_$'].sum()

        else:

            usda_data = usda_data.query("NAICS != '1119'")

            usda_data.loc[:, 'farm_counts'] = usda_data['farm_counts'].apply(
                lambda x: x.replace(',', "")
                ).astype(int)

            # Create COUNTY_FIPS column to match mfg data
            usda_data.loc[:, 'COUNTY_FIPS'] = usda_data.fipstate.add(usda_data.county_ansi)
            usda_data.update(usda_data.COUNTY_FIPS.astype('int'))

            # Drop Aleutian Islands
            usda_data = usda_data.query("COUNTY_FIPS !=2")

            usda_data.set_index(['fipstate', 'NAICS', 'COUNTY_FIPS'],
                                inplace=True)
            usda_data.loc[:, 'state_naics_count'] = \
                usda_data.farm_counts.sum(level=[0, 1])
            usda_data.loc[:, 'statefraction'] = \
                usda_data.farm_counts.divide(usda_data.state_naics_count)
            usda_data = usda_data.reset_index()

        return usda_data

    def get_usda_elec(self):
        """
        Separate method to get USDA survey results for electricity expenditures. 
        There does not appear to be a way to use the NASS API to obtain these
        data.

        Returns
        -------
        usda_elec : pandas.DataFrame
            Farm electricity expenditures (in $1,000)
        """

        # No consistent naming for URL of updated data. This is the most recent as of 1/5/2023
        zip_url = 'https://www.ers.usda.gov/media/ql5phs2c/farmincome_wealthstatisticsdata_december2022.zip'

        r = requests.get(zip_url)

        with zipfile.ZipFile(BytesIO(r.content)) as zf:
            with zf.open(zf.filelist[0]) as f:
                usda_elec = pd.read_csv(f, encoding='latin_1')

        year = self._survey_year

        usda_elec = usda_elec.query(
            'Year==@year & VariableDescriptionPart2=="Electricity"'
            ).reset_index(drop=True)

        usda_elec.rename(columns={'State': 'state_abbr'}, inplace=True)

        usda_elec = pd.merge(
            usda_elec, self._region_file[['state', 'state_abbr', 'region']],
            on='state_abbr', how='inner'
            )

        usda_elec = usda_elec.pivot_table(
            index=['state', 'region'], columns='Year',
            values='Amount'  # Expenditures in $1,000
            ).reset_index(drop=False)

        return usda_elec

    def calc_fuel_fraction(self, usda_data):
            """

            Parameters
            ----------
            usda_data : dict of pandas.DataFrames

            Returns
            -------
            state_fuel : pandas.Dictionary
            """

            region_fuel = pd.concat(
                    [usda_data[k] for k in usda_data.keys() if k in ['gasoline', 'diesel', 'lp_gas', 'other']],
                    axis=0, ignore_index=True
                    )
            region_fuel.set_index(['region', 'fuel_type'], inplace=True)
            region_fuel.loc[:, 'fuel_type_frac'] = region_fuel['expense_$'].divide(
                    region_fuel['expense_$'].sum(level='region')
                    )

            region_file = self._region_file[
                ['state', 'state_abbr', 'state_code', 'region']
                ].copy(deep=True)

            state_fuel = usda_data['fuels'].copy(deep=True)

            state_fuel = pd.merge(state_fuel, region_file,
                                  on=['state', 'state_abbr'], how='outer')

            state_fuel.set_index(
                ['region', 'state', 'state_abbr', 'fipstate', 'NAICS'],
                inplace=True
                )

            state_fuel = region_fuel.fuel_type_frac.multiply(state_fuel['expense_$'])
            state_fuel.name = 'fuel_expense_dollars'
            state_fuel = pd.DataFrame(state_fuel)
            state_fuel.reset_index(inplace=True)

            # state_fuel = pd.merge(state_fuel, region_fuel,
            #                       on=['region', 'fuel_type'], how='left')
            # state_fuel.set_index(['region', 'state'], inplace=True)
            # state_fuel['expense_$'].multiply(region_fuel.fuel_type_frac, level='region')

            return state_fuel

    def get_eia_prices(self, years, fuel_type):
        """
        Download monthly petroleum fuel price data from EIA and aggregate
        to annual, state-level. Data from https://www.eia.gov/petroleum/gasdiesel/.

        Parameters
        ----------
        years : int, list or range
            Years of fuel prices to return.

        fuel_type : str; diesel, gasoline, LPG, OTHER
            Petroleum fuel type.

        Returns
        -------
        fuel_prices : pandas.DataFrame
            DataFrame of fuel prices by State.

        """

        if fuel_type == 'diesel':
            fuel_prices = pd.read_excel(
                'https://www.eia.gov/petroleum/gasdiesel/xls/psw18vwall.xls',
                sheet_name='Data 2', header=2
                )

            fuel_prices.columns = [
                'Date',
                'US',
                'EAST COAST',
                'NEW ENGLAND',
                'CENTRAL ATLANTIC',
                'LOWER ATLANTIC',
                'MIDWEST',
                'GULF COAST',
                'ROCKY MOUNTAIN',
                'WEST COAST',
                'CALIFORNIA',
                'WEST COAST EXCEPT CALIFORNIA'
                ]

            fuel_prices.drop(['US', 'EAST COAST', 'WEST COAST'],
                             axis=1)

        elif fuel_type == 'gasoline':
            fuel_prices = pd.read_excel(
                'https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls',
                sheet_name='Data 3', header=2
                )

            fuel_prices.columns = [
                'Date',
                'US',
                'EAST COAST',
                'NEW ENGLAND',
                'CENTRAL ATLANTIC',
                'LOWER ATLANTIC',
                'MIDWEST',
                'GULF COAST',
                'ROCKY MOUNTAIN',
                'WEST COAST',
                'CALIFORNIA',
                'COLORADO',
                'FLORIDA',
                'MASSACHUSETTS',
                'MINNESOTA',
                'NEW YORK',
                'OHIO',
                'TEXAS',
                'WASHINGTON',
                'BOSTON', 'CHICAGO', 'CLEVELAND', 'DENVER',
                'HOUSTON', 'LOS ANGELES', 'MIAMI', 'NEW YORK CITY',
                'SAN FRANCISCO', 'SEATTLE'
                ]

            fuel_prices.drop(
                ['US', 'EAST COAST', 'BOSTON', 'CHICAGO', 'CLEVELAND',
                 'DENVER', 'HOUSTON', 'LOS ANGELES', 'MIAMI',
                 'NEW YORK CITY', 'SAN FRANCISCO',
                 'SEATTLE'], axis=1,
                inplace=True
                )

        elif fuel_type == 'LPG':
            fuel_prices = pd.read_excel(
                'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPLLPA_PWR_DPGAL_w.xls',
                sheet_name='Data 1', header=2
                )

            re_str = r'(?<=Weekly ).*(?= Propane)'

        elif fuel_type == 'OTHER':
            fuel_prices = pd.read_excel(
                'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPD2F_PWR_DPGAL_w.xls',
                sheet_name='Data 1', header=2
                )

            re_str = r'(?<=Weekly ).*(?= No. 2)'

        if fuel_type in ['LPG', 'OTHER']:
            col_list = ['Date']

            for col in fuel_prices.columns:
                new_col = re.search(re_str, col)

                if new_col:
                    col_list.append(new_col.group(0).upper())

                else:
                    continue

            fuel_prices.columns = col_list

            fuel_prices.rename(columns={'U.S.': 'US'}, inplace=True)
            fuel_prices.columns =\
                [re.sub(' \(PADD \w+\)', '', x) for x in fuel_prices.columns]

        fuel_prices.loc[:, 'Date'] = pd.to_datetime(
            fuel_prices.Date
            )

        fuel_prices.set_index('Date', inplace=True)

        fuel_prices = fuel_prices.groupby(
            fuel_prices.index.year
            ).mean()

        fuel_prices = fuel_prices.T

        if fuel_type == 'LPG':
            fcols = ['state', 'state_code',
                     f'{fuel_type.lower()}_wholesale_padd']
            fuel_prices.index.name = f'{fuel_type.lower()}_wholesale_padd'

        else:
            fuel_prices.index.name = f'{fuel_type.lower()}_padd'
            fcols = ['state', 'state_code', f'{fuel_type.lower()}_padd']

        fuel_prices = fuel_prices[years]
        fuel_prices['fuel_type'] = fuel_type
        fuel_prices.reset_index(level=0, inplace=True)

        region_file = self._region_file[fcols].copy(deep=True)

        fuel_prices = pd.merge(
            fuel_prices, region_file, on=fcols[2],
            how='outer'
            )

        final_cols = ['state', 'state_code', 'fuel_type']
        for y in years:
            final_cols.append(y)

        fuel_prices = fuel_prices[final_cols]
        fuel_prices.dropna(subset=['state'], inplace=True)

        return fuel_prices

    def combine_fuel_prices(self, fuel_price_dfs):
        """
        Concatenate formatted EIA fuel price data and
        calculate $/MMBtu.

        Parameters
        ----------
        fuel_price_dfs : list of pandas.DataFrames
            Fuel price dataframes.

        Returns
        -------
        all_fuel_prices : pandas.DataFrame
            Fuel price (in $/MMBtu) by state.

        """

        all_fuel_prices = pd.concat(
            fuel_price_dfs, axis=0, ignore_index=True
            )
        all_fuel_prices.set_index(
            ['fuel_type', 'state', 'state_code'], inplace=True
            )

        convert = pd.DataFrame.from_dict(
            self._heat_content, orient='index'
            )
        convert.columns = ['MMBtu_per_barrel']
        convert.index.name = 'fuel_type'

        # all_fuel_prices = pd.merge(
        #     all_fuel_prices, convert, on='fuel_type',
        #     how='left'
        #     )

        price_cols = all_fuel_prices.columns.to_list()

        # Original fuel prices in $/gal; convert to $/MMBtu
        all_fuel_prices = pd.concat(
            [convert.divide(
                all_fuel_prices[c], level='fuel_type', axis=0
                ) * 42 for c in all_fuel_prices.columns],
            axis=1, ignore_index=False
            )

        # Prices in $/MMBtu
        all_fuel_prices.columns = price_cols
        all_fuel_prices.reset_index(inplace=True)
        all_fuel_prices.replace(
            {'OTHER': 'other', 'LPG': 'lp_gas'},
            inplace=True
            )

        return all_fuel_prices

    def get_elec_prices(self, years):
        """
        Download EIA electricity price data 
        for industrial customers by state for
        specified years.

        Parameters
        ----------
        years : list of integers


        Returns
        -------
        elec_data : pandas.DataFrame
            DataFrame of EIA state-level electricity prices for
            specified years.
        """

        elec_price = pd.read_excel(
            'https://www.eia.gov/electricity/data/state/sales_annual_a.xlsx',
            sheet_name='Total Electric Industry', header=[0,1,2],
            index_col=[0, 1]
            )

        elec_price = elec_price.loc[:, ('INDUSTRIAL', 'Price')].copy(deep=True)
        elec_price.reset_index(inplace=True)
        elec_price.columns = ['year', 'state', 'cents_per_kWh']
        elec_price = pd.merge(
            elec_price,
            self._region_file[['state', 'state_abbr', 'state_code']],
            on='state', how='inner'
            )

        elec_price = elec_price.where(
            elec_price.year.isin(years)
            ).dropna().reset_index(drop=True)

        elec_price = elec_price.pivot_table(
            index=['state', 'state_abbr', 'state_code'],
            columns='year',
            values='cents_per_kWh'
            ).reset_index()

        return elec_price

    # def get_eia_elec_data(self, years):
    #     """
    #     Parameters
    #     ----------
    #     years : list of integers


    #     Returns
    #     -------
    #     elect_data : pandas.DataFrame
    #         DataFrame of EIA state-level electricity sales (MWh)
    #         and revenue ($1,000). Price estimated as revenue/sales.

    #     """

    #     def dl_data(years, type):
    #         excel_url = f'https://www.eia.gov/electricity/data/state/{type}_annual.xlsx'
    #         df = pd.read_excel(excel_url, header=1)
    #         df = df.loc[df['Year'].isin(years)]
    #         df = df[
    #             (df['Industry Sector Category'] == 'Total Electric Industry') &
    #             (df['State'] != 'US')
    #             ]
    #         df = df[['State', 'Industrial', 'Year']]
    #         if type =='revenue':
    #             new_col = 'rev_000$'
    #         else:
    #             new_col = 'sal_mWh'
    #         df.rename(columns={
    #             'State': 'state_abbr', 'Industrial': new_col,
    #             'Year': 'year'
    #             }, inplace=True)

    #         df.set_index(['state_abbr', 'year'], inplace=True)

    #         return df

    #     elect_data = pd.concat(
    #         [dl_data(years, type) for type in ['revenue', 'sales']],
    #         axis=1
    #         )
        
    #     elect_data.loc[:, 'dollar_per_MWh'] = \
    #         elect_data['rev_000$'].divide(elect_data.sal_mWh)*1000

    #     elect_data.drop(['rev_000$', 'sal_mWh'], axis=1,
    #                     inplace=True)

    #     return elect_data

    def calc_state_energy_use(self, eia_prices, usda_data):
        """
        Calculates energy use based on USDA fuel  or e
        electricity expenditures and EIA fuel or
        electricity prices.

        Parameters
        ----------
        eia_prices : pandas.DataFrame
            EIA fuel or electricity prices

        usda_data : dict of pandas.DataFrames


        Returns
        -------
        county_energy : pandas.DataFrame
        """

        # year_cols = eia_prices.columns.to_list()
        if 'fuel_type' in eia_prices.columns:
            region_fuel = pd.concat(
                    [usda_data[k] for k in usda_data.keys() if k in ['gasoline', 'diesel', 'lp_gas', 'other']],
                    axis=0, ignore_index=True
                    )

            region_fuel.set_index(['region', 'fuel_type'], inplace=True)
            region_fuel.loc[:, 'fuel_type_frac'] = region_fuel['expense_$'].divide(
                    region_fuel['expense_$'].sum(level='region')
                    )

            region_file = self._region_file[
                ['state', 'state_abbr', 'state_code', 'region']
                ].copy(deep=True)

            state_energy = pd.merge(
                usda_data['fuels'], region_file[['state', 'region']],
                on='state', how='inner'
                )

            state_energy.set_index(
                ['region', 'state', 'state_abbr', 'fipstate', 'NAICS'],
                inplace=True
                )

            fuel_exp = region_fuel.fuel_type_frac.multiply(state_energy['expense_$'])
            fuel_exp = fuel_exp.multiply(state_energy['expense_frac'])

            eia_prices.set_index(
                ['fuel_type', 'state'], inplace=True
                )

            energy = eia_prices[self._census_year].multiply(fuel_exp)
            energy.name = 'MMBtu'

            state_energy = pd.concat(
                [state_energy, energy], axis=1,
                ignore_index=False
                )

        else:
            eia_prices.set_index('state', inplace=True)
            state_energy.set_index('state', inplace=True)

            state_energy.loc[:, 'MMBtu'] = state_energy.divide(
                eia_prices[self._census_year], axis=0
                ) * 100 * 1000 * 3412.14 / 10**6  # price is in cents per kWh; expenditures in $1000

            state_energy = pd.concat(
                [usda_data['electricity'].set_index(['state', 'NAICS']),
                 usda_data['electricity'].set_index(['state', 'NAICS'])['expense_frac'].multiply(state_energy.MMBtu)],
                axis=1, ignore_index=False
                )

        return state_energy


    def calc_county_intensity(self, state_energy, usda_counts):
        """
        
        Parameters
        ----------
        state_energy : pandas.DataFrame

        usda_counts : pandas.DataFrame

        Returns
        -------
        county_intensity : pandas.DataFrame

        """

        return



if __name__ == '__main__':

    # API keys stored locally in json file
    with open(
        os.path.join(os.environ['USERPROFILE'], 'Documents', 'API_auth.json')
        ) as f:
        keys = json.load(f)

    ag = Ag(usda_api_key=keys['usda_API'], eia_api_key=keys['eia_API'])
    year = [2017]

    usda_data = {k: ag.call_nass_api(data_cat=k, **ag._data_fields[k]) for k in ag._data_fields.keys()}
    usda_elec = ag.get_usda_elec()

    eia_prices = [ag.get_eia_prices(year, f) for f in ['diesel', 'gasoline', 'LPG', 'OTHER']]
    eia_prices = ag.combine_fuel_prices(eia_prices)

    eia_price_elec = ag.get_elec_prices(year)

    # for f in eia_fuels.keys():
    #     logging.info(f, eia_fuels[f].head())