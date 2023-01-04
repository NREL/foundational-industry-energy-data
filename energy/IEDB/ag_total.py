
import requests
import pandas as pd
import json
import logging
import os
import re


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

        self._data_fields = {
            'fuels': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'STATE',
                'short_desc': 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $',
                'domain_desc': 'NAICS CLASSIFICATION'
                },
            'electricity': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'STATE',
                'short_desc': 'AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $',
                'domain_desc': 'NAICS CLASSIFICATION'
                },
            'farm_counts': {
                'source_desc': 'CENSUS',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'FARMS & LAND & ASSETS',
                'agg_level_desc': 'COUNTY',
                'short_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS',
                'domain_desc': 'NAICS CLASSIFICATION'
                },
            'gasoline': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, GASOLINE - EXPENSE, MEASURED IN $'
                },
            'diesel': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $'
                },
            'lp_gas': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, LP GAS - EXPENSE, MEASURED IN $'
                },
            'other': {
                'source_desc': 'SURVEY',
                'sector_desc': 'ECONOMICS',
                'group_desc': 'EXPENSES',
                'agg_level_desc': 'REGION : MULTI-STATE',
                'commodity_desc': 'FUELS',
                'short_desc': 'FUELS, OTHER - EXPENSE, MEASURED IN $'
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
        params['year'] = 2017

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

            elif params['agg_level_desc'] == 'REGION : MULTI-STATE':

                usda_data.loc[:, 'expense_frac'] = \
                    usda_data['expense_$'] / usda_data['expense_$'].sum()

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
            logging.info(fuel_prices.columns)
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

        region_file = pd.read_csv(
            os.path.abspath('energy/IEDB/input_region.csv'),
            usecols=fcols
            )

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

        """

        all_fuel_prices = pd.concat(
            fuel_price_dfs, axis=0, ignore_index=False)
        all_fuel_prices.set_index(
            ['fuel_type', 'state', 'state_code'], inplace=True
            )

        convert = pd.DataFrame.from_dict(
            self._heat_content, orient='index'
            )
        convert.columns = ['MMBtu_per_barrel']
        convert.index.name = 'fuel_type'

        all_fuel_prices = pd.merge(
            all_fuel_prices, convert, on='fuel_type',
            how='left'
            )

        # Original fuel prices in $/gal; convert to $/MMBtu
        all_fuel_prices = all_fuel_prices[all_fuel_prices.columns].divide(
            convert['MMBtu_per_barrel'], level=0
            ) * 42

        all_fuel_prices.reset_index(inplace=True)

        return all_fuel_prices

    def get_eia_elec_data(self, years):
        """
        Parameters
        ----------
        years : list of integers


        Returns
        -------
        elect_data : pandas.DataFrame
            DataFrame of EIA state-level electricity sales (MWh)
            and revenue ($1,000). Price estimated as revenue/sales.

        """

        def dl_data(years, type):
            excel_url = f'https://www.eia.gov/electricity/data/state/{type}_annual.xlsx'
            df = pd.read_excel(excel_url, header=1)
            df = df.loc[df['Year'].isin(years)]
            df = df[
                (df['Industry Sector Category'] == 'Total Electric Industry') &
                (df['State'] != 'US')
                ]
            df = df[['State', 'Industrial', 'Year']]
            if type =='revenue':
                new_col = 'rev_000$'
            else:
                new_col = 'sal_mWh'
            df.rename(columns={
                'State': 'state_abbr', 'Industrial': new_col,
                'Year': 'year'
                }, inplace=True)

            df.set_index(['state_abbr', 'year'], inplace=True)

            return df

        elect_data = pd.concat(
            [dl_data(years, type) for type in ['revenue', 'sales']],
            axis=1
            )

        return elect_data

    def calc_fuel_use(self, usda_fuels, eia_prices):
        """
        Calculates fuel use based on USDA fuel expenditures
        and EIA fuel prices
    
        Parameters
        ----------
        usda_fuels : list of pandas.DataFrames

        eia_prices : pandas.DataFrame

        Returns
        -------
        usda_use : pandas.DataFrame
        """

        for df in usda_fuels:
            if 


if __name__ == '__main__':

    # API keys stored locally in json file
    with open(
        os.path.join(os.environ['USERPROFILE'], 'Documents', 'API_auth.json')
        ) as f:
        keys = json.load(f)

    ag = Ag(usda_api_key=keys['usda_API'], eia_api_key=keys['eia_API'])
    year = [2017]

    usda_fuels = []
    usda_counts = []

    for k in ag._data_fields.keys():

        if k == 'farm_counts':
            usda_counts.append(ag.call_nass_api(data_cat=k, **ag._data_fields[k]))

        else:
            usda_fuels.append(ag.call_nass_api(data_cat=k, **ag._data_fields[k]))

    # eia_fuels = {f: ag.get_eia_prices(year, f) for f in ['diesel', 'gasoline', 'LPG', 'OTHER']}
    eia_prices = [ag.get_eia_prices(year, f) for f in ['diesel', 'gasoline', 'LPG', 'OTHER']]
    eia_prices = ag.combine_fuel_prices(eia_prices)

    eia_fuels['electricity'] = ag.get_eia_elec_data(year)

    for f in eia_fuels.keys():
        logging.info(f, eia_fuels[f].head())