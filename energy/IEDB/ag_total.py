
import requests
import pandas as pd
import json
import logging

class Ag:

    def __init__(self):
        logging.basicConfig(level=logging.INFO)


    def get_ag_census_data(self, year, fuel_type, api_key):
        """
        Automatically collect state-level total fuel expenses data by NAICS
        code from USDA NASS 2017 Census results.

        Parameters
        ----------
        year : int; 2012 or 2017
            Ag Census year. Coducted every 5 years.

        fuel_type : str; fuels, gasoline, diesel, lp_gas, other, or electricity
            "Other" assumed to be residual oil. 

        api_key : str
            USDA API key. Obtain from https://www.ers.usda.gov/developer/data-apis/.

        Returns
        -------
        state_tot : pandas.DataFrame
            DataFrame of fuel or electricity expenditures by state. 

        """

        data_fields = {
            'fuels': {
                'data_desc': 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                },
            'electricity': {
                'data_desc': 'AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                },
            'farm_counts': {
                'data_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS',
                'group_desc': 'FARMS & LAND & ASSETS',
                'agg_level': 'COUNTY'
                },
            'gasoline': {
                'data_desc': 'FUELS, GASOLINE - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                },
            'diesel': {
                'data_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                },
            'lp_gas': {
                'data_desc': 'FUELS, LP GAS - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                },
            'other': {
                'data_desc': 'FUELS, OTHER - EXPENSE, MEASURED IN $',
                'group_desc': 'EXPENSES',
                'agg_level': 'STATE'
                }
            }

        base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

        params = {
            'key': api_key,
            'source_desc': 'CENSUS', 'sector_desc': 'ECONOMICS',
            'group_desc':  data_fields[fuel_type]['group_desc'], 
            'year': year,
            'agg_level_desc': data_fields[fuel_type]['agg_level'], 
            'short_desc': data_fields[fuel_type]['data_desc'],
            'domain_desc': 'NAICS CLASSIFICATION'
            }

        try:
            r = requests.get(base_url, params=params)

        except requests.HTTPError as e:
            logging.error(f'{e}')

        #print(r.content)
        # url = r.url
        #print(url)

        # response = urllib.request.urlopen(url)

        data = r.content

        datajson = json.loads(data)

        state_tot = pd.DataFrame(
            datajson['data'], columns=['state_name', 'state_alpha',
                                       'domaincat_desc', 'Value']
            )

        pd.set_option('display.max_columns', None)
        #print(state_tot.head(20))


        ####### Split the column of NAICS codes:
        state_tot[['a', 'b']] = \
            state_tot.domaincat_desc.str.split("(", expand=True)

        state_tot[['NAICS', 'c']] = state_tot.b.str.split(")", expand=True)

        state_tot = state_tot.drop(['domaincat_desc', 'a', 'b', 'c'], axis=1)
        #print(state_tot.head(20))


        ####### Remove invalid values & Rename columns & Set index & Sort
        invalid = '                 (D)'

        state_tot = state_tot.replace(invalid, state_tot.replace([invalid], '0'))

        state_tot.rename(
            columns={
                'state_name':'state',
                'state_alpha':'state_abbr',
                'Value':'ag_expense_$'
                }, inplace=True
            )

        state_tot.set_index('state', inplace=True)

        state_tot = state_tot.sort_index(ascending=True)

        ####### Remove commas in numbers
        state_tot['ag_expense_$'] = state_tot['ag_expense_$'].apply(
            lambda x: x.replace(',', "")
            ).astype(int)

        ####### Find fraction by state
        state_tot['ag_expense_state_pct'] = state_tot['ag_expense_$'].divide(
                                       state_tot['ag_expense_$'].sum(level='state')
                                       )

        return state_tot

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
            excel_url = f'http://www.eia.gov/electricity/data/state/{type}_annual.xlsx'
            df = pd.read_excel(excel_url, header=1)
            df = df.loc[df['Year'].isin(years)]
            df = df.query("Industry Sector Category=='Total Electric Industry'&State!='US'")
            df = df[['State', 'Industrial', 'Year']]
            if type =='revenue':
                new_col = 'rev_000$'
            else:
                new_col = 'sal_mWh'
            df.rename(columns={
                'State': 'state_abbr', 'Industrial':new_col,
                'Year': 'year'
                }, inplace=True)

            df.set_index(['state_abbr','Year'], inplace=True)
            return df

        elect_data = pd.concat(
            [dl_data(years, type) for type in ['revenue', 'sales']],
            axis=1
            )

        return elect_data




if __name__ == '__main__':

    ag = Ag()

    for f in ['diesel', 'gasoline', 'electricity']:
        data = ag.get_ag_census_data(2017, f, '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8')
        logging.info(f'Data: {data.head()}')

    elect_data = ag.get_eia_elect_data([2017])
    logging.info(f'EIA data: {elect_data.head()}')