
import pandas as pd
import requests


class Mining:

    def __ini__(self):

        self._census_api_params = {
            'for':'us'
            }

    def get_econ_census(self, api_key, year=2017):
        """
        Call Econonomic Census API to download cost of fuels used by mining sector.

        Parameters
        ----------
        api_key : str
            Census API key

        year : int
            Year of Economic Census. Conducted every 5 years; latest avaiable at the 
        time of publication is 2017.

        Returns
        -------
        econ_census : pandas.DataFrame
        """

        base_url = f'https://api.census.gov/data/{year}/ecnmatfuel'

        params = self._census_api_params.copy()
        params['key'] = api_key
        params[f'NAICS{year}'] = '21*'

        # Different Economic Census years use different variables in their APIs
        # Fuel cost in $1,000s
        api_vars = requests.get(f'https://api.census.gov/data/{year}/ecnmatfuel/variables.json').json()['variables']
        api_vars = [api_vars[v]['attributes'] for v in ['MATFUELQTY', 'MATFUELCOST', 'UNITS', 'MATFUEL']]
        api_vars = ','.join(api_vars)
        api_vars = api_vars+f',M_FI,MATFUEL,MATFUELQTY,MATFUELCOST'
        params['get'] = api_vars

        r = requests.get(base_url, params=self._census_api_params)

        econ_census = pd.DataFrame(r.json())
        econ_census.columns = econ_census.iloc[0]
        econ_census = econ_census.drop(0, axis=0).reset_index(drop=True)

        econ_census.rename(columns={
            f'NAICS{year}':'NAICS',
            'MATFUEL':'fuel_id',
            'MATFUEL_LABEL': 'fuel_type_long',
            'MATFUEL_TTL':'fuel_type', 
            'MATFUELCOST':'fuel_cost_k_usd',
            'MATFUELCOST_F':'fuel_cost_missing',
            'MATFUELQTY':'fuel_qty',
            'MATFUELQTY_F': 'fuel_qty_missing',
            'UNITS_TTL':'fuel_qty_unit',
            'UNITS_LABEL': 'units',
            'UNITS_F': 'units_missing',
            'M_FI':'fuel_flag'
            }, inplace=True)

        econ_census = econ_census[['NAICS', 'fuel_id', 'fuel_type',
                                   'fuel_cost_k_usd', 'fuel_cost_missing',
                                   'fuel_qty', 'fuel_qty_unit', 'fuel_flag']]

        fuel_type_dict = {
            2012: [
                {'fuel_id': [2, 960018, 974000, 21111003, 21111015, 21111101,
                            21211003, 32411015, 32411017, 32411019],
                'fuel_type':['TOTAL','MISC','UNDISTRIBUTED','NATURAL GAS',
                             'NATURAL GAS','CRUDE OIL','COAL','GASOLINE',
                             'DIESEL','RESIDUAL FUEL OIL']}
                ],
            2017: [
                {'fuel_id': [772002, 960018, 974000, 21111003, 21111015,
                             21211003, 32411015, 32411021, 21111029,
                             21111101, 21211013],
                'fuel_type':['TOTAL', 'MISC', 'UNDISTRIBUTED','NATURAL GAS',
                             'NATURAL GAS', 'COAL', 'GASOLINE', 'DIESEL',
                             'NATURAL GAS', 'CRUDE OIL', 'COAL']}
                ]
            }

        fuel_type = pd.DataFrame(fuel_type_dict[year][0])
    
        econ_census = econ_census[econ_census.fuel_flag == 'F']
        econ_census = econ_census.drop('fuel_flag', axis=1)

        for c in ['NAICS', 'fuel_id', 'fuel_cost_k_usd', 'fuel_qty']:
            if c == 'fuel_qty':
                econ_census[c] = econ_census[c].astype(float)

            else:
                econ_census[c] = econ_census[c].astype(int)

        
        econ_census = pd.merge(
            econ_census, fuel_type, on='fuel_id',
            how='left'
            )

        missing_cost = econ_census.groupby(
            ['NAICS', 'fuel_type', 'fuel_cost missing']
            ).fuel_cost_k_usd.count()

        cost = econ_census.groupby(
            ['NAICS', 'fuel_type']
            ).fuel_cost_k_usd.sum()


        