
import pandas as pd
import requests

class Mining:

    def __init__(self, **api_keys, year):
        """

        """

        self._census_api_params = {
            'for': 'us'
            }

        self._eia_key = api_keys['eia']
        self._census_key = api_keys['census']


    def get_econ_census(self):
        """
        Call Econonomic Census API to download cost of fuels used by mining
        and construction sector.


        Returns
        -------
        econ_census : pandas.DataFrame
        """

        year = 2017

        base_url = f'https://api.census.gov/data/{year}/ecnmatfuel'

        params = self._census_api_params.copy()
        params['key'] = self._census_key
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
            f'NAICS{year}': 'NAICS',
            'MATFUEL': 'fuel_id',
            'MATFUEL_LABEL': 'fuel_type_long',
            'MATFUEL_TTL': 'fuel_type',
            'MATFUELCOST': 'fuel_cost_k_usd',
            'MATFUELCOST_F': 'fuel_cost_missing',
            'MATFUELQTY': 'fuel_qty',
            'MATFUELQTY_F': 'fuel_qty_missing',
            'UNITS_TTL': 'fuel_qty_unit',
            'UNITS_LABEL': 'units',
            'UNITS_F': 'units_missing',
            'M_FI': 'fuel_flag'
            }, inplace=True)

        econ_census = econ_census[['NAICS', 'fuel_id', 'fuel_type_long',
                                   'fuel_cost_k_usd', 'fuel_cost_missing',
                                   'fuel_qty', 'units', 'fuel_flag']]

        fuel_type_dict = {
            2012: [
                {'fuel_id': [2, 960018, 974000, 21111003, 21111015, 21111101,
                             21211003, 32411015, 32411017, 32411019],
                 'fuel_type': ['TOTAL', 'MISC', 'UNDISTRIBUTED', 'NATURAL GAS',
                             'NATURAL GAS', 'CRUDE OIL', 'COAL', 'GASOLINE',
                             'DIESEL', 'RESIDUAL FUEL OIL']}
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

        missing_cost_count = econ_census.groupby(
            ['NAICS', 'fuel_type', 'fuel_cost_missing']
            ).fuel_cost_k_usd.count()

        cost = econ_census.groupby(
            ['NAICS', 'fuel_type']
            ).fuel_cost_k_usd.sum()

        cost.xs('TOTAL', level=1).subtract(
            econ_census[econ_census.fuel_type !='TOTAL'].groupby(['NAICS']).fuel_cost_k_usd.sum()
            )

        cost = econ_census.groupby()

        # #TODO change from hard-coding to automated
        # Unlike 2012, 2017 Economic Census withholds total fuel expenditures
        # for some 6-digit NAICS (212221, 212222, 212291, 212392, 212393, 213114, 213115)
        total_fuel_exp = 8176591  # $1000. From https://data.census.gov/cedsci/table?hidePreview=true&table=EC1721BASIC&tid=ECNBASIC2017.EC1721BASIC&lastDisplayedRow=47&q=EC1721BASIC%3A%20Mining%3A%20Summary%20Statistics%20for%20the%20U.S.,%20States,%20and%20Selected%20Geographies%3A%202017


        # #TODO automate filling in missing data. 
        # Aggregate fuel spend higher-level NAICS
        cost

        # aggregate

    def get_eia_prices(self, fuel, year):
        """
        Download and format Energy Information
        Administration Monthly Energy Review prices.

        Parameters
        ----------
        fuel : str; residual, ng, gasoline, crude
            Fuel type.

        year : int
            Year of fuel price.

        Returns
        -------
        fuel_price : pandas.DataFrame

        """

        params = {
            'diesel': {
                'tbl': 'T09.04',
                'msn': 'DFONUUS',
                'fuel_type': 'DIESEL',
                'conversion':  42 / 5.774 # EIA MER Table A3
                },
            'residual': {
                'tbl': 'T09.06',
                'msn': 'D2WHUUS',
                'fuel_type': 'RESIDUAL FUEL OIL',
                'conversion': 42 / 6.287  # EIA MER Table A3
                },
            'ng': {
                'tbl': 'T09.10',
                'msn': 'NGCGUUS',
                'fuel_type': 'NATURAL GAS',
                'conversion': (1/1000 * 1/1024) * 1000000 # https://www.eia.gov/tools/faqs/faq.php?id=45&t=8
                },
            'gasoline': {
                'tbl': 'T09.04',
                'msn': 'RMRTUUS',
                'fuel_type': 'GASOLINE',
                'conversion': 42 / 5.063  # EIA MER Table A3
                },
            'crude': {
                'tbl': 'T09.01',
                'msn': 'CODPUUS',
                'fuel_type': 'CRUDE OIL',
                'conversion': 1/5.8  # EIA MER Table A3
                },
            'coal': {
                'base_url': 'https://api.eia.gov/v2/coal/price-by-rank/data/?',
                'fuel_type': 'COAL',
                'conversion': 1/21.449 # EIA MER Table A5
                },
            'misc': {
                'base_url': 'https://api.eia.gov/v2/seds/data/',
                'fuel_type': 'MISC',
                }
            }

        if fuel == 'coal':

            api_params = {
                'api_key': self._eia_key,
                'frequency': 'annual',
                'data[0]': 'price',
                'facets[stateRegionId][]': 'US',
                'facets[coalRankId][]': 'TOT',
                'sort[0][column]': 'period',
                'sort[0][direction]': 'desc',
                }

            r = requests.get(params[fuel]['base_url'],
                             params=api_params)

            fuel_price = pd.DataFrame(r.json()['response']['data'])

            fuel_price = fuel_price.query('period==@year')

            fuel_price.loc[:, 'price_usd_per_mmbtu'] = \
                fuel_price.unit * params[fuel]['conversion']

        elif fuel == 'misc':

            api_params = {
                'api_key': self._eia_key,
                'frequency': 'annual',
                'data[0]': 'value',
                'facets[seriesId][]': 'PEICD',
                'facets[stateRegionId][]': 'US',
                'sort[0][column]': 'period',
                'sort[0][direction]': 'desc',
                }
   
            r = requests.get(params[fuel]['base_url'],
                             params=api_params)

            fuel_price = pd.DataFrame(r.json()['response']['data'])

            fuel_price = fuel_price.query('period==@year')

            fuel_price.rename(columns={'value': 'price_usd_per_mmbtu'}, inplace=True)

        else:

            fuel_price = pd.read_csv(
                        f'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl={params[fuel]['tbl']}',
                        dtype={'Value': 'float'}
                        )

            fuel_price = fuel_price.loc[
                    (fuel_price['MSN'] == params[fuel]['msn']) &
                    (fuel_price['YYYYMM'] == int(f'{year}13'))
                    ]

            fuel_price['price_usd_per_mmbtu'] = \
                fuel_price.Value * params[fuel]['conversion']

        fuel_price = fuel_price[['price_usd_per_mmbtu']]
        fuel_price['fuel_type'] = params[fuel]['fuel_type']
        fuel_price = fuel_price[['fuel_type', 'price_usd_per_mmbtu']]

        return fuel_price

    def combine_eia_prices(self, fuels, year):
        """

        Parameters
        ----------
        fuels : list of strings
            Fuels 

        year : int
            Year of fuel prices

        """

        fuel_prices = pd.concat(
            [self.get_eia_prices(f, year) for f in fuels],
            axis=0, ignore_index=True            
            )

        return fuel_prices

    def calc_national_fuel()

    def main(self):

        # API keys stored locally in json file
        with open(
            os.path.join(os.environ['USERPROFILE'], 'Documents', 'API_auth.json')
        ) as f:
            keys = json.load(f)

        year = 2017

        mining = Mining(keys, year)
    
        fuels = ['residual', 'ng', 'diesel', 'coal', 'gasoline', 'crude', 'misc']


if __name__ == '__main__':

    api_keys = {
        'eia': 'fb1b162b14d1e65ca506cf0bdf0fe173',
        'census': '489f08f390013bc6d41ee377e86ea8c1b0dd5267'
        }




    eia_prices = mining
