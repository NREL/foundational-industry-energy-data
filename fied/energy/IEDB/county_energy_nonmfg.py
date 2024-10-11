import pandas as pd
import requests


class County_energy:

    def __init__(self, api_keys**):

        self._bea_api_parms = {
            'method': 'GetData',
            'datasetname': 'RegionalProduct',
            'Component': 'RGDP_SAN',
            'Year': 'ALL',
            'GeoFips': 'STATE',
            'ResultFormat': 'JSON'
            'UserID': api_keys['bea']
            }

        # API parameters for Census Materials and Fuels table
        self._cenmat_api_params = {
            'get': 'NAICS2012,MATFUEL,MATFUEL_TTL,MATFUELCOST,MATFUELCOST_F,MATFUELQTY,UNITS_TTL,M_FI',
            'for':'us',
            'key': api_keys['census']
            }

        # API parameters for Census basic table
        self._cenbas_api_params = {
            'get':'ESTAB,ELECPCH',
            'for':'us'
            }

        # API parameters for EIA SEDS
        self._eiaseds_api_params = {
            'api_key': api_keys['eia'],
            'series_id': 'SEDS.PEICD.US.A'
            }
    

    def get_econ_census_matfuel(self, industry_id, year=2017):
        """
        API call for Economic Census, materials and fuels table. 
    
        Parameters
        ----------
        params : dict
            Dictionary of parameters to pass to API call.
            Must include api_key

        Returns
        -------
        census_data : pandas.DataFrame
        """

        base_url = f'https://api.census.gov/data/{year}/ecnmatfuel'


    def get_econ_census_basic(self, industry_id, year=2017):
        """
        API call for Economic Census, basic table.

        Parameters
        ----------
        industry_id : str
            2-digit NAICS code of industry.

        year : int; 2017 or 2012
            Year of Economic Census,

        Returns
        -------
        census_data : pandas.DataFrame

        """

        base_url = f'http://api.census.gov/data/{year}/ecnbasic'

        params = self._cenmat_api_params
        params[f'NAICS{year}'] = f'{industry_id}*'

        r = requests.get(base_url, params)

        census_data = pd.DataFrame(r.json()) 
        census_data.columns = census_data.iloc[0]
        census_data = census_data[1:]

        return census_data


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
                }
        }

        fuel_price = pd.read_csv(
                    f'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl={params[fuel]['tbl']}',
                    dtype={'Value': 'float'}
                    )

        fuel_price = fuel_price.loc[
                (fuel_price['MSN'] == params[fuel]['msn']) &
                (fuel_price['YYYYMM'] == int(f'{year}13'))
                ]

        # fuel_price = fue
        # fuel_price.rename(columns = {'Value':'price_usd_per_gal'}, inplace=True)

        # fuel_price = fuel_price.astype({'price_usd_per_gal': float})

        fuel_price['price_usd_per_mmbtu'] = \
            fuel_price.Value * params[fuel]['conversion']

        fuel_price = fuel_price[['price_usd_per_mmbtu']]
        fuel_price['fuel_type'] = params[fuel]['fuel_type']
        fuel_price = fuel_price[['fuel_type', 'price_usd_per_mmbtu']]

        return fuel_price

    def get_eia_seds(self, api_key, series_id, year):
        """
        Use Energy Information Administration State Energy Data System (SEDS)
        API for industrial total energy.

        Parameters
        ----------
        api_key : str
            EIA API key

        series_id : str
            EIA API series ID for SEDSprice_diesel

        year : int
            Year of data.

        Returns
        -------
        seds_price : pandas.DataFrame

        """
    
        base_url = 'http://api.eia.gov/series/'

        params = {
            'api_key': api_key,
            'series_id': series_id
            }

        r = requests.get(base_url, params=params)

        data = r.json()
        seds_price = pd.DataFrame(
            data['series'][0]['data'],
            columns=['year', 'price_usd_per_mmbtu']
            )

        seds_price = seds_price.loc[seds_price['year']==str(year)]

        if 'COAL' in series_id:
            seds_price.loc[:, 'price_usd_per_mmbu'] = seds_price.price_usd_per_mmbtu / 21.449
            seds_price.loc[:, 'fuel_type'] = 'COAL'

        elif 'PEICD' in series_id:
            seds_price['fuel_type'] = 'MISC'

        seds_price = seds_price[['fuel_type', 'price_usd_per_mmbtu']]

        return seds_price

    def get_bea_gdp(self, calculation_years, industry_id):
        """
        Use BEA API to get annual real GDP by sector.

        Parameters
        ----------
        calculation_years : list of int
            Indicate starting and ending years of GDP data 
            to get.

        industry_id : str
            First 2-digits of industry NAICS code (e.g., '21' 
            for mining).

        Returns
        -------
        bea : pandas.DataFrame


        """

        base_url = 'https://apps.bea.gov/api/data'
        params = self._bea_api_params
        params['IndustryID'] = industry_id

        r = requests.get(base_url, params=params)

        bea = pd.DataFrame(
            r.json()['BEAAPI']['Results']['Data'],
            columns=['GeoName', 'TimePeriod', 'DataValue']
            )

        bea.rename(columns={
            'GeoName': 'state',
            'TimePeriod': 'year',
            'DataValue': 'gdp'}, inplace=True
            )

        invalid = '(D)'
        bea = bea.replace(invalid, bea.replace([invalid], None))
        bea = bea[(bea.gdp != '(NA)') & (bea.gdp != '(L)')]

        bea['gdp'] = bea['gdp'].apply(lambda x: x.replace(',',"")).astype(float)
        bea = bea.astype({'year': int})

        bea = bea[bea.year.between(
            calculation_years[0], calculation_years[-1]
            )]

        return bea

    def calc_energy_multiplier(self, bea, base_year):
        """
        Create a GDP multiplier for scaling non-mfg energy
        use from a base year (base year is the year of
        original energy data, typically an economic or 
        agricultural census year).

        Parameters
        ----------
        bea : pandas.DataFrame
            BEA state-level GDP data for relevant years

        base_year : int

        Returns
        -------
        multiplier : pandas.DataFrame
        """

        multiplier = bea.pivot(index='state', columns='year', values='gdp')

        #multiplier['base_year_2012'] = multiplier[2012]

        #years = range(1997,2018)
        #for y in years:
        #    multiplier[y] = multiplier[y] / multiplier['base_year_2012']
            
        multiplier.loc[:, [y for y in bea.year.unique()]] = \
            multiplier[[y for y in bea.year.unique()]].divide(
                multiplier[base_year], axis=0
                )

        multiplier.reset_index(inplace=True)                                     
        #multiplier = multiplier.drop('base_year_2012', axis=1)                     
        multiplier['state'] = multiplier['state'].str.upper()

        multiplier.set_index('state',inplace=True)
        multiplier = multiplier.drop(
            ['FAR WEST', 'GREAT LAKES', 'MIDEAST', 
             'NEW ENGLAND', 'PLAINS', 'ROCKY MOUNTAIN',
             'SOUTHEAST', 'SOUTHWEST', 'UNITED STATES']
             )

        multiplier.reset_index(inplace=True) 

        return multiplier


    def scale_energy(self, multiplier, county_energy):
        """
        Scale county energy estimates based on GDP multiplier.

        Parameters
        ----------
        multiplier : pandas.DataFrame

        county_energy : pandas.DataFrame

        Returns
        -------
        county_energy : pandas.DataFrame

        
        """

        county_energy = pd.merge(
            county_energy, multiplier, on='state', how='outer'
            )

        county_energy.loc[:, [y for y in multiplier.columns]] =\
                county_energy[[y for y in multiplier.columns]].multiply(
                        county_energy.fuel_county_mmbtu, axis=0
                        )
            
        county_energy = county_energy.drop('fuel_county_mmbtu', axis=1)

        return county_energy