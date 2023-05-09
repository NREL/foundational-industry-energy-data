#!/usr/bin/python
import pandas as pd
import requests
import urllib
import json
import get_cbp
import datetime as dt
import numpy as np

"""
Estimate county-level fuel consumption in the mining sector 
"""


calculation_years = range(2010, 2017)

# 1.NATIONAL FUEL CONSUMPTION #################################################
# (1.1) fuel_cost: 2012 Costs (000$) by NAICS & Fuel ##########################
"""
fuel_cost_source: 2012 Economic Census data from ‘Mining: Subject Series:
Materials Summary: Selected Supplies, Minerals Received for Preparation,
Purchased Machinery, and Fuels Consumed by Type of Industry: 2012.’
Variables: https://api.census.gov/data/2012/ecnmatfuel/variables.html.
Columns: NAICS, fuel_id, fuel_type, fuel_cost

fuel_cost: National mining-sector fuel costs by fuel type. Columns: NAICS, 
fuel_id, fuel_type (only crude, coal, diesel, residual, natural gas, gasoline,
and misc), fuel_cost_k_usd.
"""

####### Collect data from Census

def get_fuel_cost():
    
    base_url = 'https://api.census.gov/data/2012/ecnmatfuel'
    
    params = {
        'get':
        'NAICS2012,MATFUEL,MATFUEL_TTL,MATFUELCOST,MATFUELCOST_F,MATFUELQTY,UNITS_TTL,M_FI',
        'for':'us', 'NAICS2012':'21*',
        'key':'489f08f390013bc6d41ee377e86ea8c1b0dd5267'
        }
    
    r = requests.get(base_url, params=params)
    #print(r.content)
#    url = r.url
    #print(url)
    
#    response = urllib.request.urlopen(url)
#    data = response.read()
#    datajson = json.loads(data)
    fuel_cost = pd.DataFrame(r.json()) 
    
    fuel_cost.columns = fuel_cost.iloc[0]
    fuel_cost = fuel_cost[1:]

    fuel_cost.rename(columns={'NAICS2012':'NAICS', 'MATFUEL':'fuel_id', 
                              'MATFUEL_TTL':'fuel_type', 
                              'MATFUELCOST':'fuel_cost_k_usd',
                              'MATFUELCOST_F':'fuel_cost_missing',
                              'MATFUELQTY':'fuel_qty',
                              'UNITS_TTL':'fuel_qty_unit',
                              'M_FI':'fuel_flag'}, inplace=True)
    
    fuel_cost = fuel_cost[['NAICS', 'fuel_id','fuel_type',
                                   'fuel_cost_k_usd', 'fuel_cost_missing',
                                   'fuel_qty','fuel_qty_unit','fuel_flag']]
    
    fuel_cost = fuel_cost.loc[:,~fuel_cost.columns.duplicated()]
    
    # Remove material costs, keep only fuel costs:
    fuel_cost = fuel_cost[fuel_cost.fuel_flag == 'F']
    fuel_cost = fuel_cost.drop('fuel_flag', axis=1)
    
    return fuel_cost


mining_naics = [211111,211112,212111,212112,212113,212210,212221,212222,212231,
                212234,212291,212299,212311,212312,212313,212319,212321,212322,
                212324,212325,212391,212392,212393,212399,213111,213112,213113,
                213114,213115]

fuel_cost_source = get_fuel_cost()


####### Change data type
fuel_cost_source['NAICS'] = fuel_cost_source['NAICS'].astype(int)

fuel_cost_source['fuel_id'] = fuel_cost_source['fuel_id'].astype(int)

fuel_cost_source['fuel_cost_k_usd'] = fuel_cost_source[
                                      'fuel_cost_k_usd'].astype(int)

fuel_cost_source['fuel_qty'] = fuel_cost_source['fuel_qty'].astype(float)

#fuel_cost_source.set_index('NAICS', inplace=True)                              # Only for test
#fuel_cost_source.to_csv('mining_fuel_cost_source.csv')                         # Only for test
#print(fuel_cost_source.head(20))


####### Fill out missing data in column fuel_cost_k_usd
#fuel_cost_source = pd.read_csv('mining_fuel_cost_source.csv')                  # Only for test

fuel_cost_source.set_index('NAICS', inplace=True)

fuel_cost = pd.DataFrame()

for n in mining_naics:

    single_sector = fuel_cost_source.loc[n, : ]
    single_sector = single_sector[single_sector.fuel_cost_missing != 'X']

    if single_sector.iloc[0]['fuel_cost_k_usd'] != 0:
        
        tot = single_sector.iloc[0]['fuel_cost_k_usd']
        cost_sum = single_sector.iloc[1:]['fuel_cost_k_usd'].sum()
        missing_value_sum = tot - cost_sum
    
        missing_value_counts = single_sector[single_sector[
                              'fuel_cost_k_usd']==0]['fuel_cost_k_usd'].count()
    
        missing_value = missing_value_sum / missing_value_counts
    
        single_sector['fuel_cost_k_usd'] = \
                     single_sector['fuel_cost_k_usd'].replace(0, missing_value)
    
    else:  
        single_sector = single_sector[single_sector.fuel_cost_missing!='D']
    
    single_sector = single_sector[['fuel_id','fuel_type','fuel_cost_k_usd']]

    fuel_cost = pd.concat([fuel_cost, single_sector])

fuel_cost = fuel_cost.reset_index()
pd.set_option('display.max_columns', None)


####### Rename fuels
fuel_type_dict = {'fuel_id':[2,960018,974000,21111003,21111015,21111101,
                             21211003,32411015,32411017,32411019],
                  'fuel_type':['TOTAL','MISC','UNDISTRIBUTED','NATURAL GAS',
                               'NATURAL GAS','CRUDE OIL','COAL','GASOLINE',
                               'DIESEL','RESIDUAL FUEL OIL']}
                  
fuel_type = pd.DataFrame(fuel_type_dict)

fuel_cost = fuel_cost.drop('fuel_type', axis=1)
fuel_cost = pd.merge(fuel_cost, fuel_type, on='fuel_id', how='outer')

fuel_cost.set_index(['NAICS','fuel_id'], inplace=True)
fuel_cost = fuel_cost.sort_index().reset_index()
fuel_cost = fuel_cost[['NAICS','fuel_id','fuel_type','fuel_cost_k_usd']]



####### Remove TOTAL and UNDISTRIBUTED from fuel_cost
fuel_cost = fuel_cost.loc[(fuel_cost['fuel_type']!='TOTAL') & 
                          (fuel_cost['fuel_type']!='UNDISTRIBUTED')]

#fuel_cost.set_index('NAICS', inplace=True)                                     # Only for test
#fuel_cost.to_csv('mining_fuel_cost.csv')                                       # Only for test
#print(fuel_cost.head(20))



# (1.2) fuel_price ($/mmbtu) ##################################################
"""
Country-level fuel price. Columns: fuel_type, fuel_price
"""

####### price_coal($/short ton to $/mmbtu)
base_url = 'http://api.eia.gov/series/'

params = {'api_key': 'fb1b162b14d1e65ca506cf0bdf0fe173',
          'series_id': 'COAL.PRICE_BY_RANK.US-TOT.A'}

r = requests.get(base_url, params=params)
url = r.url

response = urllib.request.urlopen(url)
info = response.read()
data = json.loads(info)
price_coal = pd.DataFrame(data['series'][0]['data'], 
                          columns=['year','price_usd_per_short_ton'])

price_coal = price_coal.loc[price_coal['year']=='2012']

price_coal['price_usd_per_mmbtu'] = \
                                 price_coal['price_usd_per_short_ton'] / 21.449 # EIA MER Table A5

price_coal['fuel_type'] = 'COAL'
price_coal = price_coal[['fuel_type', 'price_usd_per_mmbtu']]




####### price_diesel ($/gal to $/mmbtu)
#Source: EIA MER Energy Prices Table 9.4 on-highway diesel fuel
price_diesel = pd.read_csv(
             'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T09.04')

price_diesel = price_diesel.loc[
           (price_diesel['MSN']=='DFONUUS') & (price_diesel['YYYYMM']==201213)] # 2012 On-Highway Diesel Fuel Price (Dollars per Gallon Including Taxes)

price_diesel.rename(columns = {'Value':'price_usd_per_gal'}, inplace=True)

price_diesel = price_diesel.astype({'price_usd_per_gal': float})

price_diesel['price_usd_per_mmbtu'] = \
                                 price_diesel['price_usd_per_gal'] * 42 / 5.774 # EIA MER Table A3

price_diesel = price_diesel[['price_usd_per_mmbtu']]
price_diesel['fuel_type'] = 'DIESEL'
price_diesel = price_diesel[['fuel_type','price_usd_per_mmbtu']]




####### price_residual ($/gal to $/mmbtu)
#Source: EIA MER Energy Prices Table 9.6 No.2 fuel oil
price_residual = pd.read_csv(
             'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T09.06')

price_residual = price_residual.loc[
       (price_residual['MSN']=='D2WHUUS') & (price_residual['YYYYMM']==201213)] # 2012 Refiner Price of No. 2 Fuel Oil for Resale (Dollars per Gallon Including Taxes)

price_residual.rename(columns = {'Value':'price_usd_per_gal'}, inplace=True)

price_residual = price_residual.astype({'price_usd_per_gal': float})

price_residual['price_usd_per_mmbtu'] = \
                               price_residual['price_usd_per_gal'] * 42 / 6.287 # EIA MER Table A3

price_residual = price_residual[['price_usd_per_mmbtu']]
price_residual['fuel_type'] = 'RESIDUAL FUEL OIL'
price_residual = price_residual[['fuel_type','price_usd_per_mmbtu']]




####### price_ng ($/000cf to $/mmbtu)
#Source: EIA MER Energy Prices Table 9.10 city-gate price
price_ng = pd.read_csv(
             'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T09.10')

price_ng = price_ng.loc[
                   (price_ng['MSN']=='NGCGUUS') & (price_ng['YYYYMM']==201213)] # 2012 Natural Gas Price, Citygate (Dollars per Thousand Cubic Feet)

price_ng.rename(columns = {'Value':'price_usd_per_kcf'}, inplace=True)

price_ng = price_ng.astype({'price_usd_per_kcf': float})

price_ng['price_usd_per_mmbtu'] = \
                          price_ng['price_usd_per_kcf'] / 1000 / 1024 * 1000000 # https://www.eia.gov/tools/faqs/faq.php?id=45&t=8

price_ng = price_ng[['price_usd_per_mmbtu']]
price_ng['fuel_type'] = 'NATURAL GAS'
price_ng = price_ng[['fuel_type','price_usd_per_mmbtu']]




####### price_gasoline ($/gal to $/mmbtu)
#Source: EIA MER Energy Prices Table 9.4 regular motor gasoline - all areas
price_gasoline = pd.read_csv(
             'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T09.04')

price_gasoline = price_gasoline.loc[
       (price_gasoline['MSN']=='RMRTUUS') & (price_gasoline['YYYYMM']==201213)] # 2012 Regular Motor Gasoline, All Areas, Retail Price (Dollars per Gallon Including Taxes)

price_gasoline.rename(columns = {'Value':'price_usd_per_gal'}, inplace=True)

price_gasoline = price_gasoline.astype({'price_usd_per_gal': float})

price_gasoline['price_usd_per_mmbtu'] = \
                               price_gasoline['price_usd_per_gal'] * 42 / 5.063 # EIA MER Table A3

price_gasoline = price_gasoline[['price_usd_per_mmbtu']]
price_gasoline['fuel_type'] = 'GASOLINE'
price_gasoline = price_gasoline[['fuel_type','price_usd_per_mmbtu']]




####### price_crude ($/barrel to $/mmbtu)
#Source: EIA MER Energy Prices Table 9.1 refiner acquisition cost: composite
price_crude = pd.read_csv(
             'https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T09.01')

price_crude = price_crude.loc[
             (price_crude['MSN']=='CODPUUS') & (price_crude['YYYYMM']==201213)] # 2012 Crude Oil Domestic First Purchase Price (Dollars per Barrel)

price_crude.rename(columns = {'Value':'price_usd_per_barrel'}, inplace=True)

price_crude = price_crude.astype({'price_usd_per_barrel': float})

price_crude['price_usd_per_mmbtu'] = \
                                      price_crude['price_usd_per_barrel'] / 5.8 # EIA MER Table A3

price_crude = price_crude[['price_usd_per_mmbtu']]
price_crude['fuel_type'] = 'CRUDE OIL'
price_crude = price_crude[['fuel_type','price_usd_per_mmbtu']]




####### price_misc ($/mmbtu)
#Source: EIA SEDS Table ET5 industrial sector primary energy price: total
base_url = 'http://api.eia.gov/series/'

params = {'api_key': 'fb1b162b14d1e65ca506cf0bdf0fe173',
          'series_id': 'SEDS.PEICD.US.A'}

r = requests.get(base_url, params=params)
url = r.url

response = urllib.request.urlopen(url)
info = response.read()
data = json.loads(info)
price_misc = pd.DataFrame(data['series'][0]['data'], 
                          columns=['year','price_usd_per_mmbtu'])

price_misc = price_misc.loc[price_misc['year']=='2012']
price_misc['fuel_type'] = 'MISC'
price_misc = price_misc[['fuel_type', 'price_usd_per_mmbtu']]


####### fuel_price
fuel_price = pd.concat([price_coal, price_diesel, price_residual, price_ng,
                        price_gasoline, price_crude, price_misc])

#fuel_price.set_index('fuel_type', inplace=True)                                # Only for test
#fuel_price.to_csv('mining_fuel_price.csv')                                     # Only for test
#print(fuel_price)



# （1.3） fuel_nation (mmbtu) ###################################################
"""
fuel_nation: country-level fuel consumption. Columns: NAICS, fuel_type, 
fuel_nation_mmbtu
"""

####### fuel_use (mmbtu) = fuel_cost (000$) / fuel_price ($/mmbtu)
#fuel_cost = pd.read_csv('mining_fuel_cost.csv')                                # Only for test
#fuel_price = pd.read_csv('mining_fuel_price.csv')                              # Only for test

fuel_nation = pd.merge(fuel_cost, fuel_price, on='fuel_type', how='outer')

fuel_nation['fuel_nation_mmbtu'] = \
     fuel_nation['fuel_cost_k_usd'] * 1000 / fuel_nation['price_usd_per_mmbtu']
     
fuel_nation = fuel_nation[['NAICS', 'fuel_type', 'fuel_nation_mmbtu']]

fuel_nation = fuel_nation.dropna()



####### Add ng self-used and crude oil to fuel_use
#fuel_cost_source = pd.read_csv('mining_fuel_cost_source.csv')                  # Only for test

fuel_cost_source.reset_index(inplace=True)

fuel_nation_added = fuel_cost_source[fuel_cost_source.fuel_cost_missing =='X']

fuel_nation_added = fuel_nation_added.drop('fuel_type', axis=1)

fuel_nation_added = pd.merge(fuel_nation_added, fuel_type, on='fuel_id',
                             how='left')

# ng self-used (billion cf to mmbtu)
fuel_nation_added_ng = fuel_nation_added[fuel_nation_added.fuel_id == 21111003]

fuel_nation_added_ng['fuel_nation_mmbtu'] = \
                                fuel_nation_added_ng['fuel_qty'] * 1000 * 1024 # EIA MER Table A4

fuel_nation_added_ng = fuel_nation_added_ng[['NAICS','fuel_type',
                                             'fuel_nation_mmbtu']]

fuel_nation = pd.concat([fuel_nation, fuel_nation_added_ng])

# crude oil (million barrels to mmbtu)
fuel_nation_added_crude = pd.DataFrame(
        fuel_nation_added[fuel_nation_added.fuel_id == 21111101]
        )

fuel_nation_added_crude['fuel_nation_mmbtu'] = \
                            fuel_nation_added_crude['fuel_qty'] * 1000000 * 5.8 # EIA MER Table A2

fuel_nation_added_crude = fuel_nation_added_crude[['NAICS','fuel_type',
                                                   'fuel_nation_mmbtu']]

fuel_nation = pd.concat([fuel_nation, fuel_nation_added_crude])

fuel_nation = fuel_nation.groupby(['NAICS','fuel_type']).sum()

#fuel_nation.to_csv('../results/mining_fuel_nation.csv')                                   # Only for test

fuel_nation.reset_index(inplace=True)



# 2.NATIONAL ELECTRICITY CONSUMPTION ##########################################
# census_data: 2012 Economic Census ###########################################
"""
census_data: Columns: NAICS, fuel_type, elec_nation_mwh, estab_counts. 
Variables: https://api.census.gov/data/2012/ecnbasic/variables.html.

elec_nation: Country-level electricity use by NAICS code. Columns: NAICS,
fuel_type, fuel_nation_mmbtu.

fuel_nation: country-level fuel consumption by NAICS. Columns: NAICS, 
fuel_type, fuel_nation_mmbtu.
"""

def get_census_data():
    
    base_url = 'http://api.census.gov/data/2012/ecnbasic'

    params = {'get':'ESTAB,ELECPCH',
              'for':'us',
              'NAICS2012':'21*',
              'key':'489f08f390013bc6d41ee377e86ea8c1b0dd5267'}

    r = requests.get(base_url, params=params)
    #print(r.content)
#    url = r.url
    #print(url)
    
#    response = urllib.request.urlopen(url)
#    data = response.read()
#    datajson = json.loads(data)
    census_data = pd.DataFrame(r.json()) 
    
    census_data.columns = census_data.iloc[0]
    census_data = census_data[1:]
    
    census_data.rename(columns = {'ESTAB':'estab_counts',
                             'NAICS2012':'NAICS', 
                             'ELECPCH':'elec_nation_mwh'}, 
                             inplace=True)
    
    census_data['n_naics'] = census_data.NAICS.apply(
            lambda x: len(str(x))
            )
    
    census_data = census_data[census_data.n_naics == 6]

    census_data.drop('n_naics', axis=1, inplace=True)
    
    census_data['estab_counts'] = census_data['estab_counts'].astype(int)
    census_data['elec_nation_mwh'] = \
        census_data['elec_nation_mwh'].astype(int)
    
    # NAICS 211111 has two rows, one of them has 0 electricity consumption.
    if census_data.shape[0] > 1:
        census_data = census_data[census_data.elec_nation_mwh != 0]
        
    census_data['fuel_type'] = 'ELECTRICITY'
    
    census_data = census_data[
            ['NAICS','fuel_type','elec_nation_mwh','estab_counts']
            ]

    return census_data

census_data = get_census_data()
    

####### Fill out missing elec use data
#missing_elec = census_data[census_data.elec_nation_mwh == 0]
#
#census212 = census_data(212)
#mwh_per_estab = census212.iloc[0]['elec_nation_mwh'] / census212.iloc[0][
#                                                       'estab_counts']
#
#missing_elec['elec_nation_mwh'] = missing_elec['estab_counts'] * mwh_per_estab
#
#census_data = census_data[census_data.elec_nation_mwh != 0]
#census_data = pd.concat([census_data, missing_elec])



####### Convert mwh to mmbtu
elec_nation = census_data[['NAICS', 'fuel_type', 'elec_nation_mwh']]
elec_nation['fuel_nation_mmbtu'] = elec_nation['elec_nation_mwh'] /1000*3412.14 # From Colin's Excel Model. Source needed. 
elec_nation = elec_nation[['NAICS','fuel_type','fuel_nation_mmbtu']]



####### Combine fuel_nation with elec_nation & Add estab_counts
fuel_nation = pd.concat([fuel_nation, elec_nation])
#print(fuel_nation)

#fuel_nation.set_index('NAICS', inplace=True)                                   # Only for test
#fuel_nation.to_csv('mining_fuel_nation.csv')                                   # Only for test




# 3. COUNTY FUEL & ELECTRICITY CONSUMPTION ####################################
# (3.1) intensity: Energy intensities per establishment #######################
"""
estab_counts: Columns: NAICS, estab_counts
intensity: fuel consumption (mmbtu) / establishment counts. Columns: NAICS, 
fuel_type, mmbtu_per_estab.
"""
#fuel_nation = pd.read_csv('mining_fuel_nation.csv')                            # Only for test

estab_counts = census_data[['NAICS', 'estab_counts']]

fuel_nation['NAICS'] = fuel_nation['NAICS'].astype(int)
estab_counts['NAICS'] = estab_counts['NAICS'].astype(int)

intensity = pd.merge(fuel_nation, estab_counts, on='NAICS', how='outer')

intensity['mmbtu_per_estab'] = \
                     intensity['fuel_nation_mmbtu'] / intensity['estab_counts']
                     
intensity = intensity[['NAICS', 'fuel_type', 'mmbtu_per_estab']]



# (3.2)cbp: Establishment Counts in each county ###############################
"""
cbp_source: 2012 establishment counts by NAICS code and county. Columns: 
fipstate, fipscty, naics, empflag, emp_nf, emp, qp1_nf, qp1, ap_nf, ap, est, 
n1_4, n5_9, n10_19, n20_49, n50_99, n100_249, n250_499, n500_999, n1000, 
n1000_1, n1000_2, n1000_3, n1000_4, censtate, cencty, COUNTY_FIPS, region, 
Under 50, naics_n, industry.

cbp: state, state_abbr, county, NAICS, est.
"""

#cbp_source = get_cbp.CBP(2012)
#cbp = cbp_source.cbp
#
#cbp = cbp[['fipstate', 'COUNTY_FIPS', 'naics', 'est']]
#
#
######### Only keep mining-sector NAICS codes
#cbp.set_index('naics', inplace=True)
#cbp = cbp.loc[mining_naics].reset_index()
#cbp.rename(columns = {'naics':'NAICS'}, 
#                      inplace=True)

    
####### Add state names and county names
fips = pd.read_csv('../calculation_data/input_us_fips_codes.csv')
fips = fips[['State', 'County_Name', 'FIPS State', 'COUNTY_FIPS']]
fips.rename(columns = {'State':'state', 
                       'County_Name':'county',
                       'FIPS State':'fipstate'},
                       inplace=True)
#
#cbp = pd.merge(cbp, fips, on=['fipstate', 'COUNTY_FIPS'], how='outer')
#
#cbp = cbp[['state','county','NAICS','est', 'fipstate', 'COUNTY_FIPS']]
#cbp = cbp.dropna()
#cbp.set_index('state',inplace=True)
#
#cbp['NAICS'] = cbp['NAICS'].astype(int)
#cbp['est'] = cbp['est'].astype(int)
#cbp.reset_index(inplace=True)
#
#cbp['state'] = cbp['state'].str.upper()
#cbp['county'] = cbp['county'].str.upper()


####### Add state_abbr
#state_abbr = pd.read_csv('../calculation_data/input_region.csv')
#state_abbr = state_abbr[['state', 'state_abbr']]
#cbp = pd.merge(cbp, state_abbr, on='state', how='outer')
#cbp = cbp[['state', 'state_abbr', 'county', 'NAICS', 'est',
#           'fipstate', 'COUNTY_FIPS']]


# Import corrected cbp
cbp_corrected = pd.read_csv(
        '../calculation_data/cbp_corrected_mining.csv', index_col=0,
        dtype={'naics':int, 'COUNTY_FIPS':int}
        )

cbp_corrected.rename(columns={'naics': 'NAICS'}, inplace=True)

cbp_corrected = pd.merge(cbp_corrected, fips,
                         on=['fipstate', 'COUNTY_FIPS'], how='left')

cbp_corrected['state'] = cbp_corrected['state'].str.upper()

cbp_corrected['county'] = cbp_corrected['county'].str.upper()

# (3.3) fuel_county ###########################################################
"""
fuel_county: Merge cbp and intensity. Columns: state, state_abbr, county, 
NAICS, fuel_type, fuel_county_mmbtu.
"""
fuel_county = pd.merge(cbp_corrected, intensity, on='NAICS', how='outer')

fuel_county['fuel_county_mmbtu'] = \
                            fuel_county['est'] * fuel_county['mmbtu_per_estab']
                            
fuel_county['NAICS'] = fuel_county['NAICS'].astype(int)

fuel_county = fuel_county[['state', 'county', 'NAICS', 
                           'fipstate', 'COUNTY_FIPS', 'fuel_type',
                           'fuel_county_mmbtu']]

#print(fuel_county.head(30))
#fuel_county.set_index('state',inplace=True)                                    # Only for test
#fuel_county.to_csv('mining_fuel_county.csv')                                   # Only for test






# 4.REAL GDP ##################################################################
# (4.1)Source Data from BEA ###################################################
"""
bea: 1997-2018 annual GDP in the mining sector by state (real GDP in 
chained dollars). Columns: state, year, gdp
"""

base_url = 'https://apps.bea.gov/api/data'

params = {'UserID':'30E3AEAC-AB9E-4368-B150-1E347556C91A',
          'method':'GetData',
          'datasetname':'RegionalProduct',
          'Component':'RGDP_SAN',                                              # RGDP_SAN: State annual naics, real GDP in chained dollars
          'IndustryId':'21',                                                  # 21: Mining sector
          'Year':'ALL',
          'GeoFips':'STATE',
          'ResultFormat':'JSON'}

r = requests.get(base_url, params=params)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
bea = pd.DataFrame(datajson['BEAAPI']['Results']['Data'],
                   columns = ['GeoName',
                              'TimePeriod',
                              'DataValue'])

####### Rename columns & Remove invalid values
bea.rename(columns = {'GeoName':'state', 
                      'TimePeriod':'year', 
                      'DataValue':'gdp'}, 
                      inplace=True)
invalid = '(D)'
bea = bea.replace(invalid, bea.replace([invalid], None))
bea = bea[(bea.gdp != '(NA)') & (bea.gdp != '(L)')]


####### Remove commas in numbers & Change data types
bea['gdp'] = bea['gdp'].apply(lambda x: x.replace(',',"")).astype(float)
bea = bea.astype({'year': int})

bea = bea[bea.year.between(calculation_years[0], calculation_years[-1])]


# (4.2)GDP Growth Rate as A Multiplier ########################################
"""
multiplier: Index: state. Columns: 1997, 1998, 1999 ... 2018.
"""
multiplier = bea.pivot(index='state', columns='year', values='gdp')

base_year = 2012
#multiplier['base_year_2012'] = multiplier[2012]

#years = range(1997,2018)
#for y in years:
#    multiplier[y] = multiplier[y] / multiplier['base_year_2012']
    
multiplier.loc[:, [y for y in calculation_years]] = \
    multiplier[[y for y in calculation_years]].divide(
            multiplier[base_year], axis=0
            )


multiplier.reset_index(inplace=True)                                           
#multiplier = multiplier.drop('base_year_2012', axis=1)                     
multiplier['state'] = multiplier['state'].str.upper()

multiplier.set_index('state',inplace=True)
multiplier = multiplier.drop(['FAR WEST', 'GREAT LAKES', 'MIDEAST', 
                              'NEW ENGLAND', 'PLAINS', 'ROCKY MOUNTAIN',
                              'SOUTHEAST', 'SOUTHWEST', 'UNITED STATES'])
multiplier.reset_index(inplace=True) 
    
#print(multiplier.head(20))


# 5. MULTIYEAR COUNTY LEVEL ENERGY CONSUMPTION ################################
fuel_county = pd.merge(fuel_county, multiplier, on='state', how='outer')

#years = range(1997, 2018)
#for y in years:
#    fuel_county[y] = fuel_county[y] * fuel_county['fuel_county_mmbtu']
#    fuel_county = fuel_county.rename(columns={y: str(y)+'_fuel_county_mmbtu'})
    
fuel_county.loc[:, [y for y in calculation_years]] =\
        fuel_county[[y for y in calculation_years]].multiply(
                fuel_county.fuel_county_mmbtu, axis=0
                )
    
fuel_county = fuel_county.drop('fuel_county_mmbtu', axis=1)

#fuel_county.set_index('state', inplace=True)

filename = 'mining_county_energy_'+ \
    dt.datetime.now().strftime('%Y%m%d_%H%M')+'.csv'

fuel_county.to_csv('../results/'+filename, index=False)

#print(fuel_county.head(10))
