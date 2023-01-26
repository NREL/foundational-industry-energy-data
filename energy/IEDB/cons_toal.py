import pandas as pd
import requests
import json
import os
import get_cbp


"""
Estimate 2000-2017 county-level fuel consumption in the construction sector
based on ...
"""

# 1.ECONOMIC CENSUS DATA ######################################################
# census: 2012 economic census ################################################
"""
Variables: https://api.census.gov/data/2012/ewks/variables.html
Columns: state, state_abbr, state_code, NAICS, estab_counts, cost_diesel,
cost_ng, cost_other, cost_elec, cost_materials, cost_fuels.
"""

def census(naics):

    base_url = 'http://api.census.gov/data/2012/ecnbasic'

    params = {'get':'ESTAB,CSTFUGT,CSTFUNG,CSTFUOT,CSTELEC,CSTMPRT,CSTEFT',
              'for':'state:*',
              'NAICS2012':naics,
              'key':'489f08f390013bc6d41ee377e86ea8c1b0dd5267'}

    r = requests.get(base_url, params=params)
    #print(r.content)
#    url = r.url
#    #print(url)
#
#    response = urllib.request.urlopen(url)
#    data = response.read()
#    datajson = json.loads(data)
    census = pd.DataFrame(r.json())

    census.columns = census.iloc[0]
    census = census[1:]

    census.rename(columns={
        'state':'state_code',
        'NAICS2012':'NAICS',
        'ESTAB':'estab_counts',
        'CSTFUGT': 'cost_diesel',
        'CSTFUNG': 'cost_ng',
        'CSTFUOT':'cost_other',
        'CSTELEC':'cost_elec',
        'CSTMPRT':'cost_materials',
        'CSTEFT':'cost_fuels'
        }, inplace=True)

    census['state_code'] = census['state_code'].astype(int)

    code_file = pd.read_csv(
        os.path.abspath('./energy/IEDB/calculation_data/input_region.csv')
        )
    code = code_file[['state', 'state_abbr', 'state_code']]
    code = code.append({'state':'DISTRICT OF COLUMBIA',
                        'state_abbr': 'DC',
                        'state_code':'11'},
                        ignore_index=True)
    code['state_code'] = code['state_code'].astype(int)

    census = pd.merge(census, code, on='state_code', how='outer')
    census = census[['state','state_abbr','state_code','NAICS','estab_counts',
                     'cost_diesel','cost_ng','cost_other','cost_elec',
                     'cost_materials','cost_fuels']]

    return census

#census_data = pd.concat(
#              [census(23), census(236), census(237), census(238)],
#              ignore_index=True)
##print(census_data)


####### Fill in missing data for DE, DC & WV
def fill_in_missing_data(census_data, selected_state):

    missing = census_data.loc[census_data['state_abbr']== selected_state]
    missing = missing.sort_index()
    missing.set_index('state',inplace=True)
    missing = missing.apply(pd.to_numeric, errors='ignore')

    missing_two = missing.loc[missing['NAICS'] == 23]

    missing_three = missing.loc[missing['NAICS'] != 23]
    missing_three['cost_frac'] = missing_three['estab_counts'].divide(
                              missing_three['estab_counts'].sum(level='state'))

    new = pd.concat([missing_two, missing_three])
    new.reset_index(level=0,  inplace=True)
    missing.reset_index(level=0, inplace=True)

    x = new.iloc[0]['cost_ng']
    new['cost_ng'] =  x * new['cost_frac']

    y = new.iloc[0]['cost_other']
    new['cost_other'] = y * new['cost_frac']

    if selected_state == 'WV':
        new = new.loc[new['NAICS'] > 236]
        missing = missing[missing.NAICS < 237]
        new = pd.concat([missing, new])
    else:
        z = new.iloc[0]['cost_elec']
        new['cost_elec'] = z * new['cost_frac']
        new = new.loc[new['NAICS'] > 23]
        missing = missing[missing.NAICS < 236]
        new = pd.concat([missing, new])

    new = new[['state','state_abbr', 'state_code', 'NAICS', 'estab_counts',
               'cost_diesel', 'cost_ng', 'cost_other', 'cost_elec',
               'cost_materials', 'cost_fuels']]
    return new

#DE = fill_in_missing_data('DE')
#DC = fill_in_missing_data('DC')
#WV = fill_in_missing_data('WV')
#
#census_data = census_data[(census_data.state_abbr != 'DE') &
#                          (census_data.state_abbr != 'DC') &
#                          (census_data.state_abbr != 'WV')]
#
#census_data = pd.concat([census_data, DE, DC, WV])
#census_data.set_index('state', inplace=True)
#census_data = census_data.sort_index().reset_index()
#
#pd.set_option('display.max_rows', None)
#census_data = census_data.apply(pd.to_numeric, errors = 'ignore')
#print(census_data)



# 2.STATE-LEVEL FUEL USE ######################################################
# (2.1)State Diesel Consumption ###############################################
def calc_diesel_state(census_data):
    """
    cost_diesel (000 $): 2012 Economic Census data. Columns: state, state_abbr,
    NAICS, cost_diesel_k_usd.

    price_diesel ($/gal):  2012 EIA region-level on-highway diesel price data.
    Columns: state, price_usd_per_gal.

    diesel_state (mmbtu): = cost_diesel / price_diesel. Columns: state,
    state_abbr, NAICS, fuel_type, fuel_state_mmbtu.
    """

    ####### cost_diesel (000 $)
    cost_diesel = pd.DataFrame(
        census_data[['state','state_abbr', 'NAICS', 'cost_diesel']]
        )

    cost_diesel.rename(columns={'cost_diesel': 'cost_diesel_k_usd'},
                       inplace=True)

    ####### price_diesel ($/gal)
    price_diesel_region = pd.read_excel(
            'https://www.eia.gov/petroleum/gasdiesel/xls/psw18vwall.xls',
            sheet_name='Data 2', header=2)

    price_diesel_region.columns = ['Date', 'US','EAST COAST', 'NEW ENGLAND',
                                   'CENTRAL ATLANTIC','LOWER ATLANTIC',
                                   'MIDWEST','GULF COAST','ROCKY MOUNTAIN',
                                   'WEST COAST','CALIFORNIA',
                                   'WEST COAST EXCEPT CALIFORNIA']

    price_diesel_region = price_diesel_region.drop(
        ['US', 'EAST COAST', 'WEST COAST'], axis=1
        )

    #Only keep 2012 data
    price_diesel_region = price_diesel_region.loc[
                          price_diesel_region['Date'].dt.year==2012
                          ]

    price_diesel_region.reset_index(level=0, inplace=True)
    price_diesel_region = price_diesel_region.drop(['index','Date'], axis=1)

    #Transpose the dataframe
    price_diesel_region = price_diesel_region.transpose()
    price_diesel_region.index.name = 'diesel_padd'

    #Calculate annual average diesel prices
    price_diesel_region['price_usd_per_gal'] = price_diesel_region.mean(axis=1)
    price_diesel_region = price_diesel_region[['price_usd_per_gal']]

    #Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state','diesel_padd']]
    region = region.append({'state':'DISTRICT OF COLUMBIA',
                            'diesel_padd':'CENTRAL ATLANTIC'},
                            ignore_index=True)

    price_diesel = pd.merge(
                   price_diesel_region, region, on='diesel_padd', how='outer')

    price_diesel.set_index('state', inplace=True)
    price_diesel= price_diesel.drop('diesel_padd', axis=1)
    price_diesel = price_diesel.sort_index().reset_index()

    ####### diesel_state (mmbtu)
    diesel_state = pd.merge(cost_diesel, price_diesel, on='state', how='outer')

    diesel_state['fuel_state_gal'] = \
           diesel_state['cost_diesel_k_usd']*1000/\
           diesel_state['price_usd_per_gal']

    diesel_state['fuel_state_mmbtu'] = \
        diesel_state['fuel_state_gal'] / 42 * 5.774   # https://www.eia.gov/totalenergy/data/monthly/pdf/sec13.pdf  (A3)

    diesel_state['fuel_type'] = 'DIESEL'

    diesel_state = diesel_state[['state','state_abbr','NAICS', 'fuel_type',
                                 'fuel_state_mmbtu']]

    return diesel_state
#print(diesel_state)

#diesel_state = calc_diesel_state(census_data)


# (2.2)State Natural Gas Consumption ##########################################
def calc_ng_state(census_data):
    """
    cost_ng (000 $): 2012 Economic Census data. Columns: state, state_abbr,
    NAICS, cost_ng_k_usd.

    price_ng ($/mmbtu):  2012 EIA state-level industrial natural gas price data.
    Columns: state_abbr, price_usd_per_mcf, price_usd_per_mmbtu.

    ng_state (mmbtu): = cost_diesel / price_diesel. Columns: state, state_abbr,
    NAICS, fuel_type, fuel_state_mmbtu.
    """

    ####### cost_ng (000 $)
    cost_ng = pd.DataFrame(
        census_data[['state','state_abbr', 'NAICS', 'cost_ng']]
        )
    cost_ng.rename(columns={'cost_ng': 'cost_ng_k_usd'},inplace=True)

    ####### price_ng ($/mmbtu)
    price_ng = pd.read_excel(
            'https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PIN_DMCF_A.xls',
            sheet_name='Data 1', header=2)

    price_ng.columns =['Date','US','AL','AK','AZ','AR','CA','CO','CT','DE','DC',
                       'FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                       'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY',
                       'NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT',
                       'VT','VA','WA','WV','WI','WY']

    price_ng = price_ng.drop('US', axis=1)

    #Only keep 2012 data
    price_ng = price_ng.loc[price_ng['Date'].dt.year==2012]
    price_ng.reset_index(level=0, inplace=True)
    price_ng = price_ng.drop(['index','Date'], axis=1)
    price_ng['DC'] = (price_ng['VA'] + price_ng['MD']) / 2                         # DC ng prices are NaN

    #Transpose the dataframe
    price_ng = price_ng.transpose()
    price_ng.index.name = 'state_abbr'
    price_ng.columns = ['price_usd_per_mcf']
    price_ng = price_ng.reset_index()

    #Convert to $/mmbtu
    price_ng['price_usd_per_mmbtu'] = price_ng['price_usd_per_mcf'] / 1.036        # https://www.eia.gov/tools/faqs/faq.php?id=45&t=8

    ####### ng_state (mmbtu)
    ng_state = pd.merge(cost_ng, price_ng, on='state_abbr', how='outer')

    ng_state['fuel_state_mmbtu'] = \
             ng_state['cost_ng_k_usd']*1000/ng_state['price_usd_per_mmbtu']

    ng_state['fuel_type'] = 'NATURAL GAS'

    ng_state = ng_state[['state','state_abbr','NAICS','fuel_type',
                         'fuel_state_mmbtu']]
    #print(ng_state)

    return ng_state

#ng_state = calc_ng_state(census_data)




# (2.3)State LPG Consumption ##################################################
def calc_lpg_state(census_data):
    """
    cost_lpg (000 $): 2012 Economic Census data. Columns: state, state_abbr,
    NAICS, cost_lpg_k_usd.

    price_lpg ($/mmbtu):  2012 EIA region-level residential propane price data.
    Columns: state, price_usd_per_gal.

    lpg_state (mmbtu): = cost_lpg / price_lpg. Columns: state, state_abbr,
    NAICS, fuel_type, fuel_state_mmbtu.
    """

    ####### cost_lpg (000 $)
    cost_lpg = pd.DataFrame(
        census_data[['state','state_abbr', 'NAICS', 'cost_other']]
        )
    cost_lpg.rename(columns = {'cost_other': 'cost_lpg_k_usd'}, inplace=True)

    ####### price_lpg ($/mmbtu)
    price_lpg_region = pd.read_excel(
         'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPLLPA_PRS_DPGAL_M.xls',
         sheet_name='Data 1', header=2)

    price_lpg_region.columns = ['Date', 'US', 'EAST COAST', 'NEW ENGLAND',
                                'CENTRAL ATLANTIC', 'LOWER ATLANTIC', 'MIDWEST',
                                'GULF COAST', 'ROCKY MOUNTAIN']

    price_lpg_region = price_lpg_region.drop(['EAST COAST', 'GULF COAST',
                                              'ROCKY MOUNTAIN'], axis=1)

    #Only keep 2012 data
    price_lpg_region = price_lpg_region.loc[
        price_lpg_region['Date'].dt.year==2012
        ]
    price_lpg_region.reset_index(level=0, inplace=True)
    price_lpg_region = price_lpg_region.drop(['index','Date'], axis=1)

    #Transpose the dataframe
    price_lpg_region = price_lpg_region.transpose()
    price_lpg_region.index.name = 'lpg_residential_padd'

    #Calculate annual average wholesale propane price
    price_lpg_region['price_usd_per_gal'] = price_lpg_region.mean(axis=1)
    price_lpg_region = price_lpg_region[['price_usd_per_gal']]

    #Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'lpg_residential_padd']]
    region = region.append({'state':'DISTRICT OF COLUMBIA',
                            'lpg_residential_padd':'CENTRAL ATLANTIC'},
                            ignore_index=True)

    price_lpg = pd.merge(price_lpg_region, region,
                         on='lpg_residential_padd', how='outer')

    price_lpg= price_lpg.drop('lpg_residential_padd', axis=1)

    price_lpg = price_lpg[['state','price_usd_per_gal']]

    ####### lpg_state (mmbtu)
    lpg_state = pd.merge(cost_lpg, price_lpg, on='state', how='outer')

    lpg_state['fuel_state_gal'] = \
                lpg_state['cost_lpg_k_usd'] * 1000 / \
                lpg_state['price_usd_per_gal']

    lpg_state['fuel_state_mmbtu'] = lpg_state['fuel_state_gal'] / 42 * 3.836       # https://www.eia.gov/totalenergy/data/monthly/pdf/sec13.pdf  (A1)

    lpg_state['fuel_type'] = 'LP GAS'

    lpg_state = lpg_state[['state','state_abbr','NAICS','fuel_type',
                           'fuel_state_mmbtu']]

    return lpg_state

#lpg_state = calc_lpg_state(census_data)



# （2.4）State Electricity Consumption ##########################################
def calc_elec_state(census_data):
    """
    cost_elec (000 $): 2012 Economic Census data. Columns: state, state_abbr,
    NAICS, cost_elec_k_usd.

    price_elec ($/kwh): = rev (000 $) /sal (MWh). Columns: state_abbr,
    price_usd_per_kwh.

    elec_state (mmbtu): = cost_lpg / price_lpg. Columns: state, state_abbr,
    NAICS, elec_state_mmbtu.
    """

    ####### cost_elec (000 $)
    cost_elec = pd.DataFrame(
        census_data[['state','state_abbr', 'NAICS', 'cost_elec']]
        )
    cost_elec.rename(columns={'cost_elec': 'cost_elec_k_usd'}, inplace=True)

    ####### price_elec ($/kWh)
    #Collect electricity revenue data (000 $)
    rev = pd.read_excel(
            'http://www.eia.gov/electricity/data/state/revenue_annual.xlsx',
            header=1)

    rev = rev.loc[rev['Year']==2012]
    rev = rev.loc[rev['Industry Sector Category']=='Total Electric Industry']
    rev = rev.loc[rev['State']!='US']
    rev = rev[['State','Industrial']]
    rev.rename(columns = {'State':'state_abbr',
                          'Industrial':'rev_k_usd'}, inplace = True)

    #Collect electricity sales data (MWh)
    sal = pd.read_excel(
            'http://www.eia.gov/electricity/data/state/sales_annual.xlsx',
             header=1)

    sal = sal.loc[sal['Year']==2012]
    sal = sal.loc[sal['Industry Sector Category']=='Total Electric Industry']
    sal = sal.loc[sal['State']!='US']
    sal = sal[['State','Industrial']]
    sal.rename(columns = {'State':'state_abbr',
                          'Industrial':'sal_mwh'}, inplace = True)

    #Calculate electricity price ($/kWh)
    merge = pd.merge(sal, rev, on='state_abbr')
    merge['price_usd_per_kwh'] = merge['rev_k_usd'] / merge['sal_mwh']
    price_elec = merge[['state_abbr','price_usd_per_kwh']]

    ####### elec_state (mmbtu)
    elec_state = pd.merge(cost_elec, price_elec, on='state_abbr', how='outer')

    elec_state['elec_state_kwh'] = \
                 elec_state['cost_elec_k_usd']*1000/\
                 elec_state['price_usd_per_kwh']

    elec_state['fuel_state_mmbtu'] = \
                 elec_state['elec_state_kwh'] * 0.0034095106405145

    elec_state['fuel_type'] = 'ELECTRICITY'

    elec_state = elec_state[['state','state_abbr','NAICS','fuel_type',
                             'fuel_state_mmbtu']]

    return elec_state

#elec_state = calc_elec_state

## (2.5)Summary: State Fuel Consumption ########################################
#"""
#fuel_state: Concatenate diesel_state, ng_state, lpg_state, and elec_state.
#Columns: state, state_abbr, NAICS, fuel_type, fuel_state_mmbtu.
#"""
#fuel_state = pd.concat([diesel_state, ng_state, lpg_state, elec_state])
#fuel_state.set_index(['state', 'state_abbr', 'NAICS'], inplace=True)
#fuel_state = fuel_state.sort_index().reset_index()
##print(fuel_state.head(100))


# 3.REAL GDP ##################################################################
# (3.1)Source Data from BEA ###################################################
def calc_bea_multiplier(calculation_years=range(2010, 2017), base_year=2012):
    """
    bea: 1997-2018 annual GDP in the construction sector by state (real GDP in
    chained dollars). Columns: state, year, gdp
    """

    base_url = 'https://apps.bea.gov/api/data'

    params = {'UserID':'30E3AEAC-AB9E-4368-B150-1E347556C91A',
              'method':'GetData',
              'datasetname':'RegionalProduct',
              'Component':'RGDP_SAN',                                              # RGDP_SAN: State annual naics, real GDP in chained dollars
              'IndustryId':'11',                                                   # 11: Construction sector
              'Year':'ALL',
              'GeoFips':'STATE',
              'ResultFormat':'JSON'}

    r = requests.get(base_url, params=params)
#    url = r.url
    #print(url)
#    response = urllib.request.urlopen(url)
#    data = response.read()
#    datajson = json.loads(data)
    bea = pd.DataFrame(r.json()['BEAAPI']['Results']['Data'],
                       columns=['GeoName','TimePeriod','DataValue'])

    ####### Rename columns & Remove invalid values
    bea.rename(columns={'GeoName':'state','TimePeriod':'year',
                        'DataValue':'gdp'}, inplace=True)
    invalid = '(D)'
    bea = bea.replace(invalid, bea.replace([invalid], None))

    ####### Remove commas in numbers & Change data types
    bea['gdp'] = bea['gdp'].apply(lambda x: x.replace(',',"")).astype(float)
    bea = bea.astype({'year': int})
    
    bea = bea[bea.year.between(calculation_years[0], calculation_years[-1])]

    # (3.2)GDP Growth Rate as A Multiplier #############################
    #multiplier: Index: state. Columns: 1997, 1998, 1999 ... 2018.
    multiplier = bea.pivot(index='state', columns='year', values='gdp')
    multiplier['base_year'] = multiplier[base_year]

    # years = range(1997,2019)
    # for y in period:
    #     multiplier[y] = multiplier[y] / multiplier['base_year']
    multiplier = multiplier.divide(multiplier.base_year, axis=0)

    multiplier.reset_index(inplace=True)
    multiplier = multiplier.drop('base_year', axis=1)
    multiplier['state'] = multiplier['state'].str.upper()

    multiplier.set_index('state',inplace=True)

    multiplier = multiplier.drop(['FAR WEST', 'GREAT LAKES', 'MIDEAST',
                                  'NEW ENGLAND', 'PLAINS', 'ROCKY MOUNTAIN',
                                  'SOUTHEAST', 'SOUTHWEST', 'UNITED STATES'])

    multiplier.reset_index(inplace=True)

    return multiplier


#print(multiplier.head(20))




# 4.COUNTY-LEVEL DATA #########################################################
# (4.1)Fraction of County Establishment Counts ################################
def calc_county_fraction(cbp_2012):
    """
    cbp_source: 2012 establishment counts by NAICS code and county. Columns:
    fipstate, fipscty, naics, empflag, emp_nf, emp, qp1_nf, qp1, ap_nf, ap, est,
    n1_4, n5_9, n10_19, n20_49, n50_99, n100_249, n250_499, n500_999, n1000,
    n1000_1, n1000_2, n1000_3, n1000_4, censtate, cencty, COUNTY_FIPS, region,
    Under 50, naics_n, industry.

    cbp: state, county, NAICS, est.

    frac: state, county, NAICS (only keep 23, 236, 237, 238), est,
    est_county_frac.
    """

    # cbp_source = get_cbp.CBP(2012)
    # cbp = cbp_source.cbp

    cbp_2012 = cbp_2012[['fipstate', 'COUNTY_FIPS', 'naics', 'est']]

    ######## Only keep NAICS 23, 236, 237 & 238
    cbp_2012.set_index('naics', inplace=True)
    cbp_2012 = cbp_2012.loc[[23, 236, 237, 238]].reset_index()
    # cbp_2012.rename(columns={'naics':'NAICS','fipstate':'state_fips',
    #                          'COUNTY_FIPS':'county_fips'}, inplace=True)

    ####### Add state names and county names
    fips = pd.read_csv('../calculation_data/input_us_fips_codes.csv')
    fips = fips[['State', 'County_Name', 'FIPS State', 'COUNTY_FIPS']]
    fips.rename(columns={'State':'state','County_Name':'county',
                         'FIPS State':'fipstate', 'naics': 'NAICS'},
                inplace=True)

    cbp_2012 = pd.merge(cbp_2012, fips, on=['fipstate','COUNTY_FIPS'],
                      how='outer')

    cbp_2012 = cbp_2012[
        ['state','fipstate','county', 'COUNTY_FIPS', 'naics','est' ]
        ]
    cbp_2012 = cbp_2012.dropna()
    cbp_2012.set_index('state', inplace=True)
    #print(county_frac.head(50))

    ####### Calculate fraction of county establishment counts
    def frac(NAICS, cbp):
        df = cbp.loc[cbp['naics'] == NAICS]
        df['est_county_frac'] = df['est'].divide(df['est'].sum(level='state'))
        return df

    county_frac = pd.concat([frac(n, cbp_2012) for n in [23, 236, 237, 238]])
    county_frac.reset_index(level=0,  inplace=True)
    county_frac['state'] = county_frac['state'].str.upper()
    county_frac['county'] = county_frac['county'].str.upper()

    return county_frac

def format_state_energy(energy_state):
    
    energy_state.rename(columns={'fuel_type': 'MECS_FT'},
                         inplace=True)
    
    energy_state.replace({'DIESEL': 'Diesel', 'ELECTRICITY':
                          'Net_electricity', 'LP GAS': 'LPG_NGL',
                          'NATURAL GAS': 'Natural_gas'}, inplace=True)
    
    
    return energy_state

#cbp_source = get_cbp.CBP(2012)
#cbp = cbp_source.cbp
#
#county_frac = calc_county_fraction(cbp_2012)

def calc_county_energy(fuel_state, county_frac, multiplier,
                       calculation_years=range(2010, 2017)):
    """
    Calculate county-level energy use.
    """
    # (4.2)County Fuel Consumption #############################################
    fuel_county = pd.merge(fuel_state, county_frac, on=['state','NAICS'],
                           how='outer')

    fuel_county['fuel_county_mmbtu'] = \
               fuel_county['fuel_state_mmbtu'] * fuel_county['est_county_frac']

    fuel_county = fuel_county[
        ['state','state_abbr','fipstate', 'COUNTY_FIPS','NAICS','MECS_FT',
         'fuel_county_mmbtu']
        ]
        
    fuel_county = pd.merge(fuel_county, multiplier, on='state', how='outer')

#    for y in calculation_years:
#        fuel_county[y] = fuel_county[y] * fuel_county['fuel_county_mmbtu']
#        fuel_county = fuel_county.rename(
#            columns={y: str(y)+'_fuel_county_mmbtu'}
#            )
        
    fuel_county.update(fuel_county[[y for y in calculation_years]].multiply(
            fuel_county.fuel_county_mmbtu, axis=0
            ))
    
    fuel_county = pd.melt(
            fuel_county, id_vars=['state_abbr','fipstate', 'COUNTY_FIPS',
                                  'NAICS','MECS_FT', 'fuel_county_mmbtu'],
            value_vars=[y for y in calculation_years], var_name='year',
            value_name='MMBtu_TOTAL'
            )
    
    fuel_county = fuel_county.drop('fuel_county_mmbtu', axis=1)

    fuel_county.set_index('fipstate', inplace=True)

    # fuel_county.to_csv('output\cons_county.csv')

    return fuel_county

    #print(fuel_county.head(10))
