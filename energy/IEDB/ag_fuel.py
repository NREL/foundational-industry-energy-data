import pandas as pd
import requests
import urllib
import json
import datetime as dt
import re


"""
Estimate 2017 county-level fuel use in the agricultural sector based on 2017
USDA Census results. Only diesel, gasoline, LP gas, and 'other' (assumed to be
residual fuel oil) are included.
"""


calc_year = 2012

# 1.EXPENSES ##################################################################
# state_tot: state total fuel expenses ($) ####################################
"""
Automatically collect state-level total fuel expenses data by NAICS
code from USDA NASS Census results.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'CENSUS',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': calc_year,
          'agg_level_desc': 'STATE',
          'short_desc': 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $',
          'domain_desc': 'NAICS CLASSIFICATION'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
state_tot = pd.DataFrame(datajson['data'],
                         columns=['state_name', 'state_alpha',
                                  'state_fips_code', 'domaincat_desc',
                                  'Value'])
pd.set_option('display.max_columns', None)
#print(state_tot.head(20))


####### Split the column of NAICS codes:
state_tot[['a','b']] = state_tot.domaincat_desc.str.split("(", expand=True)
state_tot[['NAICS','c']] = state_tot.b.str.split(")", expand=True)
state_tot = state_tot.drop(['domaincat_desc','a','b','c'], axis=1)
#print(state_tot.head(20))


####### Remove invalid values & Rename columns & Set index & Sort
invalid = '                 (D)'
state_tot = state_tot.replace(invalid, state_tot.replace([invalid], '0'))
state_tot.rename(columns = {'state_name':'state', 'state_alpha':'state_abbr',
                            'state_fips_code': 'fipstate',
                            'Value':'state_fuel_expense'},
                            inplace=True)
state_tot = state_tot.sort_index(ascending=True)
state_tot = state_tot[['state','state_abbr','fipstate',
                       'NAICS','state_fuel_expense']]
#print(state_tot.head(50))


####### Remove commas in numbers
state_tot['state_fuel_expense'] = state_tot['state_fuel_expense'].apply(
                                  lambda x: x.replace(',', "")).astype(int)
#print(state_tot.head(50))


# region_diesel: region diesel expenses ($) ###################################
"""
Automatically collect region-level disel expense data from USDA NASS 2017
Survey results.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': calc_year,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_diesel = pd.DataFrame(datajson['data'],
                             columns=['region_desc',
                                      'short_desc',
                                      'Value'])
pd.set_option('display.max_columns', None)
#print(region_diesel)


####### Rename columns
region_diesel.rename(columns = {'region_desc':'region',
                                'short_desc':'fuel_type',
                                'Value':'fuel_expenses_$'},
                                inplace=True)


####### Reformat the column of region
region_diesel[['region','a']] = region_diesel.region.str.split(
                                ",", expand=True)
region_diesel = region_diesel.drop('a', axis=1)
region_diesel.set_index('region', inplace=True)
#print(region_diesel)


####### Reformat the column of fuel type
region_diesel['fuel_type'] = region_diesel['fuel_type'].replace(
                            'FUELS, DIESEL - EXPENSE, MEASURED IN $', 'DIESEL')
region_diesel = region_diesel.sort_index(ascending=True)
#print(region_diesel)


####### Remove commas in numbers
region_diesel['fuel_expenses_$'] = region_diesel['fuel_expenses_$'].apply(
                                 lambda x: x.replace(',', "")).astype('int64')
#print(region_diesel)


# region_gasoline: region gasoline expenses ($) ###############################
"""
Automatically collect region-level gasoline expense data from USDA NASS 2017
Survey results.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': calc_year,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, GASOLINE - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_gasoline = pd.DataFrame(datajson['data'],
                               columns=['region_desc',
                                        'short_desc',
                                        'Value'])
pd.set_option('display.max_columns', None)
#print(region_gasoline)


####### Rename columns
region_gasoline.rename(columns = {'region_desc':'region',
                                  'short_desc':'fuel_type',
                                  'Value':'fuel_expenses_$'},
                                  inplace=True)


####### Reformat the column of region
region_gasoline[['region','a']] = region_gasoline.region.str.split(
                                  ",", expand=True)
region_gasoline = region_gasoline.drop('a', axis=1)
region_gasoline.set_index('region', inplace=True)
#print(region_gasoline)


####### Reformat the column of fuel type
region_gasoline['fuel_type'] = region_gasoline['fuel_type'].replace(
                        'FUELS, GASOLINE - EXPENSE, MEASURED IN $', 'GASOLINE')
region_gasoline = region_gasoline.sort_index(ascending=True)
#print(region_gasoline)


####### Remove commas in numbers
region_gasoline['fuel_expenses_$'] = region_gasoline['fuel_expenses_$'].apply(
                                  lambda x: x.replace(',', "")).astype('int64')
#print(region_gasoline)



# region_lpg: region lp gas expenses ($) ######################################
"""
Automatically collect region-level LP gas expense data from USDA NASS 2017
Survey results.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': calc_year,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, LP GAS - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_lpg = pd.DataFrame(datajson['data'],
                          columns=['region_desc',
                                   'short_desc',
                                   'Value'])
pd.set_option('display.max_columns', None)
#print(region_lpg)


####### Rename columns
region_lpg.rename(columns = {'region_desc':'region',
                             'short_desc':'fuel_type',
                             'Value':'fuel_expenses_$'},
                             inplace=True)


####### Reformat the column of region
region_lpg[['region','a']] = region_lpg.region.str.split(",", expand=True)
region_lpg = region_lpg.drop('a', axis=1)
region_lpg.set_index('region', inplace=True)
#print(region_lpg)


####### Reformat the column of fuel type
region_lpg['fuel_type'] = region_lpg['fuel_type'].replace(
                          'FUELS, LP GAS - EXPENSE, MEASURED IN $', 'LPG')
region_lpg = region_lpg.sort_index(ascending=True)
#print(region_lpg)


####### Remove commas in numbers
region_lpg['fuel_expenses_$'] = region_lpg['fuel_expenses_$'].apply(
                                lambda x: x.replace(',', "")).astype('int64')
#print(region_lpg)



# region_other: other region fuel expenses ($) ################################
"""
Automatically collect other region-level fuel expense data from USDA NASS 2017
Survey results.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': calc_year,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, OTHER - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_other = pd.DataFrame(datajson['data'],
                            columns=['region_desc',
                                     'short_desc',
                                     'Value'])
pd.set_option('display.max_columns', None)
#print(region_other)


####### Rename columns
region_other.rename(columns = {'region_desc':'region',
                               'short_desc':'fuel_type',
                               'Value':'fuel_expenses_$'},
                               inplace=True)

####### Reformat the column of region
region_other[['region','a']] = region_other.region.str.split(",", expand=True)
region_other = region_other.drop('a', axis=1)
region_other.set_index('region', inplace=True)
#print(region_other)


####### Reformat the column of fuel type
region_other['fuel_type'] = region_other['fuel_type'].replace(
                            'FUELS, OTHER - EXPENSE, MEASURED IN $', 'OTHER')
region_other = region_other.sort_index(ascending=True)
#print(region_other)


####### Remove commas in numbers
region_other['fuel_expenses_$'] = region_other['fuel_expenses_$'].apply(
                                  lambda x: x.replace(',', "")
                                  ).astype('int64')
#print(region_other)



# frac: fuel type fraction of total fuel expenses #############################
"""
Calculate each fuel type's fraction of total fuel expenses based on
region_diesel, region_gasoline, region_lp, and region_other.
"""

frac = pd.merge(region_diesel, region_gasoline, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = pd.merge(frac, region_lpg, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = pd.merge(frac, region_other, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = frac.set_index('fuel_type', append=True)

frac['fuel_type_frac'] = frac['fuel_expenses_$'].divide(
                         frac['fuel_expenses_$'].sum(level='region'))

frac = frac.drop(['fuel_expenses_$'], axis=1)

frac = frac.reset_index()
#print(frac)



# expense： state expenses by fuel type and NAICS ($) ##########################
"""
Combine state_tot and frac
Step 1： In the dataframe 'state_tot', add each state's region.
Step 2: Merge 'frac' and 'state_tot'. Name the merged df as 'expense'.
Step 3: Create a new column 'fuel_expense_$', which equals
        'state_fuel_expense_$' * 'fuel_type_frac'.
Final columns: state, state_abbr, region, NAICS, fuel_type, fuel_expenses_$
"""

####### Step 1
region_file = pd.read_csv('../calculation_data/input_region.csv')

region = region_file[['state', 'state_abbr', 'state_code', 'region']]

state_tot = pd.merge(state_tot, region, on=[
            'state', 'state_abbr'], how='outer')

state_tot = state_tot[[
            'state','state_abbr','state_code', 'region','NAICS',
            'state_fuel_expense'
            ]]

####### Step 2
expense = pd.merge(state_tot, frac, on='region')

####### Step 3
expense['fuel_expense_dollar'] = \
              expense.state_fuel_expense * expense.fuel_type_frac

expense = expense.drop(['state_fuel_expense', 'fuel_type_frac'], axis=1)
#print(expense.head(30))



# 2.PRICES#####################################################################
# price_diesel: state level diesel price ($/gal) ##############################
def get_diesel_price(year):
    """
    Automatically collect monthly diesel retail price data by region from EIA
    and calculate state-level data.
    """
    
    price_diesel_region = pd.read_excel(
            'https://www.eia.gov/petroleum/gasdiesel/xls/psw18vwall.xls',
            sheet_name='Data 2', header=2)
    
    price_diesel_region.columns = ['Date',
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
                                   'WEST COAST EXCEPT CALIFORNIA']
    
    price_diesel_region = price_diesel_region.drop([
                          'US', 'EAST COAST', 'WEST COAST'], axis=1)
    
    ####### Only keep 2017 data
    price_diesel_region = price_diesel_region.loc[
                          price_diesel_region['Date'].dt.year==year]
    price_diesel_region.reset_index(level=0, inplace=True)
    price_diesel_region = price_diesel_region.drop(['index','Date'], axis=1)
    #print(price_diesel_region)
    
    
    ####### Transpose the dataframe
    price_diesel_region = price_diesel_region.transpose()
    price_diesel_region.index.name = 'diesel_padd'
    #print(price_diesel_region)
    
    
    ####### Calculate 2017 average diesel price
    price_diesel_region['price_dollar_per_gal'] = price_diesel_region.mean(axis=1)
    price_diesel_region = price_diesel_region[['price_dollar_per_gal']]
    #print(price_diesel_region)
    
    
    ####### Mark the fuel type
    price_diesel_region['fuel_type'] = 'DIESEL'
    price_diesel_region = price_diesel_region[[
                          'fuel_type', 'price_dollar_per_gal']]
    price_diesel_region.reset_index(level=0, inplace=True)
    #print(price_diesel_region)
    
    
    ####### Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'state_code', 'diesel_padd']]
    
    price_diesel = pd.merge(
                   price_diesel_region, region, on='diesel_padd', how='outer'
                   )
    
    price_diesel.set_index('state', inplace=True)
    price_diesel= price_diesel.drop('diesel_padd', axis=1)
    price_diesel = price_diesel.sort_index()
    #print(price_diesel)

    return price_diesel


price_diesel = get_diesel_price(year=calc_year)


# price_gasoline: region level gasoline price ($/gal) #########################
def get_gasoline_price(year):
    """
    Automatically collect monthly gasoline retail price data by region from
    EIA and calculate state-level data.
    """
    
    price_gasol_region = pd.read_excel(
            'https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls',
            sheet_name='Data 3', header=2)
    
    price_gasol_region.columns = ['Date',
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
                                  'BOSTON','CHICAGO','CLEVELAND','DENVER',
                                  'HOUSTON','LOS ANGELES','MIAMI','NEW YORK CITY',
                                  'SAN FRANCISCO','SEATTLE']
    
    price_gasol_region = price_gasol_region.drop(['US', 'EAST COAST', 'BOSTON',
                 'CHICAGO','CLEVELAND','DENVER','HOUSTON','LOS ANGELES','MIAMI',
                 'NEW YORK CITY','SAN FRANCISCO','SEATTLE'], axis=1)
    
    
    ####### Only keep 2017 data
    price_gasol_region = price_gasol_region.loc[
                         price_gasol_region['Date'].dt.year==year]
    price_gasol_region.reset_index(level=0, inplace=True)
    price_gasol_region = price_gasol_region.drop(['index','Date'], axis=1)
    #print(price_gasol_region)
    
    
    ####### Transpose the dataframe
    price_gasol_region = price_gasol_region.transpose()
    price_gasol_region.index.name = 'gasoline_padd'
    #print(price_gasol_region)
    
    
    ####### Calculate 2017 average gasoline price
    price_gasol_region['price_dollar_per_gal'] = price_gasol_region.mean(axis=1)
    price_gasol_region = price_gasol_region[['price_dollar_per_gal']]
    #print(price_gasol_region)
    
    
    ####### Mark the fuel type
    price_gasol_region['fuel_type'] = 'GASOLINE'
    price_gasol_region = price_gasol_region[[
                         'fuel_type', 'price_dollar_per_gal']]
    price_gasol_region.reset_index(level=0, inplace=True)
    #print(price_gasol_region)
    
    
    ####### Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'state_code', 'gasoline_padd']]
    
    price_gasoline = pd.merge(
                     price_gasol_region, region, on='gasoline_padd', how='outer')
    
    price_gasoline.set_index('state', inplace=True)
    price_gasoline= price_gasoline.drop('gasoline_padd', axis=1)
    price_gasoline = price_gasoline.sort_index()
    #print(price_gasoline)
    
    return price_gasoline

price_gasoline = get_gasoline_price(year=calc_year)


# price_lpg: region level lp gas price ($/gal) ################################
def get_lpg_price(year):
    """
    Automatically collect monthly monthly wholesale propane price data by region from
    EIA and calculate state-level data.
    """
    
    if year == 2012:
        
        price_lpg_region = pd.read_excel(
             'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPLLPA_PRS_DPGAL_w.xls',
             sheet_name='Data 1', header=2
             )
    
    if year == 2017:
        
        price_lpg_region = pd.read_excel(
             'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPLLPA_PWR_DPGAL_w.xls',
             sheet_name='Data 1', header=2
             )
        
    col_list = ['Date']
    
    for col in price_lpg_region.columns:
        
        if re.search('(?<=Weekly ).*(?= Propane)', col) == None:
            
            continue
        
        else:
            
            col_list.append(
                    re.search('(?<=Weekly ).*(?= Propane)',
                              col).group(0).upper()
                    )
        
    price_lpg_region.columns = col_list
    
    price_lpg_region.rename(columns={'U.S. WEEKLY': 'US'}, inplace=True)
    
    price_lpg_region.columns =\
        [re.sub(' \(PADD \w+\)', '', x) for x in price_lpg_region.columns]

#    price_lpg_region.columns = ['Date', 'US', 'EAST COAST', 'CENTRAL ATLANTIC',
#                                'MARYLAND', 'NEW JERSEY', 'NEW YORK',
#                                'PENNSYLVANIA', 'LOWER ATLANTIC', 'GEORGIA',
#                                'NORTH CAROLINA', 'VIRGINIA', 'MIDWEST',
#                                'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS',
#                                'KENTUCKY', 'MICHIGAN', 'MINNESOTA', 'MISSOURI',
#                                'NEBRASKA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
#                                'SOUTH DAKOTA', 'WISCONSIN', 'GULF COAST',
#                                'ALABAMA', 'ARKANSAS', 'MISSISSIPPI', 'TEXAS',
#                                'ROCKY MOUNTAIN', 'COLORADO']
    
    ####### Only keep 2017 data
    price_lpg_region = price_lpg_region.loc[
            price_lpg_region['Date'].dt.year==year
            ]
    price_lpg_region.reset_index(level=0, inplace=True)
    price_lpg_region = price_lpg_region.drop(['index','Date'], axis=1)
    #print(price_lpg_region)
    
    ####### Transpose the dataframe
    price_lpg_region = price_lpg_region.transpose()
    price_lpg_region.index.name = 'lpg_wholesale_padd'
    #print(price_lpg_region)
    
    ####### Calculate 2017 average wholesale propane price
    price_lpg_region['price_dollar_per_gal'] = price_lpg_region.mean(axis=1)
    price_lpg_region = price_lpg_region[['price_dollar_per_gal']]
    #print(price_lpg_region)
    
    
    ####### Mark the fuel type
    price_lpg_region['fuel_type'] = 'LPG'
    price_lpg_region = price_lpg_region[['fuel_type', 'price_dollar_per_gal']]
    price_lpg_region.reset_index(level=0, inplace=True)
    #print(price_lpg_region)
    
    
    ####### Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'state_code', 'lpg_wholesale_padd']]
    
    price_lpg = pd.merge(price_lpg_region, region,
                         on='lpg_wholesale_padd', how='outer')
    
    price_lpg.set_index('state', inplace=True)
    price_lpg= price_lpg.drop('lpg_wholesale_padd', axis=1)
    price_lpg = price_lpg.sort_index()
    
    price_lpg.dropna(inplace=True)
    #print(price_lpg)
    
    return price_lpg


price_lpg = get_lpg_price(calc_year)


# price_other: region level heating oil price ($/gal) #########################
def get_heatingoil_price(year):
    
    """
    Automatically collect 2012 weekly residential propane price or 
    2017 weekly wholesale propane price data by region from
    EIA and calculate state-level data.
    """
    
    if year == 2012:
        
        price_other_region = pd.read_excel(
             'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPD2F_PRS_DPGAL_W.xls',
             sheet_name='Data 1', header=2)

    if year == 2017:
    
        price_other_region = pd.read_excel(
             'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPD2F_PWR_DPGAL_w.xls',
             sheet_name='Data 1', header=2)

    col_list = ['Date']
    
    for col in price_other_region.columns:
        
        if re.search('(?<=Weekly ).*(?= No. 2)', col) == None:
            
            continue
        
        else:
            
            col_list.append(
                    re.search('(?<=Weekly ).*(?= No. 2)',
                              col).group(0).upper()
                    )
        
    price_other_region.columns = col_list
    
    price_other_region.rename(columns={'U.S. WEEKLY': 'US'}, inplace=True)
    
    price_other_region.columns =\
        [re.sub(' \(PADD \w+\)', '', x) for x in price_other_region.columns]
    
#    price_other_region.columns = ['Date', 'US', 'EAST COAST', 'NEW ENGLAND',
#                                  'CONNECTICUT', 'MAINE', 'MASSACHUSETTS',
#                                  'NEW HAMPSHIRE', 'RHODE ISLAND', 'VERMONT',
#                                  'CENTRAL ATLANTIC', 'DELAWARE', 'MARYLAND',
#                                  'NEW JERSEY', 'NEW YORK', 'PENNSYLVANIA',
#                                  'LOWER ATLANTIC', 'NORTH CAROLINA', 'VIRGINIA',
#                                  'MIDWEST', 'ILLINOIS', 'INDIANA', 'IOWA',
#                                  'KANSAS', 'KENTUCKY', 'MICHIGAN', 'MINNESOTA',
#                                  'MISSOURI', 'NEBRASKA', 'NORTH DAKOTA', 'OHIO',
#                                  'SOUTH DAKOTA', 'WISCONSIN']

    price_other_region.rename(columns={'PADD 2': 'MIDWEST'}, inplace=True)

    price_other_region = price_other_region.drop(
                         ['EAST COAST', 'NEW ENGLAND'], axis=1)
    
    ####### Only keep data for selected year
    price_other_region = price_other_region.loc[
                         price_other_region['Date'].dt.year==year]
    
    price_other_region.reset_index(level=0, inplace=True)
    price_other_region = price_other_region.drop(['index','Date'], axis=1)
    #print(price_other_region)
    
    ####### Transpose the dataframe
    price_other_region = price_other_region.transpose()
    price_other_region.index.name = 'other_padd'
    #print(price_other_region)
    
    ####### Calculate 2017 average wholesale heating oil price
    price_other_region['price_dollar_per_gal'] = price_other_region.mean(axis=1)
    price_other_region = price_other_region[['price_dollar_per_gal']]
    #print(price_other_region)
    
    ####### Mark the fuel type
    price_other_region['fuel_type'] = 'OTHER'
    price_other_region = price_other_region[['fuel_type', 'price_dollar_per_gal']]
    price_other_region.reset_index(level=0, inplace=True)
    #print(price_other_region)
    
    ####### Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'state_code', 'other_padd']]
    
    price_other = pd.merge(
                  price_other_region, region, on='other_padd', how='outer')
    
    price_other.set_index('state', inplace=True)
    price_other= price_other.drop('other_padd', axis=1)
    price_other = price_other.sort_index()
    #print(price_other)
    
    return price_other

price_other = get_heatingoil_price(year=calc_year)


# price: state prices by fuel type ($/mmbtu) ##################################
def combine_prices(prices_list):
    """
    Concatenate price_diesel, price_gasoline, price_lpg, and price_other.
    Convert $/gallon to $/barrel then to $/mmbtu
    prices_list = [price_diesel, price_gasoline, price_lpg, price_other]
    """
    
    price = pd.concat(prices_list)
    
    price['price_dollar_per_barrel'] = price['price_dollar_per_gal'] * 42
    
    heat_content = pd.DataFrame(
                   {'fuel_type': ['DIESEL', 'GASOLINE', 'LPG', 'OTHER'],
                    'mmbtu_per_barrel':[5.772, 5.053, 3.836, 6.287]}
                   )   # https://www.eia.gov/totalenergy/data/monthly/pdf/sec13.pdf
    
    price = price.reset_index().merge(
            heat_content, how='outer'
            ).set_index('state')
    
    price['price_dollar_per_mmbtu'] = price['price_dollar_per_barrel'] / \
            price['mmbtu_per_barrel']
    
    price = price.drop([
                      'price_dollar_per_gal', 'price_dollar_per_barrel',
                      'mmbtu_per_barrel', 'state_code'], axis=1)

    #print(price)
    
    return price

price = combine_prices([price_diesel, price_gasoline, price_lpg, price_other])

# 3.FUEL CONSUMPTION ##########################################################
# fuel_state: state fuel use (mmbtu) ##########################################
"""
Merge expense and price. Divide expense by price to calculate state-level fuel
consumption by NAICS.
Columns: state, state_abbr, region, NAICS, fuel_type, fuel_expense_dollar,
price_dollar_per_mmbtu, fuel_state_mmbtu.
"""

fuel_state = pd.merge(expense, price, on=['state','fuel_type'], how='left')

fuel_state['fuel_state_mmbtu'] = \
        fuel_state['fuel_expense_dollar']/fuel_state['price_dollar_per_mmbtu']

#print(fuel_state.head(50))



# fc: county farm counts by NAICS #############################################
"""
Automatically collect county-level farm counts data by NAICS from USDA NASS
Census results and calculate each county's state fraction.
Columns: state, county, fc_statefraction.
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'CENSUS',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'FARMS & LAND & ASSETS',
          'year': calc_year,
          'agg_level_desc': 'COUNTY',
          'short_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS',
          'domain_desc': 'NAICS CLASSIFICATION'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)

response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
fc = pd.DataFrame(datajson['data'],
                  columns=['state_name','state_alpha','state_fips_code',
                           'county_name', 'county_ansi',
                           'domaincat_desc','Value'])
        
# Alaskan counties do not have ansi codes
# Note that USDA combines Aleutian Islands, whereas census has separate FIPS
# codes for east and west.
akc = pd.read_csv(
    'https://www2.census.gov/geo/docs/reference/codes/files/st02_ak_cou.txt',
    header=None ,names=['state_abbr', 'fipstate', 'county_ansi',
                        'county_name', 'other'], dtype={'county_ansi':str}
    )

akc.county_name.update(akc[akc.other == 'H1'].county_name.apply(
        lambda x: x.split(' Borough')[0]
        ))

akc.county_name.update(akc[akc.other == 'H1'].county_name.apply(
        lambda x: x.split(' City and')[0]
        ))

akc.county_name.update(akc[akc.other == 'H5'].county_name.apply(
        lambda x: x.split(' Census Area')[0]
        ))

akc.county_name.update(akc[akc.other == 'H6'].county_name.apply(
        lambda x: x.split(' City and Borough')[0]
        ))

akc.county_name.update(akc[akc.other == 'H6'].county_name.apply(
        lambda x: x.split(' Municipality')[0]
        ))

akc['county_name'] = akc.county_name.apply(lambda x: x.upper())

fc = pd.merge(fc, akc[['county_name', 'county_ansi']], on='county_name',
              suffixes=['', '_ak'], how='left')

fc.county_ansi.update(fc.county_ansi_ak)

fc.drop('county_ansi_ak', axis=1, inplace=True)


####### Split the column of NAICS codes:
fc[['a','b']] = fc.domaincat_desc.str.split("(", expand=True)
fc[['NAICS','c']] = fc.b.str.split(")", expand=True)
fc = fc.drop(['domaincat_desc','a','b','c'], axis=1)
#print(fc.head(20))


####### Remove invalid values & Rename columns & Set index & Sort
invalid = '                 (D)'
fc = fc.replace(invalid, fc.replace([invalid], '0'))
fc.rename(columns = {'state_name':'state', 'state_alpha':'state_abbr',
                     'state_fips_code': 'fipstate',
                     'Value':'farm_counts',
                     'county_name':'county'},
                     inplace=True)

#print(fc.head(100))

####### Remove observations for NAICS 1119, which double counts observations
####### for 11192 and "11193 & 11194 & 11199".
fc = fc[fc.NAICS != '1119']
#print(fc.head(20))

####### Remove commas in numbers
fc['farm_counts'] = fc['farm_counts'].apply(lambda x: x.replace(
        ',', "")).astype(int)

# Create COUNTY_FIPS column to match mfg data
fc['COUNTY_FIPS'] = fc.fipstate.add(fc.county_ansi)

fc.COUNTY_FIPS = fc.COUNTY_FIPS.astype('int')

# Drop Aleutian Islands
fc = fc[fc.COUNTY_FIPS !=2]


####### Calculate the fraction of county-level establishments by NAICS
# Corrected to sum on state-county combination, not just on county name as
# there are duplicate county names.
# state_county = fc[['state','county']]
# state_county = state_county.drop_duplicates()
fc.set_index(['fipstate', 'NAICS', 'COUNTY_FIPS'], inplace=True)

fc['state_naics_count'] = fc.farm_counts.sum(level=[0,1])

# fc = fc.groupby(['COUNTY_FIPS', 'NAICS'], as_index=False)['farm_counts'].sum()
fc['fc_statefraction'] = fc.farm_counts.divide(fc.state_naics_count)
# fc = pd.merge(state_county, fc, on='county', how='outer')
# fc.set_index(['state','county'], inplace=True)
# fc = fc.sort_index()
# fc['fc_statefraction'] = fc['farm_counts'].divide(
#                          fc['farm_counts'].sum(level='state'))
fc = fc.reset_index()
#print(fc)


# fuel_county: county fuel use (mmbtu) ########################################
"""
Calculations based on fuel_state (state-level fuel use by NAICS) and fc
(county farm counts by NAICS).
Index: state.
Columns: state_abbr, county, NAICS, fuel_type, fuel_county_mmbtu.
"""

# fuel_state = fuel_state[['state', 'state_abbr', 'state_code', 'NAICS',
#                          'fuel_type', 'fuel_state_mmbtu']]

fuel_state.rename(columns={'state_code': 'fipstate'}, inplace=True)

fc['fipstate'] = fc.fipstate.astype(int)

fuel_county = pd.merge(
    fc, fuel_state[['fipstate', 'NAICS', 'fuel_type', 'fuel_state_mmbtu']],
     on=['fipstate', 'NAICS'], how='left'
     )

# Duplicate Alaskan counties
fuel_county.set_index(['COUNTY_FIPS', 'NAICS', 'fuel_type'], inplace=True)

fuel_county = fuel_county[~fuel_county.index.duplicated()]

fuel_county.reset_index(inplace=True)

#fuel_county = pd.merge(fuel_state, fc, on='state', how='outer')

fuel_county['fuel_county_mmbtu'] = \
                    fuel_county.fc_statefraction * fuel_county.fuel_state_mmbtu

fuel_county = fuel_county[['state', 'state_abbr', 'fipstate', 'county',
                           'COUNTY_FIPS', 'NAICS', 'fuel_type',
                           'fuel_county_mmbtu']]

#print(fuel_county.head(50))

output_name = 'ag_output_fuel_use_by_county_'+ str(calc_year) + '_' +\
        dt.datetime.today().strftime('%Y%m%d_%H%M')+'.csv'
    
fuel_county.to_csv('../results/'+output_name)


## 3. 2000-2017 COUNTY-LEVEL FUEL USE ##########################################
#def calc_multiplier(calculation_years=range(2010, 2018), base_year=calc_year):
#    """
#    Calculate fuel expense multipliers based on USDA NASS Survey
#    results.
#    """
#    
#    base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'
#    
#    params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
#              'source_desc': 'SURVEY',
#              'sector_desc': 'ECONOMICS',
#              'group_desc': 'EXPENSES',
#              'agg_level_desc': 'REGION : MULTI-STATE',
#              'commodity_desc': 'FUELS',
#              'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $'}
#    
#    r = requests.get(base_url, params=params)
#    url = r.url
#    
#    response = urllib.request.urlopen(url)
#    data = response.read()
#    datajson = json.loads(data)
#    multiplier = pd.DataFrame(datajson['data'],
#                              columns=['region_desc',
#                                       'year',
#                                       'Value'])
#    
#    multiplier = multiplier[multiplier.year.isin(calculation_years)]
#    
#    ####### Rename columns
#    multiplier.rename(columns = {'region_desc':'region',
#                                 'Value':'expense'},
#                                 inplace=True)
#    
#    ####### Reformat the column of region
#    multiplier[['region','a']] = multiplier.region.str.split(",", expand=True)
#    multiplier = multiplier.drop('a', axis=1)
#    
#    selected_region = ['ATLANTIC', 'MIDWEST', 'PLAINS', 'SOUTH', 'WEST']
#    multiplier = multiplier[multiplier.region.isin(selected_region)]
#    
#    
#    ####### Remove commas in numbers
#    multiplier['expense'] = multiplier['expense'].apply(
#                            lambda x: x.replace(',', "")).astype('int64')
#    
#    
#    ####### Pivot the table & Calculate multipliers (Base year=2017)
#    multiplier = multiplier.pivot(
#            index='region', columns='year', values='expense'
#            )
#    
#    multiplier.loc[:, [y for y in calculation_years]] = \
#            multiplier[[y for y in calculation_years]].divide(
#                    multiplier[calc_year], axis=0
#                    )
##    for y in calcualtion_years:
##        multiplier[y] = multiplier[y] / multiplier[calc_year]
#    
#    
#    ####### Add state column
#    region_file = pd.read_csv('../calculation_data/input_region.csv')
#    region = region_file[['state', 'region']]
#    
#    multiplier = pd.merge(multiplier, region, on='region', how='outer')
#    
#    multiplier.set_index('state', inplace=True)
#    multiplier = multiplier.drop('region', axis=1)
#    multiplier = multiplier.sort_index()
#    
#    return multiplier
#
#
#
#def calc_county_fuel(fuel_county, multiplier,
#                     calculation_years=range(2010, 2018)):
#    """
#    Calculate county-level fuel use data.
#    """
#    fuel_county = pd.merge(fuel_county, multiplier, on='state', how='outer')
#    
#    fuel_county[[y for y in calculation_years]].update(
#            fuel_county[[y for y in calculation_years]].multiply(
#                    fuel_county.fuel_county_mmbtu
#                    )
#            )
#            
#    fuel_county = fuel_county.drop('fuel_county_mmbtu', axis=1)
#    
##    for y in years:
##        fuel_county[y] = fuel_county[y] * fuel_county['fuel_county_mmbtu']
##        fuel_county = fuel_county.rename(columns={y: str(y)+'_fuel_county_mmbtu'})
#    
#    
#    fuel_county.set_index('state', inplace=True)
#    
#    return fuel_county
#    
##    output_name = 'ag_output_fuel_use_by_county_'+\
##        dt.datetime.today().strftime('%Y%m%d_%H%M')+'.csv'
##    
##    fuel_county.to_csv('../results/'+output_name)
##    
#    #print(fuel_county.head(50))
