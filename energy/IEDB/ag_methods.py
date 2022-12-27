# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 20:24:41 2019

@author: cmcmilla
"""

import requests
import urllib
import pandas as pd
import json


def calc_multiplier(base_year, calculation_years=range(2010, 2018)):
    """
    Calculate fuel expense multipliers based on USDA NASS Survey
    results.
    """
    
    base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'
    
    params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
              'source_desc': 'SURVEY',
              'sector_desc': 'ECONOMICS',
              'group_desc': 'EXPENSES',
              'agg_level_desc': 'REGION : MULTI-STATE',
              'commodity_desc': 'FUELS',
              'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $'}
    
    r = requests.get(base_url, params=params)
    url = r.url
    
    response = urllib.request.urlopen(url)
    data = response.read()
    datajson = json.loads(data)
    multiplier = pd.DataFrame(datajson['data'],
                              columns=['region_desc',
                                       'year',
                                       'Value'])
    
    multiplier = multiplier[multiplier.year.isin(calculation_years)]
    
    ####### Rename columns
    multiplier.rename(columns = {'region_desc':'region',
                                 'Value':'expense'},
                                 inplace=True)
    
    ####### Reformat the column of region
    multiplier[['region','a']] = multiplier.region.str.split(",", expand=True)
    multiplier = multiplier.drop('a', axis=1)
    
    selected_region = ['ATLANTIC', 'MIDWEST', 'PLAINS', 'SOUTH', 'WEST']
    multiplier = multiplier[multiplier.region.isin(selected_region)]
    
    
    ####### Remove commas in numbers
    multiplier['expense'] = multiplier['expense'].apply(
                            lambda x: x.replace(',', "")).astype('int64')
    
    
    ####### Pivot the table & Calculate multipliers (Base year=2017)
    multiplier = multiplier.pivot(
            index='region', columns='year', values='expense'
            )
    
    multiplier.loc[:, [y for y in calculation_years]] = \
            multiplier[[y for y in calculation_years]].divide(
                    multiplier[base_year], axis=0
                    )
        
#    for y in calcualtion_years:
#        multiplier[y] = multiplier[y] / multiplier[calc_year]
    
    ####### Add state column
    region_file = pd.read_csv('../calculation_data/input_region.csv')
    region = region_file[['state', 'region']]
    
    multiplier = pd.merge(multiplier, region, on='region', how='outer')
    
    multiplier.set_index('state', inplace=True)
    multiplier = multiplier.drop('region', axis=1)
    multiplier = multiplier.sort_index()
    
    return multiplier


def calc_county_fuel(fuel_county, multiplier,
                     calculation_years=range(2010, 2018)):
    """
    Calculate county-level fuel use data.
    """
    fuel_county = pd.merge(fuel_county, multiplier, on='state', how='left')
    
    fuel_county.drop(['county', 'state_abbr'], axis=1, inplace=True)
    
    fuel_county.loc[:, [y for y in calculation_years]] =\
            fuel_county[[y for y in calculation_years]].multiply(
                    fuel_county.fuel_county_mmbtu, axis=0
                    )
            
    fuel_county = fuel_county.drop('fuel_county_mmbtu', axis=1)
    
    fuel_county.fillna(0, inplace=True)
    
#    for y in years:
#        fuel_county[y] = fuel_county[y] * fuel_county['fuel_county_mmbtu']
#        fuel_county = fuel_county.rename(columns={y: str(y)+'_fuel_county_mmbtu'})
    
    
    fuel_county.set_index(['COUNTY_FIPS', 'NAICS', 'fuel_type'], inplace=True)
    
    return fuel_county