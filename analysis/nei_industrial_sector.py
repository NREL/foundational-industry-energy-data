"""

Combine the 2 NEI downloads and save as NEI industrial sector data

"""

import pandas as pd


# read NEI 2017 downloads for point sources (all 10 EPA regions b/t 2 files)
# download 'point' source: https://www.epa.gov/air-emissions-inventories/2017-national-emissions-inventory-nei-data
nei1 = pd.read_csv('point_12345.csv')
nei2 = pd.read_csv('point_678910.csv')


# read EnviroFacts crosswalk file for EIS facility ID to FRS facility ID
xwalk = pd.read_csv('EnvirofactsRestAPI.CSV')

xwalk = xwalk[['EIS_FACILITY_ID', 'FRS_FACILITY_ID']]

xwalk.columns = xwalk.columns.str.lower()

# Another way: get FRS facility ID from matching lat/lon of FRS data

#frs_ind = pd.read_csv('EPA_REGISTRY_ID_ALL_INDUSTRY.csv')

#frs_loc = frs_ind[['REGISTRY_ID','LATITUDE83','LONGITUDE83']].rename(
#    columns={'LATITUDE83':'site_latitude','LONGITUDE83':'site_longitude'})
# consider keeping NAICS_CODES as well

#frs_loc = frs_loc.drop_duplicates(subset=['REGISTRY_ID']).dropna()

#nei_merged = pd.merge(nei_ind, frs_loc,
#                      left_on=['site_latitude','site_longitude'],
#                      right_on=['LATITUDE83','LONGITUDE83'],
#                      how='left')


# remove spaces and replace with underscores in column names
nei1.columns = nei1.columns.str.replace(' ','_')

nei1.rename(columns={'calculation_method_':'calculation_method',
                     'fips_state_code':'stfips',
                     'fips_code':'fips',
                     'pollutant_type(s)':'pollutant_type',
                     'epa_region_code':'region'},
            inplace=True)


# unit types & emissions calc methods in nei2 are truncated; 
# match to full unit type names in nei1
full_unit = list(nei1['unit_type'].unique())
partial_unit = list(nei2['unit_type'].unique())

dict_unit={}


for x in partial_unit:
    
    for y in full_unit:
        
        if (x[:5]) in ([item[:5] for item in full_unit]):  
            
            if y.startswith(x):
                dict_unit[partial_unit[partial_unit.index(x)]]=\
                    full_unit[full_unit.index(y)]
           
        else:
            dict_unit[partial_unit[partial_unit.index(x)]]=\
                partial_unit[partial_unit.index(x)]
                
nei2.replace({'unit_type':dict_unit},inplace=True)


# match to full calculation method names in nei1
full_method = list(nei1['calculation_method'].unique())
partial_method = list(nei2['calculation_method'].unique())

dict_method={}
                
for x in partial_method:
    
    for y in full_method:
        
        if (x[:9]) in ([item[:9] for item in full_method]):  
            
            if y.startswith(x):
                dict_method[partial_method[partial_method.index(x)]]=\
                    full_method[full_method.index(y)]
           
        else:
            dict_method[partial_method[partial_method.index(x)]]=\
                partial_method[partial_method.index(x)]



nei2.replace({'calculation_method':dict_method},inplace=True)


nei = pd.concat([nei1,nei2])




# match FRS facility IDs to EIS facility IDs
nei_ind = pd.merge(nei_ind, xwalk, on='eis_facility_id', how='left')

# save file
nei_ind.to_csv('nei_industry.csv',index=False)

