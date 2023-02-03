"""

Calculate unit throughput (later, energy input or op hours?) from emissions,
    specifically from: PM, SO2, NOX, VOCs, CO
Use NEI Emissions Factors (EFs) or WebFire EFs per pollutant

"""

import pandas as pd
import numpy as np
import yaml


nei_ind = pd.read_csv('nei_industry.csv')

webfr = pd.read_csv('webfirefactors.csv')

with open(r'unit_conversions.yml') as file:
    unit_conv = yaml.load(file, Loader=yaml.FullLoader)
    

# use only NEI emissions of PM, CO, NOX, SOX, VOC
nei_emiss = nei_ind[
    nei_ind.pollutant_code.str.contains('PM|CO|NOX|NO3|SO2|VOC')].copy()



# match WebFire EF data to NEI data
def match_webfire_to_nei(nei, web):
    
    # remove duplicate EFs for the same pollutant and SCC; keep max EF
    
    web = web.sort_values('FACTOR').drop_duplicates(
        subset=['SCC','NEI_POLLUTANT_CODE'], keep='last')
    
    
    nei = nei.merge(web[['SCC','NEI_POLLUTANT_CODE',
                                  'FACTOR','UNIT','MEASURE',
                                  'MATERIAL','ACTION']],
                         left_on=['scc','pollutant_code'],
                         right_on=['SCC','NEI_POLLUTANT_CODE'],
                         how='left')
    
    return nei




# for throughput calculation,
#   convert NEI emissions to LB; NEI EFs to LB/TON; and WebFire EFs to LB/TON
def convert_emissions_units(nei, uc):
    
    unit_to_lb = {'LB':'LB_to_LB',
                'TON':'TON_to_LB',
                'TONS':'TON_to_LB',
                'G':'G_to_LB',
                'KG':'KG_to_LB',
                'MILLIGRM':'MILLIGRM_to_LB',
                 'MG':'MILLIGRM_to_LB'}
    
    
    unit_to_ton = {'TON':'TON_to_TON',
                   'LB':'LB_to_TON'}
    
    
    unit_to_mmbtu = {'E6BTU':'E6BTU_to_E6BTU',
                     'E6FT3':'E6FT3_to_E6BTU'}
    
    
    
    # NEI--------------------------------------------------
    
    # convert NEI total emissions value to LB
    nei.loc[:,'emissions_conv_fac'] = \
        nei['emissions_uom'].map(unit_to_lb).map(uc['basic_units'])
        
    nei.loc[:,'total_emissions_LB'] = \
        nei['total_emissions'] * nei['emissions_conv_fac']



    # convert NEI emission_factor to LB/TON for throughput
    
    nei.loc[:,'nei_ef_num_fac'] = \
        nei['ef_numerator_uom'].map(unit_to_lb).map(uc['basic_units'])


    
    nei.loc[:,'nei_ef_denom_fac'] = \
        nei['ef_denominator_uom'].map(unit_to_ton).map(uc['basic_units'])
    
    
    nei.loc[:,'nei_ef_LB_per_TON'] = \
        nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_ef_denom_fac']
        
        
    
    # convert NEI emission_factor to LB/E6BTU for energy input
    
    
    
    
    # WebFire----------------------------------------------
    
    nei['UNIT'] = nei['UNIT'].str.upper()
    
    nei['FACTOR'] = pd.to_numeric(nei['FACTOR'],errors='coerce')
    
    # replace WebFire EF denominator units (MEASURE) with NEI units
    measure_dict = {'Tons':'TON',
                    'Million Standard Cubic Feet':'E6FT3',
                    'Million Btus':'E6BTU',
                    '1000 Gallons':'E3GAL',
                    'Megagrams':'MEGAGRAM'}
    
    nei.replace({'MEASURE': measure_dict}, inplace=True)
    
    
    # convert WebFire EF to LB/TON
    
    nei.loc[:,'web_ef_num_fac'] = \
        nei['UNIT'].map(unit_to_lb).map(uc['basic_units'])

    
    nei.loc[:,'web_ef_denom_fac'] = \
        nei['MEASURE'].map(unit_to_ton).map(uc['basic_units'])
    
    
    nei.loc[:,'web_ef_LB_per_TON'] = \
        nei['FACTOR']*nei['web_ef_num_fac']/nei['web_ef_denom_fac']
    
    
    return



# calculate throughput quantity in TON
def calculate_unit_throughput(nei):
    
    
    # check for "Stack Test" emissions factor method where units are wrong
    #   according the to the emission comment text; remove emission factor
    check_nei_ef_idx = ((nei['emission_factor']>0) &
                          (nei['calc_method_code']==4) &
                          (nei['emission_comment'].str.contains(
                              'lb/hr|#/hr|lbs/hr|Lb/hr')) &
                          (nei['ef_denominator_uom']!='HR'))
    
    
    nei.loc[check_nei_ef_idx,'nei_ef_LB_per_TON'] = np.nan
    

    
    # if there is an NEI EF, use NEI EF  
    nei.loc[(nei['nei_ef_LB_per_TON']>0),'throughput_TON'] = \
        nei['total_emissions_LB'] / nei['nei_ef_LB_per_TON']
        
    
    # if there is not an NEI EF, use WebFire EF
    nei.loc[(nei['nei_ef_LB_per_TON'].isnull()) & 
            (nei['web_ef_LB_per_TON']>0),'throughput_TON'] = \
        nei['total_emissions_LB'] / nei['web_ef_LB_per_TON'] 
        
    
    # remove throughput_TON if WebFire ACTION is listed as Burned
    nei.loc[(~nei['throughput_TON'].isnull()) & 
            (nei['ACTION']=='Burned'),'throughput_TON'] = np.nan
    
            

    return



# replace throughput_TON quantity with median of indiv unit 
#   when there are multiple emissions per unit
def get_median_throughput(nei):
    
    med_unit_df = nei[nei['throughput_TON']>0].groupby(
        ['eis_unit_id',
         'eis_process_id',
         'eis_facility_id'])['throughput_TON'].median().reset_index()
    
    med_unit_df.to_csv('NEI_unit_throughput.csv',index=False)
    
    
    return 
    



nei_emiss = match_webfire_to_nei(nei_emiss, webfr)
convert_emissions_units(nei_emiss, unit_conv)
calculate_unit_throughput(nei_emiss)
get_median_throughput(nei_emiss)





    # check the difference between max and min throughput for single unit
    #nei_emiss[(nei_emiss.throughput_TON>0) & 
    #          (nei_emiss.eis_process_id.duplicated()==True)].groupby(
    #              ['eis_unit_id','eis_process_id']
    #              )['throughput_TON'].agg(np.ptp).describe()
    
    # compare units that had throughput calculated vs all units
    #(nei_emiss[nei_emiss.throughput_TON>0].groupby(
    #    ['naics_sub'])['eis_process_id'].count()/nei_emiss.groupby(
    #        ['naics_sub'])['eis_process_id'].count())




    
