"""

Calculates unit throughput and energy input (later op hours?) from emissions
    and emissions factors,specifically from: PM, SO2, NOX, VOCs, CO
    
Uses NEI Emissions Factors (EFs) and, if not listed, WebFire EFs

Returns file: 'NEI_unit_throughput_and_energy.csv'

"""

import pandas as pd
import numpy as np
import yaml
import matplotlib.pyplot as plt
import seaborn as sns


nei_ind = pd.read_csv('nei_industry.csv')

webfr = pd.read_csv('webfirefactors.csv')

iden_scc = pd.read_csv('iden_scc.csv')

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
    
    nei.rename(columns={'SCC':'SCC_web'},inplace=True)
    
    return nei


# assign unit type and fuel type based on NEI and SCC descriptions
def get_unit_and_fuel_type(nei, scc):
    
    #scc = scc[scc.SCC.str.isnumeric()]
    
    scc.SCC = scc.SCC.astype('int64')
    
    scc.rename(columns={'unit_type':'scc_unit_type',
                        'fuel_type':'scc_fuel_type'},inplace=True)
    
    
    # merge SCC descriptions of unit and fuel types with NEI SCCs
    nei = nei.merge(scc[['SCC','scc_unit_type','scc_fuel_type']],
                    left_on='scc',right_on='SCC', how='left')
    
    # set unit type equal to SCC unit type if listed as 
    #   'Unclassified' or'Other' in NEI    
    nei.loc[(nei['unit_type']=='Unclassified')|
            (nei['unit_type']=='Other process equipment'),
            'unit_type'] = nei['scc_unit_type']
    
    
    # get fuel types from NEI text and SCC descriptions
    nei.unit_description = nei.unit_description.str.lower()
    nei.process_description = nei.process_description.str.lower()
    nei.scc_fuel_type = nei.scc_fuel_type.str.lower()
    
    
    
    fuel_dict = {'natural gas|natural-gas| ng|-ng':'natural_gas',
                'coal':'coal',
                'diesel|distillate':'distillate_oil',
                'fuel oil|residual oil|crude oil|heavy oil':'residual_oil',
                'lignite':'lignite',
                'coke':'coke',
                'petroleum coke|pet coke':'pet_coke',
                'wood':'wood',
                'bagasse':'bagasse',
                'process gas|waste gas':'process_gas',
                'lpg|liquified petroleum|propane':'LPG',
                'gasoline':'gasoline'}   
    
    
    for f in fuel_dict.keys():
        
        # search for fuel types listed in NEI unit/process descriptions
        nei.loc[(nei['unit_description'].str.contains(f,na=False))|
                (nei['process_description'].str.contains(f,na=False)),
                'fuel_type'] = fuel_dict[f]
        
        # search for the same fuel types listed in SCC
        nei.loc[(nei['fuel_type'].isnull())&
                (nei['scc_fuel_type'].str.contains(f,na=False)),
                'fuel_type'] = fuel_dict[f]
        
    
    return nei


# for throughput calculation,
#   convert NEI emissions to LB; NEI EFs to LB/TON; 
#   and WebFire EFs to LB/TON

# for energy input calculation,
#   convert NEI emissions to LB; NEI EFs to LB/MJ; 
#   and WebFire EFs to LB/MJ

def convert_emissions_units(nei, uc):
    
    # map unit of emissions and EFs in NEI/WebFire to unit conversion key
    
    # unit conversions for total emissions and EF numerators
    unit_to_lb = {'LB':'LB_to_LB',
                'TON':'TON_to_LB',
                'TONS':'TON_to_LB',
                'G':'G_to_LB',
                'KG':'KG_to_LB',
                'MILLIGRM':'MILLIGRM_to_LB',
                 'MG':'MILLIGRM_to_LB'}
    
    # unit conversions for EF denominators (calculating throughput)
    unit_to_ton = {'TON':'TON_to_TON',
                   'LB':'LB_to_TON'}
    
    # unit conversions for EF denominators (calculating energy)
    unit_to_mj = {'E6BTU':'E6BTU_to_MJ',
                  'HP-HR':'HP-HR_to_MJ',
                  'E3HP-HR':'E3HP-HR_to_MJ',
                  'THERM':'THERM_to_MJ',
                  'KW-HR':'KW-HR_to_MJ',
                  'FT3':'FT3_to_MJ',
                  'E3FT3':'E3FT3_to_MJ',
                  'E6FT3':'E6FT3_to_MJ',
                  'E6FT3S':'E6FT3S_to_MJ',
                  'GAL':'GAL_to_MJ',
                  'E3GAL':'E3GAL_to_MJ',
                  'E6GAL':'E6GAL_to_MJ',
                  'LB':'LB_to_MJ',
                  'TON':'TON_to_MJ',
                  'E3BBL':'E3BBL_to_MJ',
                  'E3BDFT':'E3BDFT_to_MJ',
                  'E6BDFT':'E6BDFT_to_MJ'}
    
    
    # NEI--------------------------------------------------
    
    # convert NEI total emissions value to LB
    nei.loc[:,'emissions_conv_fac'] = \
        nei['emissions_uom'].map(unit_to_lb).map(uc['basic_units'])
        
    nei.loc[:,'total_emissions_LB'] = \
        nei['total_emissions'] * nei['emissions_conv_fac']



     # convert NEI emission_factor numerator to LB
    nei.loc[:,'nei_ef_num_fac'] = \
        nei['ef_numerator_uom'].map(unit_to_lb).map(uc['basic_units'])
        

    # convert NEI emission_factor to LB/TON for throughput
    nei.loc[:,'nei_ef_denom_fac'] = \
        nei['ef_denominator_uom'].map(unit_to_ton).map(uc['basic_units'])
    
    nei.loc[:,'nei_ef_LB_per_TON'] = \
        nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_ef_denom_fac']
        
        
    
    # convert NEI emission_factor to LB/MJ for energy input
    for f in nei.fuel_type.dropna().unique():
       
       nei.loc[nei.fuel_type==f,'nei_denom_fuel_fac'] = \
           nei['ef_denominator_uom'].map(unit_to_mj).map(
               unit_conv['energy_units'][f])
           
    # if there is no fuel type listed, 
    #   use energy to energy units only OR assume NG for E6FT3
    nei.loc[(nei.fuel_type.isnull()) &
            ((nei.ef_denominator_uom=='E6BTU')|
             (nei.ef_denominator_uom=='HP-HR')|
             (nei.ef_denominator_uom=='THERM')|
             (nei.ef_denominator_uom=='E6FT3')),'nei_denom_fuel_fac'] = \
        nei['ef_denominator_uom'].map(unit_to_mj).map(
            unit_conv['energy_units']['natural_gas'])
        
    nei['nei_denom_fuel_fac'] = nei['nei_denom_fuel_fac'].astype(float)
    
    nei.loc[:,'nei_ef_LB_per_MJ'] = \
        nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_denom_fuel_fac']
    
    
    
    # WebFire----------------------------------------------
    
    nei['UNIT'] = nei['UNIT'].str.upper()
    
    nei['FACTOR'] = pd.to_numeric(nei['FACTOR'],errors='coerce')
    
    # replace WebFire EF denominator units (MEASURE) with NEI units
    measure_dict = {'Tons':'TON',
                    'Pounds':'LB',
                    'Million Standard Cubic Feet':'E6FT3',
                    'Million Btus':'E6BTU',
                    '1000 Gallons':'E3GAL',
                    'Megagrams':'MEGAGRAM',
                    'Million Gallons':'E6GAL',
                    'Gallons':'GAL',
                    '1000 Pounds':'E3LB',
                    '1000 Barrels':'E3BBL',
                    'Dry Standard Cubic Feet':'FT3',
                    'Million Dry Standard Cubic Feet':'E6FT3',
                    'Standard Cubic Feet':'FT3',
                    '1000 Cubic Feet':'E3FT3',
                    'MMBtu':'E6BTU',
                    '1000 Horsepower-Hours':'E3HP-HR'}
    
    nei.replace({'MEASURE': measure_dict}, inplace=True)
    
    
    # convert WebFire EF numerator to LB
    nei.loc[:,'web_ef_num_fac'] = \
        nei['UNIT'].map(unit_to_lb).map(uc['basic_units'])

    # convert WebFire EF to LB/TON for throughput
    nei.loc[:,'web_ef_denom_fac'] = \
        nei['MEASURE'].map(unit_to_ton).map(uc['basic_units'])
    
    
    nei.loc[:,'web_ef_LB_per_TON'] = \
        nei['FACTOR']*nei['web_ef_num_fac']/nei['web_ef_denom_fac']
        
    
    
    # convert WebFire EF to LB/E6BTU for energy input
    for f in nei.fuel_type.dropna().unique():
       
       nei.loc[nei.fuel_type==f,'web_denom_fuel_fac'] = \
           nei['MEASURE'].map(unit_to_mj).map(
               unit_conv['energy_units'][f])
           
           
     # if there is no fuel type listed, 
     #   use energy to energy units only OR assume NG for E6FT3
    nei.loc[(nei.fuel_type.isnull()) &
            (nei.MEASURE=='E6BTU')|
            (nei.MEASURE=='HP-HR')|
            (nei.MEASURE=='THERM')|
            (nei.MEASURE=='E6FT3'),'web_denom_fuel_fac'] = \
        nei['MEASURE'].map(unit_to_mj).map(
            unit_conv['energy_units']['natural_gas'])
        
    nei['web_denom_fuel_fac'] = nei['web_denom_fuel_fac'].astype(float)
    
    nei.loc[:,'web_ef_LB_per_MJ'] = \
        nei['FACTOR']*nei['web_ef_num_fac']/nei['web_denom_fuel_fac']
    
    
    return



# calculate throughput quantity in TON and energy input in MJ
def calculate_unit_throughput_and_energy(nei):
    
    
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
    
    nei.loc[(nei['nei_ef_LB_per_MJ']>0),'energy_MJ'] = \
        nei['total_emissions_LB'] / nei['nei_ef_LB_per_MJ']
    
        
    
    # if there is not an NEI EF, use WebFire EF
    nei.loc[(nei['nei_ef_LB_per_TON'].isnull()) & 
            (nei['web_ef_LB_per_TON']>0),'throughput_TON'] = \
        nei['total_emissions_LB'] / nei['web_ef_LB_per_TON'] 
    
    nei.loc[(nei['nei_ef_LB_per_MJ']>0),'energy_MJ'] = \
        nei['total_emissions_LB'] / nei['nei_ef_LB_per_MJ']
    
        
    
    # remove throughput_TON if WebFire ACTION is listed as Burned
    nei.loc[(~nei['throughput_TON'].isnull()) & 
            (nei['ACTION']=='Burned'),'throughput_TON'] = np.nan
    
            

    return


# plot difference between max and min throughput_TON quanitites for unit
#   when there are multiple emissions per unit
def plot_throughput_difference(nei):

    duplic = \
        nei[(nei.throughput_TON>0) &
            (nei.eis_process_id.duplicated(keep=False)==True)].groupby(
                ['eis_process_id']).agg(
                    perc_diff=('throughput_TON',
                               lambda x: ((x.max()-x.min())/x.mean())*100)
                    ).reset_index()

    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.sans-serif'] = "Arial"                     

    sns.histplot(data=duplic, x="perc_diff")
    plt.xlabel('Percentage difference')
    plt.ylabel('Units')

    return


# plot difference between max and min energy_MJ quanitites for unit
#   when there are multiple emissions per unit
def plot_energy_difference(nei):
    
    duplic = \
        nei[(nei.energy_MJ>0) &
            (nei.eis_process_id.duplicated(keep=False)==True)].groupby(
                ['eis_process_id']).agg(
                    perc_diff=('energy_MJ',
                               lambda x: ((x.max()-x.min())/x.mean())*100)
                    ).reset_index()
    
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.sans-serif'] = "Arial"                     
   
    sns.histplot(data=duplic, x="perc_diff") # sns.kdeplot
    plt.xlabel('Percentage difference')
    plt.ylabel('Units')
    
    return



# use the median throughput_TON and energy_MJ for individual unit 
#   when there are multiple emissions per unit
def get_median_throughput_and_energy(nei):
    
    
    med_unit = nei[(nei['throughput_TON']>0)|(nei['energy_MJ']>0)
                   ].groupby(['eis_process_id',
                              'eis_facility_id',
                              'naics_code',
                              'naics_sub',
                              'unit_type',
                              'fuel_type'
                              ],dropna=False
                              )[['throughput_TON',
                                 'energy_MJ']].median().reset_index()
    
                                 
    # MATERIAL and ACTION columns can have multiple values for same
    #   eis_process_id so if included in groupby, need to remove duplicates
                            
    #med_unit.drop(med_unit[
    #    (med_unit.eis_process_id.duplicated(keep=False)==True) & 
    #    (med_unit.MATERIAL.isnull())].index, inplace=True)
    
    
    med_unit.to_csv('NEI_unit_throughput_and_energy.csv',index=False)
    
    
    return
    



nei_emiss = match_webfire_to_nei(nei_emiss, webfr)
nei_emiss = get_unit_and_fuel_type(nei_emiss, iden_scc)
convert_emissions_units(nei_emiss, unit_conv)
calculate_unit_throughput_and_energy(nei_emiss)
get_median_throughput_and_energy(nei_emiss)





    # check the difference between max and min throughput for single unit
    #nei_emiss[(nei_emiss.throughput_TON>0) & 
    #          (nei_emiss.eis_process_id.duplicated(keep=False)==True)].groupby(
    #              ['eis_unit_id','eis_process_id']
    #              )['throughput_TON'].agg(np.ptp).describe()
    
    # compare units that had throughput calculated vs all units
    #(nei_emiss[nei_emiss.throughput_TON>0].groupby(
    #    ['naics_sub'])['eis_process_id'].count()/nei_emiss.groupby(
    #        ['naics_sub'])['eis_process_id'].count())




    
