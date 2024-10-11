"""

Plot NEI unit type breakdown by 6dig NAICS and facility

"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors
import seaborn as sns
import re


# read in NEI industry data
nei_ind = pd.read_csv('nei_industry.csv')

# read in SCC matching data
scc_id = pd.read_csv('scc_descriptions.csv') #idd_scc.csv




# convert units of CO2 emissions to mtCO2e
def convert_emissions(nei_df):
    
    em_conv = {'Carbon Dioxide':0.907185,
               'Methane':0.907185*29.8,
               'Nitrous Oxide':0.907185*273}
    # 1 ton = 0.907185 metric ton ; GWP 100-year IPCC AR6
    
    
    
    nei_df.loc[nei_df.pollutant_desc.str.contains('Carbon Dioxide'),
               'emissions_mtCO2e'] = \
        nei_df[nei_df.pollutant_desc.str.contains('Carbon Dioxide')].apply(
            lambda x: x['total_emissions']*em_conv[x['pollutant_desc']],
                        axis=1)
    
    return



# convert capacity units to mmbtu/hr
def convert_capacity_units(nei_df):

    #https://farm-energy.extension.org/energy-conversion-values/
    #https://www.engineeringtoolbox.com/boiler-horsepower-d_1061.html
    unit_conv = {'E6BTU/HR': 1,
                 'E3LB/HR': 1, # 0.970 exact, or 1 for rule of thumb
                 'HP' : 0.0345, # assuming boiler hp
                 'BLRHP' : 0.0345,
                 'LB/HR' : 0.002545,
                 'TON/DAY' : 0.083,
                 'FT3/DAY' : 0.001555*10**-3,
                 'KW' : 3.413*10**-3,
                 'MW' :3.413,
                 'GAL': 0.137381, #1gal heating oil = 137,381Btu
                 'TON/HR' : 2,
                 'BTU/HR' : 10**-6,
                 'DATAMIGR': 1,
                 'DATAMIGRATION': 1,
                 '0':0
                }

    # for capacity values not listed, assign as '0' for calculation
    nei_df.design_capacity.fillna('0',inplace=True)
    nei_df.design_capacity_uom.fillna('0',inplace=True)

    # convert listed capacity units to mmbtu/hr acc. to unit conversions
    nei_df.loc[nei_df.design_capacity_uom.isin(unit_conv.keys()),
               'cap_mmbtuhr'] = \
        nei_df[nei_df.design_capacity_uom.isin(unit_conv.keys())].apply(
                      lambda x: x['design_capacity']*\
                          unit_conv[x['design_capacity_uom']], 
                                  axis=1)

    # find capacity values in other columns, get numerical value
    for i in nei_df[
            (nei_df.design_capacity_uom.str.contains('DATAMIGR'))&
            (nei_df.unit_description.str.contains('MMBTU/HR'))
                       ].index:nei_df.loc[i,'cap_mmbtuhr'] = \
        float(re.findall('\d*\.?\d+', nei_df['unit_description'][i])[0])

    for i in nei_df[
            (nei_df.design_capacity_uom.str.contains('DATAMIGR'))&
            (nei_df.process_description.str.contains('MMBTU/HR'))
                       ].index:nei_df.loc[i,'cap_mmbtuhr'] = \
        float(re.findall('\d*\.?\d+', nei_df['process_description'][i])[0])
        
        
    nei_df['cap_mmbtuhr'] = pd.to_numeric(nei_df['cap_mmbtuhr'],
                                             downcast="float")


    return



# match scc unit type and fuel type descriptors to nei data and merge
def add_scc_unit_info(scc_df, nei_df):

    # remove SCC codes that have letters in them
    #scc_df.drop(scc_df[~scc_df.SCC.str.isnumeric()].index,inplace=True)
    
    #scc_df.rename(columns={'unit_type':'unit_type_scc'},inplace=True)

    scc_df.SCC = scc_df.SCC.astype('int64')

    nei_df = nei_df.merge(scc_df[['SCC','scc_unit_type','scc_fuel_type']],
                               left_on='scc',right_on='SCC', how='left')
    
    nei_df.loc[nei_df.scc_unit_type.isnull(),'scc_unit_type'] = 'other'

    return nei_df



# create plot of count, pollutant emissions, total capacity per unit type
#   for each 6digit naics industry (mfg only) by facility
#   and for each combustion pollutant (CO2, PM, SO2)
def plot_unit_types_breakdown(nei_df):


    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.sans-serif'] = "Arial"
    
    
   
    for p in ['Carbon Dioxide',
              'PM10 Primary (Filt + Cond)',
              'Sulfur Dioxide']:
       
       pol_df = nei_df[nei_df['pollutant_desc']==p].copy()
       
       naics_list = pol_df[
           (pol_df['naics_sub']//10).isin([31,32,33])]['naics_code'].unique()
       

       if p == 'Carbon Dioxide':
           emiss_col = 'emissions_mtCO2e'  #converted TON to mtCO2
           emiss_unit = 'mtCO2'
       else:
           emiss_col = 'total_emissions'   #TON
           emiss_unit = 'ton'
           

       for n in naics_list:
    
            fig, (ax1,ax2,ax3) = plt.subplots(nrows=3,ncols=1,sharex=True)
            fig.suptitle(n,fontsize=20)
            
            figsz = (25,15)
            color_seq = matplotlib.colors.ListedColormap(
                (sns.color_palette("tab20b",20)+\
                             sns.color_palette("tab20",8)))
    
            pol_df[pol_df['naics_code']==n].groupby(
                ['eis_facility_id','scc_unit_type'])['eis_unit_id']\
                .nunique().unstack(level=0).T.plot.bar(
                                    ax=ax1,stacked=True, 
                                    figsize=figsz,
                                    ylabel='Count',
                                    cmap=color_seq,
                                    legend=False)
    
            pol_df[(pol_df['naics_code']==n)].groupby(
                ['eis_facility_id','scc_unit_type'])[emiss_col]\
                .sum().unstack(level=0).T.plot.bar(
                                   ax=ax2,stacked=True, 
                                   figsize=figsz,
                                   ylabel='Emissions, {} ({})'.format(
                                       p,emiss_unit),
                                   cmap=color_seq,
                                   legend=False)
    
            pol_df[pol_df['naics_code']==n].groupby(
                ['eis_facility_id','scc_unit_type'])['cap_mmbtuhr']\
                .sum().unstack(level=0).T.plot.bar(
                                   ax=ax3,stacked=True, 
                                   figsize=figsz,
                                   ylabel='Heat capacity (mmbtu/hr)',
                                   cmap=color_seq,
                                   legend=False)
    
            ax1.legend(loc='upper left',ncol=3)
    
            plt.tight_layout(pad=2.5)
    
            plt.savefig('NEI_unit_types_{}\{}'.format(p,n))
            
            plt.close(fig)
            
            print(n)

    return



convert_emissions(nei_ind)
convert_capacity_units(nei_ind)
nei_ind = add_scc_unit_info(scc_id,nei_ind)


#plot_unit_types_breakdown(nei_ind)





