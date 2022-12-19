"""

Plot GHGRP unit type breakdwon by 6dig NAICS and facility

"""

import pandas as pd
import matplotlib.pyplot as plt


# GHGRP (Emissions by Unit and Fuel Type) - add process emissions later?
ghgrp = pd.read_excel('GHGRP_emissions_by_unit_and_fuel_type.xlsb',
                      engine='pyxlsb',sheet_name='UNIT_DATA')

ghgrp_ind = ghgrp[(ghgrp['Primary NAICS Code']//10000).isin(
    [11,21,23,31,32,33])].copy()

ghgrp_ind['Unit Type'] = ghgrp_ind['Unit Type'].str.replace('.','',regex=True)


# take most recent year, 2021
ghgrp_21 = ghgrp_ind[ghgrp_ind['Reporting Year']==2021].copy()

ghgrp_21.loc[:,'naics_sub'] = ghgrp_21[
    'Primary NAICS Code'].astype(str).str[:3].astype(int)



# get list of unique 6 digit naics industries
mfg_naics = ghgrp_21[(ghgrp_21['Primary NAICS Code']//10000).isin(
        [31,32,33])]['Primary NAICS Code'].unique()



ghgrp_mfg = ghgrp_21[(ghgrp_21['Primary NAICS Code']//10000).isin(
    [31,32,33])].copy()


# get list of unique unit types in manufacturing industries
mfg_units = ghgrp_mfg['Unit Type'].unique()

mfg_units = mfg_units[~pd.isnull(mfg_units)]




# plot bar chart of count, emissions & capacity of unit types 
#   by facility, for each 6dig naics
def plot_unit_types_breakdown(naics_list, ghgrp_df):
    
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.sans-serif'] = "Arial"
    
    for n in naics_list:
    
        
        fig, (ax1,ax2,ax3) = plt.subplots(nrows=3,ncols=1,sharex=True)
        fig.suptitle(n,fontsize=20)
        
        ghgrp_df[ghgrp_df['Primary NAICS Code']==n].groupby(
            ['Facility Id','Unit Type']).size().unstack(level=0).T.plot.bar(
                ax=ax1,stacked=True, 
                figsize=(25,15),
                ylabel='Count',
                cmap='tab20',
                legend=False)
                
                
        ghgrp_df[ghgrp_df['Primary NAICS Code']==n].groupby(
            ['Facility Id',
             'Unit Type'])['Unit CO2 emissions (non-biogenic) '
                           ].sum().unstack(level=0).T.plot.bar(
                               ax=ax2,stacked=True, 
                               figsize=(25,15),
                               ylabel='Emissions (mtCO2)',
                               cmap='tab20',
                               legend=False)
            
        ghgrp_df[ghgrp_df['Primary NAICS Code']==n].groupby(
            ['Facility Id',
             'Unit Type'])['Unit Maximum Rated Heat Input (mmBTU/hr)'
                           ].sum().unstack(level=0).T.plot.bar(
                               ax=ax3,stacked=True, 
                               figsize=(25,15),
                               ylabel='Max heat capacity (MMBtu/hr)',
                               cmap='tab20',
                               legend=False)
       
        ax1.legend(loc='upper left',ncol=3)
        
        plt.tight_layout(pad=2.5)
    
        plt.savefig('GHGRP_naics_unit_type\{}'.format(n))
    
    return


# plot bar chart of count, emissions, & capacity by naics, for each unit type
def plot_industries_by_unit(unit_list, ghgrp_df):

    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.sans-serif'] = "Arial"
    figsz = (16,8)
    col = 'steelblue'
    


    for n in unit_list:


        fig, (ax1,ax2,ax3) = plt.subplots(nrows=3,ncols=1,sharex=True)
        fig.suptitle(n,fontsize=20)

        ghgrp_df[ghgrp_df['Unit Type']==n].groupby(
            ['Primary NAICS Code']).size().plot.bar(
                ax=ax1,stacked=True, 
                figsize=figsz,
                ylabel='Count',
                color=col,
                legend=False)


        ghgrp_df[ghgrp_df['Unit Type']==n].groupby(
            ['Primary NAICS Code'])['Unit CO2 emissions (non-biogenic) '
                                    ].sum().plot.bar(
                                        ax=ax2,stacked=True,
                                        figsize=figsz,
                                        ylabel='Emissions (mtCO2)',
                                        color=col,
                                        legend=False)

        ghgrp_df[ghgrp_df['Unit Type']==n].groupby(
            ['Primary NAICS Code'])['Unit Maximum Rated Heat Input (mmBTU/hr)'
                                    ].sum().plot.bar(
                                        ax=ax3,stacked=True,
                                        figsize=figsz,
                                        ylabel='Max heat capacity (MMBtu/hr)',
                                        color=col,
                                        legend=False)


        plt.xticks(rotation=90)

        plt.tight_layout(pad=1)

        plt.savefig('GHGRP_unit_types\{}'.format(n))
        

    return

plot_unit_types_breakdown(mfg_naics, ghgrp_21)
plot_industries_by_unit(mfg_units, ghgrp_mfg)
        
