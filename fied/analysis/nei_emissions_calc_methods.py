"""

Check emissions calculation methods in NEI

"""

import pandas as pd
import matplotlib.pyplot as plt


plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.sans-serif'] = "Arial"


# read in NEI industry data
nei_ind = pd.read_csv('nei_industry.csv')


# determine how often each emissions calc method is used per mfg subsector
nei_calc_methods = nei_ind[(nei_ind['naics_sub']//10).isin(
    [31,32,33])].groupby(['naics_sub','calculation_method']).size()


# plot emissions calculation methods
nei_calc_methods.reset_index().rename(columns={0:'count'}).groupby(
    ['naics_sub','calculation_method'])['count'].sum().unstack(
        'calculation_method').plot.bar(stacked=True,legend=True,cmap='tab20')
        
plt.legend(bbox_to_anchor=(1, 1))

        
# get percentage of emissions calculation methods by subsector
perc = nei_ind[(nei_ind['naics_sub']//10).isin([31,32,33])].groupby(
    'naics_sub',as_index=False)['calculation_method'].value_counts(
        normalize=True)

# get percentage of "engineering judgement" as the calc method      
perc[perc['calculation_method'].str.contains('Engineer')].groupby(
    'naics_sub',as_index=False)['proportion'].sum()