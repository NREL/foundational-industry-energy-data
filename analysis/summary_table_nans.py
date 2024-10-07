

import pandas as pd 


table_data

summary_counts = pd.DataFrame(index=['Value', 'NAN', 'frac_NAN'])

for c in ['registryID', 'unitTypeStd', 'fuelTypeStd', 'designCapacity', 'eisFacilityID',
          'ghgrpID', 'energyMJ', 'energyMJq2', 'ghgsTonneCO2e', 'ghgsTonneCO2eQ2']:
    
    c_name = c
    
    if c == 'registryID':

        c_name = "unique_{c}"
        nan_frac = 0
    
        counts_val = len(table_data.dropna(subset=[c])[c].unique())
        counts_nan = table_data[table_data[c].isnull()].registryID.count()

    else:

        counts_val = table_data.dropna(subset=[c])[c].count()
        counts_nan = table_data[table_data[c].isnull()].registryID.count()

        try:
            assert(counts_val + counts_nan == len(table_data))

        except:
            print("totals don't match")

            break

        else:

            nan_frac = counts_nan/len(table_data)

    summary_counts.loc[:, c] = [counts_val, counts_nan, nan_frac]

summary_counts.to_csv('c:/users/cmcmilla/desktop/summary_counts.csv')

# test = (summary_counts.style\
#     .applymap(lambda x: x.format("{:.2%}"), subset=pd.IndexSlice["frac_NAN", :])\
#     .applymap(lambda x: x.format('{:.0%}'), subset=pd.IndexSlice[["Value", "NAN"], :])
#     .to_excel("test_styled.xlsx", engine="openpyxl"))



# summary_colunts.style.format({
#     'frac_NAN': '{:.2%}', 
#     'Value': '{:.0f}', 
#     'NAN': '{:.0f}'
#     }
#     )
#     subset=pd.IndexSlice["frac_NAN", :])

    