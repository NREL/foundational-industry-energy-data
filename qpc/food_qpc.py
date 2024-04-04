

from census_qpc import QPC
import pandas as pd
import sys
sys.path.append(r'c:/users/cmcmilla/foundational-industry-energy-data')


start_year = 2011
end_year = 2022

qpc_meth = QPC()

qpc_data = pd.concat(
    [qpc_meth.get_qpc_data(y) for y in range(start_year, end_year+1)],
    axis=0, ignore_index=True
    )

qpc_data.loc[:, 'n3'] = qpc_data.NAICS.apply(
    lambda x: int(str(x)[0:3])
    )

qpc_data = qpc_data[qpc_data.n3 == 311]
qpc_data.to_csv('c:/users/cmcmilla/industry-musings/OnSite/food_qpc.csv')