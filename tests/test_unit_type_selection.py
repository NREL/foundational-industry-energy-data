import pytest
import pandas as pd
import numpy as np
from nei.nei_EF_calculations import NEI



def test_unit_type_selection():

    test_df = pd.DataFrame(
    [['other', 'other', 'other', 'a', 'b', 'c'],  #a
    ['other', 'other', 'boiler', 'a', 'b', 'c'],   #c
    ['other', 'other', np.nan, 'a', 'b', 'c'],  #a
    ['other', 'boiler', 'other', 'a', 'b', 'c'], #b
    ['other', 'boiler', 'boiler', 'a', 'b', 'c'], #b
    ['other', 'boiler', 'oven', 'a', 'b', 'c'],  #b
    ['other', 'boiler', np.nan, 'a', 'b', 'c'], #b
    ['other', np.nan, np.nan, 'a', 'b', 'c'],  #a
    ['other', np.nan, 'boiler', 'a', 'b', 'c'], #c
    ['boiler', 'other', 'boiler', 'a', 'b', 'c'], #a
    ['boiler', 'other', 'oven', 'a', 'b', 'c'], #a
    ['boiler', 'boiler', 'other', 'a', 'b', 'c'], #a
    ['boiler', 'oven', 'other', 'a', 'b', 'c'], #a
    ['boiler', 'boiler', 'boiler', 'a', 'b', 'c'], #a
    ['boiler', 'boiler', 'oven', 'a', 'b', 'c'], #a
    ['boiler', 'boiler', np.nan, 'a', 'b', 'c'], #a
    ['boiler', 'oven', 'oven', 'a', 'b', 'c'], #b
    ['boiler', 'oven', 'furnace', 'a', 'b', 'c'], #a
    ['boiler', 'oven', np.nan, 'a', 'b', 'c'], #a
    ['boiler', 'furnace', 'furnace', 'a', 'b', 'c'], #b
    ['boiler', np.nan, np.nan, 'a', 'b', 'c'], #a
    ['oven', 'oven', 'furnace', 'a', 'b', 'c'], #a
    ['oven', 'furnace', 'furnace', 'a', 'b', 'c'], #b
    ['oven', np.nan, np.nan, 'a', 'b', 'c'], #a
    [np.nan, np.nan, np.nan, 'a', 'b', 'c']], #np.nan
    columns = ['nei_unit_type_std', 'scc_unit_type_std', 'desc_unit_type_std', 'nei_unit_type', 'scc_unit_type', 'unit_description']
    )

    expected_results = ['a', 'c', 'a', 'b', 'b', 'b', 'b', 'a', 'c', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'b', 'a', 'a', 'b', 'a', 'a', 'b', 'a', None]

    results = test_df.apply(lambda x: NEI().unit_type_selection(x), axis=1).to_list()

    assert results == expected_results
    