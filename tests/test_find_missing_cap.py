
import pytest
import logging
import pandas as pd
from nei.nei_EF_calculations import NEI


def test_find_missing_cap():

    test_data = pd.read_csv('tests/missing_cap_test_data.csv')

    test_data.rename(
        columns={'design_capacity': 'designCapacity', 'unit_description': 'unitDescription'},
        inplace=True
        )

    nei = NEI()

    test_data = nei.find_missing_cap(test_data)

    found_cap_index = test_data[test_data.designCapacity.notnull()].index

    test_data.loc[found_cap_index, 'found_cap'] = True
    test_data.found_cap.fillna(False, inplace=True)

    assert(all(test_data.cap_avail == test_data.found_cap))