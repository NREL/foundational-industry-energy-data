
import pytest
import logging
import pandas as pd
import numpy as np
from fied_compilation import separate_unit_data
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char


def import_input_data():

    input_data = {}

    input_data['nei_data'] = pd.read_csv(
        "nei_data_formatted.csv",
        low_memory=False
        )

    input_data['frs_data'] = pd.read_csv(
        "./data/FRS/frs_data_formatted.csv",
        low_memory=False
        )

    ghgrp_energy_file = "ghgrp_energy_20230508-1606.parquet"
    input_data['ghgrp_unit_data'] = \
        GHGRP_unit_char(ghgrp_energy_file, 2017).main()

    return input_data


def make_test_data(input_data):

    test_data = separate_unit_data(
        input_data['frs_data'],
        input_data['nei_data'],
        input_data['ghgrp_unit_data']
        )

    return test_data


input_data = import_input_data()
test_data = make_test_data(input_data)


def test_separate_unit_data():

    assert np.array_equiv(
            test_data['nei_shared'].registryID.values,
            test_data['ghgrp_shared'].registryID.values
            )

    # Test that registry ids are being separated appropriately, i.e., 
    # "only" ids shouldn't appear in "shared" ids"
    assert len(np.intersect1d(
        test_data['nei_only'].registryID.unique(),
        test_data['ghgrp_only'].registryID.unique()
        )) == 0

    assert len(np.intersect1d(
        test_data['nei_only'].registryID.unique(),
        test_data['nei_shared'].registryID.unique()
        )) == 0

    assert len(np.intersect1d(
        test_data['ghgrp_only'].registryID.unique(),
        test_data['ghgrp_shared'].registryID.unique()
        )) == 0

    ind_len = 0

    for k in test_data.keys():
        ind_len = ind_len + len(test_data[k].registryID.unique())

    # test that all registry IDs are being accounted for in
    # separated DataFrames
    assert ind_len == len(test_data['frs_data'].registryID.unique())

