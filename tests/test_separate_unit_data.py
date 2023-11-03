
import pytest
import logging
import pandas as pd
import numpy as np
from fied_compilation import separate_unit_data
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char


def import_input_data():

    input_data = {}

    input_data['nei_data'] = pd.read_csv(
        "./data/NEI/nei_ind_data.csv",
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


def test_separate_unit_data(test_data):

    assert all(
        np.sort(
            test_data['nei_shared'].registryID
            ) == np.sort(
                test_data['ghgrp_shared'].registryID
                )
        )
