
import pytest
import logging
import os
import pandas as pd
import numpy as np
from ghgrp.ghgrp_fac_unit import GHGRP_unit_char


tpath = os.path.join(
    './data/GHGRP/',
    'emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip'
    )

ghgrp_energy_file = 'ghgrp_energy_20230508-1606.parquet'
reporting_year = 2017

def test_download_unit_data():

    method_path = GHGRP_unit_char(
        ghgrp_energy_file, reporting_year
        ).download_unit_data()

    abspath = os.path.abspath(tpath)

    assert(abspath == method_path)


test_download_unit_data()