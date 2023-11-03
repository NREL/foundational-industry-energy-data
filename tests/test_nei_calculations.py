
import pandas as pd
import sys
sys.path.insert(0,'c:/users/cmcmilla/foundational-industry-energy-data/')
# import pytest
from nei.nei_EF_calculations import NEI


def test_calc_unit_energy():
    """
    Test how well reported emission factors 
    do against WebFires Emissions factors for various Calculation
    Method Codes. 
    """
    nei_methods = NEI()
    nei_data = nei_methods.load_nei_data()
    iden_scc = nei_methods.load_scc_unittypes()
    webfr = nei_methods.load_webfires()

    # logging.info("Merging WebFires data...")
    nei_char = nei_methods.match_webfire_to_nei(nei_data, webfr)
    # logging.info("Merging SCC data...")
    nei_char = nei_methods.assign_types_nei(nei_char, iden_scc)
    #  logging.info("Converting emissions units...")
    nei_char = nei_methods.convert_emissions_units(nei_char)
    # logging.info("Estimating throughput and energy...")

    # compare 
    nei_char.loc[:, 'calc_code_comparison'] = \
        nei_char.web_ef_LB_per_MJ.divide(
            nei_char.nei_ef_LB_per_MJ
            ) - 1

    comparison_summary = nei_char[
        nei_char.calc_code_comparison.notnull()
        ].groupby(
            ['calculation_method', 'pollutant_code', 'fuel_type', 'scc']
            ).calc_code_comparison.describe()

    comparison_summary.to_csv('./tests/calc_method_code_comparison.csv')

    return

if __name__ == '__main__':
    test_calc_unit_energy()