
import pandas as pd
import os
import pytest
import logging
from nei.nei_unit_characterizationpy import id_external_combustion


class TestID:
    def __init__(self):

        self._scc_data = pd.read_csv(
            './scc/SCCDownload-2022-1205-161322.csv',
            index_col='SCC')

        self._scc_data.columns = [x.replace(' ', '_') for x in self._scc_data.columns]

    def test_external_combustion(self):
        """
        
        """

        logging.basicConfig(level=logging.INFO)

        # logging.basicConfig(
        #     filename='/tests/test_external_combustion.log',
        #     filemode='w',
        #     level=logging.INFO
        #     )

        scc_ec = self._scc_data.query('scc_level_one=="External Combustion"')

        scc_ec_unit_info = pd.DataFrame(columns=['unit_type', 'fuel_type'])
        scc_ec_unit_info = pd.concat(
                [scc_ec_unit_info, scc_ec.apply(
                    lambda x: id_external_combustion(
                        x.scc_level_two,
                        x.scc_level_three,
                        x.scc_level_four
                    ), axis=1
                )], axis=0
            )

        print(scc_ec_unit_info.head())

        logging.info(f'info: {scc_ec_unit_info}')

        scc_ec = pd.concat(
            [scc_ec[
                [f'scc_level_{l}' for l in ['one', 'two', 'three', 'four']]
                ], scc_ec_unit_info], axis=1
            )


        logging.info(f'{scc_ec}')

if __name__ == '__main__':
    tid = TestID()
    tid.test_external_combustion()
