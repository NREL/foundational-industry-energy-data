
import pandas as pd
import os
import zipfile
import json
import requests
from io import BytesIO
from pyxlsb import open_workbook
import logging


class GHGRP_unit_char():

    def __init__(self, ghgrp_energy_file, reporting_year):

        logging.basicConfig(level=logging.INFO)

        self._ghgrp_unit_url = 'https://www.epa.gov/system/files/other-files/2022-10/emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip'

        self._data_dir = os.path.abspath('./data/GHGRP/')

        self._ghgrp_energy_file = ghgrp_energy_file

        self._data_source = 'GHGRP'

        self._reporting_year = reporting_year

        def import_data_schema(data_source):
            """
            Import data schema for relevant data set.

            Parameters
            ----------
            data_source : str; "NEI", "GHGRP", "QPC", "FRS"
                Source of data

            Returns
            -------
            self._data_schema : dict

            """

            with open('./nei/extracted_data_schema.json') as file:
                data_schema = json.load(file)
            data_schema = data_schema[0][data_source]

            return data_schema

        self._data_schema = import_data_schema(self._data_source)

    def download_unit_data(self):
        """
        Download and unzip GHGRP unit data.

        Returns
        -------
        file_path : str
            File path of unit data spreadsheet.
        """

        r = requests.get(self._ghgrp_unit_url)

        with zipfile.ZipFile(BytesIO(r.content)) as zf:
            file_path = os.path.join(self._data_dir, zf.namelist()[0])

            if os.path.exists(file_path):
                pass

            else:
                zf.extractall(self._data_dir)

        return file_path

    # TODO fix up code for getting capacity data
    def get_unit_capacity(self, ghgrp_df):
        """
        Retrieve unit capacity data from EPA GHGRP data file.

        Parameters
        ----------
        ghgrp_df : pandas.DataFrame
            Dataframe from GHGRP energy calculations with
            UNIT_TYPE column updated from OCS to a specific
            unit type.

        Returns
        -------
        unit_capacity : pandas.DataFrame
            Dataframe with unit capacity information by Facility ID and
            unit name.

        """

        unit_data_file_path = self.download_unit_data()

        # engine='pyxlsb' not working with python 3.6.5.final.0 and pandas 0.24.2
        # XLRDError: Excel 2007 xlsb file; not supported
        # ghgrp_units = pd.read_excel(
        #     unit_data_file_path,
        #     engine='pyxlsb', sheet_name='UNIT_DATA'
        #     )

        df = []
        with open_workbook(unit_data_file_path) as wb:
            with wb.get_sheet('UNIT_DATA') as sheet:
                for row in sheet.rows():
                    df.append([item.v for item in row])

        ghgrp_ind = pd.DataFrame(df[7:], columns=df[6])

        ghgrp_ind.update(
            ghgrp_ind['Primary NAICS Code'].astype(int),
            overwrite=True
            )

        # Select entries that are industrial facilities and 
        # for reporting years that match GHGRP energy data years
        ghgrp_ind = ghgrp_ind.where(
            (ghgrp_ind['Primary NAICS Code'].apply(
                lambda x: str(x)[0:2] in ['11', '21', '23', '31', '32', '33']
                )) &
            (ghgrp_ind['Reporting Year'].isin(ghgrp_df.REPORTING_YEAR))
            ).dropna(how='all')

        ghgrp_ind.update(
            ghgrp_ind['Unit Maximum Rated Heat Input (mmBTU/hr)'].replace(
                {'': None}
                )
            )

        ghgrp_df = pd.merge(
            ghgrp_df,
            ghgrp_ind[[
                'Reporting Year', 'Facility Id',
                'Unit Maximum Rated Heat Input (mmBTU/hr)', 'Unit Name',
                'FRS Id']],
            left_on=['REPORTING_YEAR', 'FACILITY_ID', 'UNIT_NAME'],
            right_on=['Reporting Year', 'Facility Id', 'Unit Name'],
            how='left', indicator=True
            )

        ghgrp_df.drop(
            ['Reporting Year', 'Facility Id', 'Unit Name'], axis=1, 
            inplace=True
            )

        ghgrp_df.rename(columns={
            'Unit Maximum Rated Heat Input (mmBTU/hr)': 'MAX_CAP_MMBTU_per_HOUR',
            'FRS Id': 'FRS_REGISTRY_ID'
            }, inplace=True
            )

        return ghgrp_df

    def format_ghgrp_df(self, ghgrp_df):
        """
        Formatting (e.g., dropping columns, aggregating fuel types)
        for GHGRP energy estimates, which now include unit capacity data.
        """

        ghgrp_df.loc[:, 'FUEL_TYPE_FINAL'] = pd.concat(
            [ghgrp_df[c].dropna() for c in ['FUEL_TYPE', 'FUEL_TYPE_BLEND', 'FUEL_TYPE_OTHER']],
            axis=0, ignore_index=False
            )

        ghgrp_df = ghgrp_df.query("REPORTING_YEAR==@self._reporting_year")

        # Aggregate. Units may combust multiple types of 
        # fuels and have multiple observations (estimates)
        # of energy use.
        ghgrp_df = ghgrp_df.groupby(
            ['FACILITY_ID', 'REPORTING_YEAR',
                'FUEL_TYPE_FINAL', 'UNIT_NAME',
                'FRS_REGISTRY_ID', 'UNIT_TYPE',
                'MAX_CAP_MMBTU_per_HOUR'], as_index=False
            ).TJ_TOTAL.sum()

        ghgrp_df.loc[:, "energyMJ"] = ghgrp_df.TJ_TOTAL * 10**6

        ghgrp_df.loc[:, 'designCapacity'] = None

        for item in ghgrp_df.MAX_CAP_MMBTU_per_HOUR.iteritems():

            try:
                ghgrp_df.loc[item[0], 'designCapacity'] = item[1]*0.2931  # Convert to MW

            except TypeError:
                logging.error(f"Can't convert this: {item[1]}")
                ghgrp_df.loc[item[0], 'designCapacity'] = None

            else:
                continue

        # ghgrp_df.loc[:, 'designCapacity'] = \
        #     ghgrp_df.MAX_CAP_MMBTU_per_HOUR * 0.2931  # Convert to MW
        ghgrp_df = pd.concat(
            [ghgrp_df, pd.Series('MW', index=ghgrp_df.index, name='designCapacityUOM')],
            axis=1, ignore_index=False
            )

        ghgrp_df.drop(["REPORTING_YEAR", "TJ_TOTAL", 'MAX_CAP_MMBTU_per_HOUR'],
                      axis=1, inplace=True)

        ghgrp_df.rename(columns={
            'FACILITY_ID': 'ghgrpID',
            'FUEL_TYPE_FINAL': 'fuelType',
            'UNIT_NAME': 'unitName',
            'FRS_REGISTRY_ID': 'registryID',
            'UNIT_TYPE': 'unitType'
            }, inplace=True)

        return ghgrp_df

    def get_unit_type(self):
        """
        Use unit name to deterimine unit type for
        unit types that are defined as OCS (Other combustion source).


        Returns
        -------
        ghgrp_df : pandas.DataFrame
            Dataframe from GHGRP energy calculations with
            UNIT_TYPE column updated from OCS to a specific
            unit type.
        """

        ghgrp_df = pd.read_parquet(
            os.path.join(self._data_dir, self._ghgrp_energy_file)
            )

        types = [
            'furnace', 'kiln', 'dryer', 'heater',
            'oven', 'calciner', 'stove', 'htr', 'furn',
            'cupola', 'boiler', 'turbine', 'building heat', 'space heater',
            'engine', 'compressor', 'pump', 'rice', 'generator',
            'hot water', 'crane', 'water heater',
            'comfort heater', 'RTO', 'TODF', 'oxidizer', 'RCO'
            ]

        ocs_units = ghgrp_df.query(
            "UNIT_TYPE == 'OCS (Other combustion source)'"
            ).UNIT_NAME

        ocs_units = ocs_units.str.lower()

        logging.info(
            f'There are {len(ocs_units)} units '
            f'or {len(ocs_units)/len(ghgrp_df):.1%} labelled as OCS'
            )

        # Assume boilers will be the most typical combustion unit type
        # pd.Series.str.find returns -1 where a string is not found
        # Not perfect, as approach assigns "boiler" to units that are 
        # aggregations, e.g., "GP-1 Boilers / Afterburners"
        named_units = pd.concat(
            [pd.Series(ocs_units.str.find(t), name=t) for t in types],
            axis=1, ignore_index=False
            )

        # Matched will show as NaN
        named_units = named_units.where(named_units == -1)

        for c in named_units.columns:
            named_units[c].fillna(c, inplace=True)

        named_units.replace(
            {
                'furn': 'furnace', 'htr': 'heater',
                'hot water': 'water heater',
                'rice': 'engine', 'comfort heater': 'space heater'
            }, inplace=True
        )

        named_units = named_units.where(named_units != -1)
        named_units = named_units.apply(lambda x: x.dropna(), axis=0)

        sing_types = named_units.count(axis=1)
        sing_types = sing_types.where(sing_types == 1).dropna()
        sing_types = pd.DataFrame(
            named_units.loc[sing_types.index, :]
            )

        mult_types = named_units.count(axis=1)
        mult_types = mult_types.where(mult_types > 1).dropna()
        mult_types = pd.DataFrame(
            named_units.loc[mult_types.index, :]
            )
        mult_types['unit_type_iden'] = False

        ocs_units = pd.concat(
            [ocs_units,
             pd.Series(index=ocs_units.index, name='unit_type_iden')],
            axis=1
            )

        # TODO why isn't sing_types.apply(lambda x: x.dropna()), result_type='reduce')
        # returning a series? Should be a faster approach than this loop
        for i in sing_types.index:
            ocs_units.loc[i, 'unit_type_iden'] = \
                sing_types.loc[i, :].dropna().values[0]

        for t in ['boiler', 'furnace', 'kiln', 'calciner', 'dryer', 'stove',
                  'space heater', 'water heater', 'turbine', 'generator',
                  'engine', 'cupola', 'compressor', 'pump', 'building heat',
                  'space heater', 'oxidizer']:

            mult_types.unit_type_iden.update(
                mult_types[t]
                )

            ocs_units.unit_type_iden.update(
                mult_types.unit_type_iden
                )

            mult_types = mult_types.where(
                mult_types.unit_type_iden != t
                ).dropna(subset=['unit_type_iden'])

        ghgrp_df.UNIT_TYPE.update(ocs_units.unit_type_iden)

        return ghgrp_df

    def main(self):

        ghgrp_df = self.get_unit_type()
        ghgrp_df = self.get_unit_capacity(ghgrp_df)
        ghgrp_df = self.format_ghgrp_df(ghgrp_df)

        ghgrp_df.to_csv(
            os.path.join(self._data_dir, 
                         self._ghgrp_energy_file.split('.')[0]
                         )+'_unittype_final.csv'
            )

        return ghgrp_df


if __name__ == '__main__':
    ghgrp_energy_file = 'ghgrp_energy_20221223-1455.parquet'
    reporting_year = 2017
    ghgrp_df = GHGRP_unit_char(ghgrp_energy_file, reporting_year).main()
