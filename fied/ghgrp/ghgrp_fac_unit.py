
import pandas as pd
import os
import zipfile
import yaml
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

    def load_fueltype_dict(self):
        """
        Opens and loads a yaml that specifies the mapping of
        GHGRP fuel types to standard fuel types that have
        aready been applied to NEI data.

        Returns
        -------
        fuel_dict : dictionary
            Dictionary of mappings between GHGRP fuel types and
            generic fuel types that have been applied to NEI data.
        """

        with open('./tools/type_standardization.yml', 'r') as file:
            docs = yaml.safe_load_all(file)

            for i, d in enumerate(docs):
                if i == 0:
                    fuel_dict = d
                else:
                    continue

        return fuel_dict

    # #TODO make into a tools method
    def harmonize_fuel_type(self, ghgrp_unit_data, fuel_type_column):
        """
        Applies fuel type mapping to fuel types reported under GHGRP.

        Parameters
        ----------
        ghgrp_unit_data : pandas.DataFrame

        fuel_type_column : str
            Name of column containing fuel types.

        Returns
        -------
        ghgrp_unit_data : pandas.DataFrame

        """

        fuel_dict = self.load_fueltype_dict()

        ghgrp_unit_data.loc[:, 'fuelTypeStd'] = ghgrp_unit_data[fuel_type_column].map(fuel_dict)

        # drop any fuelTypes that are null
        ghgrp_unit_data = ghgrp_unit_data.where(
            ghgrp_unit_data[fuel_type_column] != 'None'
            ).dropna(how='all')

        return ghgrp_unit_data

    def download_unit_data(self):
        """
        Download and unzip GHGRP unit data. 

        Returns
        -------
        file_path : str
            File path of unit data spreadsheet.
        """

        zipfile_name = self._ghgrp_unit_url.split('/')[-1]

        file_path = os.path.join(self._data_dir, zipfile_name)

        if os.path.isfile(file_path):

            with zipfile.ZipFile(file_path) as zf:

                xlsbpath = os.path.join(self._data_dir, zf.namelist()[0])

                if os.path.isfile(xlsbpath):

                    pass

                else:
                    zf.extractall(self._data_dir)

        else:

            try:
                r = requests.get(self._ghgrp_unit_url)
                r.raise_for_status()

            except requests.exceptions.HTTPError as e:
                print(e)

                print(
                    f"Try downloading zipfile from {self._ghgrp_unit_url} and saving to {self._data_dir}"
                    )

            with zipfile.ZipFile(BytesIO(r.content)) as zf:

                zf.extract(zf.namelist()[0], self._data_dir)

                xlsbpath = os.path.join(self._data_dir, zf.namelist()[0])

        return xlsbpath

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
        try:
            ghgrp_ind = pd.read_excel(
                unit_data_file_path,
                engine='pyxlsb', sheet_name='UNIT_DATA',
                skiprows=6
                )
            
        except XLRDError as e:
            logging.error(f"{e}")

        else:

            df = []
            with open_workbook(unit_data_file_path) as wb:
                with wb.get_sheet('UNIT_DATA') as sheet:
                    for row in sheet.rows():
                        df.append([item.v for item in row])

            # Make sure to catch column row (first few rows of spreadsheet has
            # values only in first column)
            n = 0
            while all(df[n]) is False:
                n+=1

            ghgrp_ind = pd.DataFrame(df[n+1:], columns=df[n])


        ghgrp_ind.columns = [x.strip() for x in ghgrp_ind.columns]

        ghgrp_ind.update(
            ghgrp_ind['Primary NAICS Code'].astype(int),
            overwrite=True
            )

        # Industrial facilities aleady selected in FRS data.
        ghgrp_ind = ghgrp_ind.where(
            ghgrp_ind['Reporting Year'].isin(ghgrp_df.REPORTING_YEAR)
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
    
    # GHG emissions in the GHGRP unit file are aggregated to unit and not fuel type and unit, which is what is
    # needed.

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

        # Harmonize fuel types for GHGRP data
        ghgrp_df = self.harmonize_fuel_type(ghgrp_df, 'FUEL_TYPE_FINAL')

        ghgrp_df.to_csv('ghgrp_emissions.csv')

        # Aggregate. Units may combust multiple types of 
        # fuels and have multiple observations (estimates)
        # of energy use.
        # NOTE that the GHG emissions total here when summed to
        # unit type may not match the GHG emissions by unit type 
        # found in the GHGRP unit data xlsb file.
        ghgrp_df = ghgrp_df.groupby(
            ['FACILITY_ID', 'FRS_REGISTRY_ID', 'REPORTING_YEAR',
             'FUEL_TYPE_FINAL', 'fuelTypeStd', 'UNIT_NAME',
             'UNIT_TYPE', 'MAX_CAP_MMBTU_per_HOUR'], as_index=False
             )[['TJ_TOTAL', 'MTCO2e_TOTAL']].sum()

        ghgrp_df.loc[:, "energyMJ"] = ghgrp_df.TJ_TOTAL * 10**6

        ghgrp_df.loc[:, 'designCapacity'] = None

        for item in ghgrp_df.MAX_CAP_MMBTU_per_HOUR.iteritems():

            try:
                ghgrp_df.loc[item[0], 'designCapacity'] = item[1]*0.2931  # Convert to MW

            except TypeError:
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
            'UNIT_TYPE': 'unitType',
            'MTCO2e_TOTAL': 'ghgsTonneCO2e'
            }, inplace=True)

        ghgrp_df.registryID.update(ghgrp_df.registryID.astype(float))

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
    # ghgrp_energy_file = 'ghgrp_energy_20240110-1837.parquet'
    ghgrp_energy_file = 'all_ghgrp_energy.parquet'
    reporting_year = 2022
    ghgrp_df = GHGRP_unit_char(ghgrp_energy_file, reporting_year).main()
    ghgrp_df.to_csv('formatted_ghgrp_unit_data_2022_DECARB.csv')
