
import pandas as pd
import os
import zipfile
import requests
from io import BytesIO
from pyxlsb import open_workbook
import logging

# code to take raw output from GHGRP energy calculations and:
# * create facility-level summaries of energy and emissions by fuel type
# * crate unit-level descriptions (indexed by FACILITY_ID) of combustion fuel use and emissions

# TODO fix up code for getting capacity data

class GHGRP_unit_char():

    def __init__(self):

        logging.basicConfig(level=logging.INFO)

        self._ghgrp_unit_url = 'https://www.epa.gov/system/files/other-files/2022-10/emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip'

        self._data_dir = os.path.abspath('./data/GHGRP/')

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
            file_path = os.path.join(self._data_dir, zf.namelist([0]))

            if os.path.exists(file_path):
                pass

            else:
                zf.extractall(self._data_dir)

        return file_path


    def get_unit_capacity(self, years):
        """
        Retrieve unit capacity data from EPA GHGRP data file.


        Parameters
        ----------
        years : list of integer(s)
            Years for which to return combustion unit capacity information.


        Returns
        -------
        unit_capacity : pandas.DataFrame
            Dataframe with unit capacity information by Facility ID and
            unit name. 

        """

        unit_data_file_path = self.download_unit_data()

        # engine='pyxlsb' not workingn with pythong 3.6.5.final.0 and pandas 0.24.2
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

        ghgrp_ind = ghgrp_units[(ghgrp_units['Primary NAICS Code']//10000).isin(
            [11, 21, 23, 31, 32, 33]
            )].copy()

        ghgrp_ind['Unit Type'] = \
            ghgrp_ind['Unit Type'].str.replace('.', '', regex=True)





        mfg_units = mfg_units[~pd.isnull(mfg_units)]

    def unit_type_regex(self, unit_name):
        """
        Use unit name to deterimine unit type.

        Parameters
        ----------
        unit_name : str
            Unit name

        Returns
        -------
        unit_type : str
            Type of combustion unit based on unit name
        """

    def unit_type_find(self, ghgrp_df):
        """
        Use unit name to deterimine unit type for
        unit types that are defined as OCS (Other combustion source).

        Parameters
        ----------
        ghgrp_df : pandas.DataFrame
            Dataframe from GHGRP energy calculations, including columns
            for UNIT_TYPE and UNIT_NAME.

        Returns
        -------
        ghgrp_df : pandas.DataFrame
            Dataframe from GHGRP energy calculations with
            UNIT_TYPE column updated from OCS to a specific
            unit type.
        """

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
            f'There are {len(ocs_units)} units'
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
        sing_types = sing_types.where(mult_types == 1).dropna()
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


    def calc_enduse(self, eu_fraction_dict, county_energy_dd, temps=False):
        """
        Calculates energy by end use based on unit type reported in GHGRP
        data and MECS end use data.
        Returns Dask DataFrame
        """
        unitname_eu_dict = {
            'Process Heating': ['furnace', 'kiln', 'dryer', 'heater',
                                'oven', 'calciner', 'stove', 'htr', 'furn',
                                'cupola'],
            'Conventional Boiler Use': ['boiler'],
            'CHP and/or Cogeneration Process': ['turbine'],
            'Facility HVAC': ['building heat', 'space heater'],
            'Machine Drive': ['engine', 'compressor', 'pump', 'rice'],
            'Conventional Electricity Generation': ['generator'],
            'Other Nonprocess Use': ['hot water', 'crane', 'water heater',
                                     'comfort heater', 'RTO', 'TODF',
                                     'oxidizer', 'RCO']
                }

        unittype_eu_dict = {
            'Process Heating': ['F', 'PD', 'K', 'PRH', 'O', 'NGLH', 'CF',
                                'HMH', 'C', 'HPPU', 'CatH', 'COB', 'FeFL',
                                'IFCE', 'Pulp Mill Lime Kiln', 'Lime Kiln',
                                'Direct Reduction Furnace',
                                'Sulfur Recovery Plant'],
            'Conventional Boiler Use': ['OB', 'S', 'PCWW', 'BFB', 'PCWD',
                                        'PCT', 'CFB', 'PCO', 'OFB', 'PFB'],
            'CHP and/or Cogeneration Process': ['CCCT', 'SCCT',
                                                'Chemical Recovery Furnace',
                                                'Chemical Recovery Combustion Unit'],
            'Facility HVAC': ['CH'],
            'Other Nonprocess Use': ['HWH', 'TODF', 'ICI', 'FLR', 'RTO',
                                     'II', 'MWC', 'Flare', 'RCO'],
            'Conventional Electricity Generation': ['RICE',
                                                    'Electricity Generator']
                }

        def eu_dict_to_df(eu_dict):
            """
            Convert unit type/unit name dictionaries to dataframes.
            """
            eu_df = pd.DataFrame.from_dict(
                    eu_dict, orient='index'
                    ).reset_index()

            eu_df = pd.melt(
                    eu_df, id_vars='index', value_name='unit'
                    ).rename(columns={'index': 'end_use'}).drop(
                            'variable', axis=1
                            )

            eu_df = eu_df.dropna().set_index('unit')

            return eu_df

        def eu_unit_type(unit_type, unittype_eu_df):
            """
            Match GHGRP unit type to end use specified in unittype_eu_dict.
            """

            enduse = re.match('(\w+) \(', unit_type)

            if enduse is not None:
                enduse = re.match('(\w+)', enduse.group())[0]
                if enduse in unittype_eu_df.index:
                    enduse = unittype_eu_df.loc[enduse, 'end_use']
                else:
                    enduse = np.nan
            else:
                if unit_type in unittype_eu_df.index:
                    enduse = unittype_eu_df.loc[unit_type, 'end_use']

            return enduse

        def eu_unit_name(unit_name, unitname_eu_df):
            """
            Find keywords in GHGRP unit name descriptions and match them
            to appropriate end uses based on unitname_eu_dict.
            """

            for i in unitname_eu_df.index:
                enduse = re.search(i, unit_name.lower())
                if enduse is None:
                    continue
                else:
                    enduse = unitname_eu_df.loc[i, 'end_use']
                    return enduse

            enduse = np.nan

            return enduse

        unittype_eu_df = eu_dict_to_df(unittype_eu_dict)
        unitname_eu_df = eu_dict_to_df(unitname_eu_dict)

        # Base ghgrp energy end use disaggregation on reported unit type and
        # unit name.
        eu_ghgrp = self.energy_ghgrp_y.copy(deep=True)

        eu_ghgrp = eu_ghgrp[eu_ghgrp.MECS_NAICS != 0]

        # First match end uses to provided unit types. Most unit types are
        # specified as OCS (other combustion source).
        unit_types = eu_ghgrp.UNIT_TYPE.dropna().unique()

        type_match = list()

        for utype in unit_types:
            enduse = eu_unit_type(utype, unittype_eu_df)
            type_match.append([utype, enduse])

        type_match = pd.DataFrame(type_match,
                                  columns=['UNIT_TYPE', 'end_use'])

        eu_ghgrp = pd.merge(eu_ghgrp, type_match, on='UNIT_TYPE', how='left')

        # Next, match end use by unit name for facilites that report OCS for
        # unit type.
        eu_ocs = eu_ghgrp[
                (eu_ghgrp.UNIT_TYPE == 'OCS (Other combustion source)') |
                (eu_ghgrp.UNIT_TYPE.isnull())
                ][['UNIT_TYPE', 'UNIT_NAME']]

        eu_ocs['end_use'] = eu_ocs.UNIT_NAME.apply(
                lambda x: eu_unit_name(x, unitname_eu_df)
                )

        eu_ghgrp.end_use.update(eu_ocs.end_use)