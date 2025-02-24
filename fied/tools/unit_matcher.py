
import pandas as pd
import numpy as np
import re
import yaml
import pathlib
import logging
import pdb

class UnitsFuels:
    """
    Class for creating standardized, 2-level unit and fuel categories and extracting characterization information.
    The unit types shown in the table below represent the first level of characterization. These act
    as the standardized types, with the second level providing additional detail.

        .. csv-table:: Level 1 Unit Types (unitTypeLv1)
            :header: "Unit Type"
    
            "Boiler"
            "Calciner"
            "Dryer"
            "Fryer"
            "Furnace"
            "Incinerator"
            "Internal combustion engine"
            "Kiln"
            "Oven"
            "Process heater"
            "Space heater"
            "Thermal oxidizer"
            "Turbine"
            "Other combustion"
            "Other"

The standardized, Level 1 fuel types are shown below.

        .. csv-table:: Level 1 Fuel Types
            :header: "Fuel Type"
    
            "Coal and coke"
            "Hydrocarbon gasl liquids"
            "Natural gas"
            "Petroleum products"
            "Biomass"
            "Other"
    """

    def __init__(self):
        
        logging.basicConfig(level=logging.INFO)

        self._unit_types_lv1 = [
            "Boiler",
            "Calciner",
            "Dryer",
            "Fryer",
            "Furnace",
            "Incinerator",
            "Internal combustion engine",
            "Kiln",
            "Oven",
            "Process heater",
            "Space heater",
            "Thermal oxidizer",
            "Turbine",
            "Other combustion",
            "Other" # represents units that are not combustion units, such as material handling equipment
            ]
        
        # YAML that contains GHGRP-specific unit types, both standard (EPA defined, default unit types)
        # and non-standard (custom unit types that are gleaned from the UNIT_NAME field in GHGRP data)
        self._ghgrp_unittypes_path = pathlib.Path(__file__).parents[1]/"tools/ghgrp_unit_types.yaml"
        
        # YAML that contains scc-specific unit types, as well as their mapping to level 1 and level 2 unit types.
        self._nei_unittypes_path = pathlib.Path(__file__).parents[1]/"tools/scc_unit_types.yaml"

        # YAML that contains fuel types
        self._fuel_types_path = pathlib.Path(__file__).parents[1]/"tools/fuel_type_standardization.yaml"

        self._ghgrp_ut_dict = {}

        with open(self._ghgrp_unittypes_path, 'r') as file:
            uts = list(yaml.safe_load_all(file))
            self._ghgrp_ut_dict['std'] = uts[0]
            self._ghgrp_ut_dict['nonstd'] = uts[1]

        with open(self._nei_unittypes_path, 'r') as file:
            self._nei_uts = yaml.safe_load(file)

        with open(self._fuel_types_path, 'r') as file:
            self._all_fuel_types = yaml.safe_load(file)

    def char_nei_units(self, nei_unit):
        """
        Characterizes a unit type with a standardized level 1 name.

        Parameters
        ----------
        nei_unit : str
            Name of unit type taken from SCC or NEI data.

        Returns
        -------
        unit_types : list
            List of standardized level 1 and level 2

        """

        matched = [re.search(r'{nei_unit}', y) for y in self._nei_uts.keys()]

        matched = [x for x in matched if x != None]

        if len(matched) == 1:

            try:
                ut1 = self._nei_uts[matched[0]]['unitTypeLv1']
                ut2 = self._nei_uts[matched[0]]['unitTypeLv2']

            except KeyError as e:
                logging.error(f"Type not in _nei_uts: {e}")

        elif len(matched) > 1:
            ut1, ut2 = 'Other combustion', matched[0]
 
        else:
            ut1, ut2 = "Other", nei_unit
        
        unit_types = [ut1, ut2]

        return unit_types


    def char_remaining_nei(self, r, data):
        """

        Parameters
        ----------
        r : namedtuple
            DataFrame row as a namedtuple, generated from the iterator Pandas itertuples method. 

        data : pandas.DataFrame
            DataFrame of NEI data. 

        Returns
        -------
        all_types : list
            List of strings with the order of [unitTypeLv1, unitTypeLv2, fuelTypeLv1, fuelTypeLv2].

        """
 
        ut = r[data.columns.to_list().index('scc_level_four') + 1]

        if ': ' in ut:
            ut_a, ut_b = ut.split(': ', maxsplit=1)

            if 'ing' in ut_a:
                ut1, ut2 = self.char_nei_units(ut_b)

            else:
                ut1, ut2 = self.char_nei_units(ut_a)

            ut = ut_b
    
        else:

            ut1, ut2 = self.char_nei_units(ut)
    
        if 'fired' in ut.lower():

            try:

                ft1, ft2 = self.match_fuel_type(
                    re.search(
                        r'(\w+ \w+)(?=-fired)|(\w+)(?=-fired)|(\w+ \w+ \w+)(?=-fired)', 
                        ut
                        ).group()
                        )

            except AttributeError:

                try:
                    ft = re.search(
                        r'cbm|nat gas|natural gas|distillate oil|residual oil|#2 oil|#6 oil|propane|coal|process gas', 
                        ut.lower()
                        ).group()

                except AttributeError:

                    if 'fired' in ut.lower():
                        ft1, ft2 = self.match_fuel_type(ut.lower().split(' fired')[0])

                    elif 'direct' in ut.lower():
                        try:
                            ft1, ft2 = self.match_fuel_type(ut.lower().split('direct ')[1])

                        except IndexError:
                            ft1, ft2 = np.nan, np.nan

                        else:
                            if ut == 'ng':
                                ft1, ft2 = self.match_fuel_type('Natural Gas')

                            else:
                                pass

                    elif ('and' in ut.lower()) | ('or' in ut.lower()):
                        ft1, ft2 = self.match_fuel_type(ut.split(' ')[1])

                    # Assume that "...Gas-Fired..." equipment refers to natural gas.
                    elif ut.lower() == 'gas':
                        ft1, ft2 = self.match_fuel_type('Natural Gas')

                    elif ut.lower() == 'oil':
                        ft1, ft2 = self.match_fuel_types('Residual Fuel Oil')

                else:
                    ft1, ft2 = self.match_fuel_type(ft.lower())

        else:
            if 'diesel' in ut.lower():
                ft1, ft2 = self.match_fuel_type('Diesel')

            elif ':' in ut: 

                try:
                    x, y = ut.split(': ')

                except ValueError:
                    ft1, ft2 = np.nan, np.nan

                else:

                    if any([z in x for z in ['Distillate', 'Residual', 'Gas',
                                            'Liquid', 'Propane']]):
    
                        ft1, ft2 = self.match_fuel_type(x)

                        ut1, ut2 = self.char_nei_units(y)

                    elif any([z in y for z in ['Distillate', 'Residual', 'Gas',
                                            'Liquid', 'Propane']]):
    
                        ft1, ft2 = self.match_fuel_type(y)

                        ut1, ut2 = self.char_nei_units(x)

                    else:
                        ft1, ft2 = np.nan, np.nan

            else:
                ft1, ft2 = np.nan, np.nan

        all_types = [ut1, ut2, ft1, ft2]        

        return all_types

    def match_fuel_type(self, ft):
        """
        Match fuel type to an entry in all fuel types yaml. Returns
        standardized level 1 and level 2 fuel types.

        Parameters
        ----------
        ft : str
            Fuel type

        Returns
        -------
        ft1, ft2 : str
            Standardized level 1 and level 2 fuel types.

        Raises
        ------
        KeyError
            If the fuel type is not included in the all fuel types yaml.
        """

        try:
            ft1 = self._all_fuel_types[ft]['fuelTypeLv1']
            ft2 = self._all_fuel_types[ft]['fuelTypeLv2']

        except KeyError as e:
            logging.error(f"{e}, fuel type: {ft}")

            return "Other", ft

        else:
            return ft1, ft2

    def char_ghgrp_units(self, ghgrp_data):
        """
        Characterize and standardize GHGRP-reported unit types.

        Parameters
        ----------
        ghgrp_data : pandas.DataFrame
            DataFrame of GHGRP data with derived energy use.


        Returns
        -------
        ghgrp_data : pandas.DataFrame
            DataFrame of GHGRP data with added columns for standardized unit types,
            `unitTypeLv1` and `unitTypeLv2`. 

        """

        std_uts = pd.DataFrame.from_dict(
            self._ghgrp_ut_dict['std'], 
            orient='index'
            ).reset_index()
        
        std_uts.rename(columns={'index': 'UNIT_TYPE'}, inplace=True)

        # Issue with section character for coke oven battery combustion stacks
        ghgrp_data['COB'] = ghgrp_data.UNIT_TYPE.dropna().apply(
            lambda x: x[0:3] == 'COB'
            )
        
        ghgrp_data.loc[ghgrp_data[ghgrp_data.COB == True].index, 'UNIT_TYPE'] = 'COB (By-product recovery coke oven battery combustion stacks)'
        
        ghgrp_data.drop(['COB'], axis=1, inplace=True)

        ghgrp_data = pd.merge(
            ghgrp_data, 
            std_uts,
            on='UNIT_TYPE',
            how='left'
            )
        
        # Check that EPA has not defined new standard unit types
        if not ghgrp_data[(ghgrp_data.UNIT_TYPE.notnull()) & (ghgrp_data.unitTypeLv1.isnull())].empty:
            ghgrp_data[(ghgrp_data.UNIT_TYPE.notnull()) & (ghgrp_data.unitTypeLv1.isnull())].UNIT_TYPE.drop_duplicates().to_pickle("new_ghgrp_units.pkl")
            raise AssertionError("Check for new standard EPA unit types.\nExporting pkl with these unit types")
                        
        
        # Try to identify unit type from UNIT_NAME field for OCS and NaN UNIT_TYPE entries
        ocs_types = ghgrp_data[ghgrp_data.UNIT_TYPE == 'OCS (Other combustion source)']
        none_types = ghgrp_data[ghgrp_data.UNIT_TYPE.isnull()]

        for df in [ocs_types, none_types]:
    
            found_types = df.UNIT_NAME.apply(lambda x: self.unit_regex(x))

            found_types_df = pd.concat(
                [pd.DataFrame(v, index=[i]) for i, v in found_types.items()],
                axis=0
                )

            found_types_df.columns=['unitTypeLv1', 'unitTypeLv2']

            ghgrp_data.update(found_types_df)

        return ghgrp_data

    def unit_regex(self, unitType):
        """
        Use regex to standardize unit types,
        where appropriate. See unit_types variable
        for included types.

        Parameters
        ----------
        unitType : str
            Detailed unit type

        Returns
        -------
        final_types : numpy.array
            Array of strings for standardized unit types for the first and second level of unit description.
        """

        ut_std = []

        for ut in self._ghgrp_ut_dict['nonstd'].keys():
        
            unit_pattern = re.compile(r'({})'.format(ut), flags=re.IGNORECASE)

            try:
                unit_search = unit_pattern.search(unitType)

            except TypeError:
                continue

            if unit_search:
                ut_std.append(ut)

            else:
                continue

        if (len(ut_std) > 1) | (len(ut_std)==0):
            final_types = np.array([['Other combustion', 'Other combustion']])

        else:
            # if self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv2']:

            final_types = np.array(
                [[self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv1'],
                    self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv2']]]
                )
            # else:
            #     final_types = np.array(
            #         [[self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv1'],
            #         unitType]]
            #         )

        return final_types


    

