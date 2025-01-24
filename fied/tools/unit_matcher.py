
import pandas as pd
import numpy as np
import re
import os
import yaml
import pathlib
import logging 

class Units:
    """
    Class for creating standardized, 2-level unit categories and extracting characterization information.
    The unit types shown in the table below reprsent the first level of characterization. These act
    as the standardized types, with the second level providing additional detail.

        .. csv-table:: Level 1 Unit Types (unitTypeLv1)
            :header: "Unit Type"
    
            "Boiler"
            "Furnace"
            "Heater"
            "Dryer"
            "Kiln"
            "Internal combustion engine"'
            "Oven"
            "Combined cycle"
            "Turbine"
            "Other combustion"
            "Other"
    """

    def __init__(self):
        
        logging.basicConfig(level=logging.INFO)

        self._unit_types_lv1 = [
            "Boiler",
            "Furnace",
            "Heater",
            "Dryer",
            "Kiln",
            "Internal combustion engine",
            "Oven",
            "Combined cycle",
            "Turbine",
            "Other combustion",
            "Other"  # represents units that are not combustion units, such as material handling equipment
            ]
        
        # YAML that contains GHGRP-specific unit types, both standard (EPA defined, default unit types)
        # and non-standard (custom unit types that are gleaned from the UNIT_NAME field in GHGRP data)
        self._ghgrp_unittypes_path = pathlib.Path(__file__).parents[1]/"tools/ghgrp_unit_types.yaml"

        self._ghgrp_ut_dict = {}

        with open(self._ghgrp_unittypes_path, 'r') as file:
            uts = list(yaml.safe_load_all(file))
            self._ghgrp_ut_dict['std'] = uts[0]
            self._ghgrp_ut_dict['nonstd'] = uts[1]


    def char_nei_units(self, nei_data):
        """

        Parameters
        ----------

        Returns
        -------
        """

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

            if self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv2']:

                final_types = np.array(
                    [[self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv1'],
                      self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv2']]]
                    )
            else:

                final_types = np.array(
                    [[self._ghgrp_ut_dict['nonstd'][ut_std[0]]['unitTypeLv1'],
                    unitType]]
                    )

        return final_types

# from nei_EF_calculations.py
    def unit_type_selection(self, series):
        """
        Algorithm for selecting unit type between NEI (unit_type), SCC, and NEI (unit_description).
        Preference unit types extracted from unit_description, even when NEI unit_type and
        SCC unit types agree.

        Parameters
        ----------
        series : pandas.Series
            Series containing columns of 'nei_unit_type_std', 'scc_unit_type_std', 'desc_unit_type_std',
            'nei_unit_type', 'scc_unit_type', 'unit_description'.

        Returns
        -------
        ut : str or float
            Returns selected unit type. May be np.nan (float).
        
        """

        if (series['nei_unit_type_std'] == 'other'):

            if (series['scc_unit_type_std'] == 'other'):

                if(series['desc_unit_type_std'] == 'other'):

                    ut = series['nei_unit_type']

                elif type(series['desc_unit_type_std']) is float:  #capturing NaN value

                    ut = series['nei_unit_type']

                else:

                    ut = series['unit_description']

                return ut

            elif type(series['scc_unit_type_std']) is float:

                if type(series['desc_unit_type_std']) is float:

                    ut = series['nei_unit_type']

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['unit_description']

                else:

                    ut = series['unit_description']

                return ut

            else:

                if type(series['desc_unit_type_std']) is float:

                    ut = series['scc_unit_type']

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['scc_unit_type']

                else:

                    ut = series['unit_description']

                return ut

        elif type(series['nei_unit_type_std']) is float:

            if (series['scc_unit_type_std'] == 'other'):

                if (series['desc_unit_type_std'] == 'other'):

                    ut = series['unit_description']

                elif type(series['desc_unit_type_std']) is float:

                    ut = series['scc_unit_type']

                else:
                    ut = series['unit_description']

                return ut

            elif type(series['scc_unit_type_std']) is float:

                if type(series['desc_unit_type_std']) is float:

                    ut = np.nan

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['unit_description']

                else:

                    ut = series['unit_description']
                
                return ut

            else:

                if type(series['desc_unit_type_std']) is float:

                    ut = series['scc_unit_type']

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['scc_unit_type']

                else:

                    ut = series['unit_description']

                return ut

        else:

            if (series['scc_unit_type_std'] == 'other'):

                if (series['desc_unit_type_std'] == 'other'):

                    ut = series['nei_unit_type']

                elif type(series['desc_unit_type_std']) is float:

                    ut = series['nei_unit_type']

                else:

                    ut = series['unit_description']

                return ut

            elif type(series['scc_unit_type_std']) is float:

                if type(series['desc_unit_type_std']) is float:

                    ut = series['nei_unit_type']

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['nei_unit_type']

                else:

                    ut = series['unit_description']

                return ut

            else:

                if type(series['desc_unit_type_std']) is float:

                    ut = series['nei_unit_type']

                elif (series['desc_unit_type_std'] == 'other'):

                    ut = series['nei_unit_type']

                else:

                    if (series['nei_unit_type_std'] == series['scc_unit_type_std'] == series['desc_unit_type_std']):

                        ut = series['nei_unit_type']
                    
                    elif (series['scc_unit_type_std'] == series['desc_unit_type_std']):
                    
                        ut = series['scc_unit_type']

                    else:
                        ut = series['unit_description']

                return ut
        
        return ut

    def assign_types(self, nei, iden_scc):
        """
        Assign unit type and fuel type based on NEI and SCC descriptions

        Paramters
        ---------
        nei : pandas.DataFrame
            Raw NEI data.

        iden_scc : pandas.DataFrame
            Processed SCCs that identify industrial units that use energy
            and process materials (for throughput estimates).

        Returns
        -------
        nei : pandas.DataFrame
            Raw NEI data with corresponding SCC unit information. 
        """

        # merge SCC descriptions of unit and fuel types with NEI SCCs
        nei = nei.merge(
            iden_scc[['SCC', 'scc_unit_type', 'scc_fuel_type']],
            left_on='scc',
            right_on='SCC',
            how='left'
            )
        
        nei.rename(columns={'unit_type': 'nei_unit_type'}, inplace=True)
        
        # Also look for unit types in unit_description
        nei.loc[:, 'desc_unit_type_std'] = nei.unit_description.dropna().apply(
            lambda x: self.unit_regex(x)
            )

        # Remove non-combustion, non-electricity unit types
        nei = self.remove_unit_types(nei)
        
        # unit_desc types are already "standardized." Do same for nei and scc types.
        for c in ['nei_unit_type', 'scc_unit_type']:

            unit_map = nei[c].dropna().drop_duplicates().copy(deep=True)
            unit_map = pd.concat([unit_map, unit_map.apply(lambda x: self.unit_regex(x))], axis=1)
            unit_map.columns = ['ut', 'ut_std']
            unit_map = dict(unit_map.values)

            nei.loc[:, f'{c}_std'] = nei[c].dropna().map(unit_map)
            
        units = nei[['nei_unit_type_std', 'scc_unit_type_std', 'desc_unit_type_std', 'nei_unit_type', 'scc_unit_type', 'unit_description']].copy(deep=True)

        units.drop_duplicates(inplace=True)  # Accounts for duplicates due to multiple pollutant types per unique eis_unit_id

        units.loc[:, 'unit_type_final'] = units.apply(lambda x: self.unit_type_selection(x), axis=1)

        units = pd.merge(units, nei[['eis_unit_id']], how='inner', left_index=True, right_index=True)

        # Multiple unit types may be assigned to a uniuqe unit id
        mult_types = units.groupby('eis_unit_id').apply(lambda x: len(x.unit_type_final.unique()))
        mult_types = mult_types[mult_types > 1]

        units.set_index('eis_unit_id', inplace=True)

        for i in mult_types.index:

            nei_ut_std = units.xs(i).nei_unit_type_std.drop_duplicates().dropna().values.tolist()  # All units should have only one nei_unit_type
            scc_ut_std = units.xs(i).scc_unit_type_std.drop_duplicates().dropna().values.tolist()

            if (nei_ut_std[0] != 'other'):

                fut = units.xs(i).nei_unit_type.drop_duplicates().dropna().values[0]

            elif len(scc_ut_std)==1:

                if scc_ut_std != 'other':

                    fut = units.xs(i).scc_unit_type.drop_duplicates().dropna().values[0]

                else:

                    fut = units.xs(i).desc_unit_type_std.drop_duplicates().dropna().values[0]

            elif len(scc_ut_std) == 2:

                try: 
                    
                    scc_ut_std.remove('other')

                except ValueError:

                    try:

                        fut = units.xs(i).desc_unit_type_std.drop_duplicates().dropna().values[0]

                    except IndexError:

                        fut = units.xs(i).scc_unit_type.drop_duplicates().dropna().values[-1]  # default to last value

            elif len(scc_ut_std) > 2:

                fut = units.xs(i).desc_unit_type_std.drop_duplicates().dropna().values[0]

            units.loc[i, 'unit_type_final'] = fut

        units.reset_index(inplace=True)
        units = units[['eis_unit_id', 'unit_type_final']].drop_duplicates()  

        nei = pd.merge(nei, units, on='eis_unit_id', how='left')

        nei.drop(['nei_unit_type_std', 'scc_unit_type_std', 'desc_unit_type_std'], axis=1, inplace=True)
        
        # get fuel types from NEI text and SCC descriptions
        for c in ['unit_description', 'process_description', 'scc_fuel_type']:
            nei.loc[:, c] = nei[c].str.lower()

        nei.loc[:, 'fuel_type'] = nei.loc[:, 'scc_fuel_type']

        # Use MATERIAL field as fuel type
        materials = {
            x:x.lower() for x in ['Natural Gas', 'Heat', 
                          'Process Gas', 'Diesel/Kerosene', 'Sawdust', 'Methane',
                          'Gas', 'Gasoline', 'Refuse', 'Solid Waste']
            }

        nei_materials = pd.DataFrame(nei.query("MATERIAL.notnull()", engine='python'))
        nei_materials.loc[:, 'fuel_type'] = nei_materials.MATERIAL.map(materials)
        nei.fuel_type.update(nei_materials.fuel_type)

        # #TODO too many fuel type standardizations happening SCC -> NEI -> std fuel types
        # Should streamline this process to avoid errors.
        # clean up identified fuel types
        fuels = {k:None for k in nei.fuel_type.dropna().unique()}

        for f in fuels.keys():

            if f.find('(') != -1:
    
                fre = f.replace(r'(', r'\(').replace(r')', r'\)')

                n = {k: re.search(fre, k) for k in self._unit_conv['fuel_dict'].keys()}

            else:

                n = {k: re.search(k, f) for k in self._unit_conv['fuel_dict'].keys()}

            if any(n.values()):

                fk = [k for k in n.keys() if n[k] is not None]

                # There may be multiple matches
                if len(fk) > 1:

                    mask = [f in x for x in fk]

                    fuels[f] = self._unit_conv['fuel_dict'][np.array(fk)[mask][0]]

                else:

                    fuels[f] = self._unit_conv['fuel_dict'][fk[0]] 

            else:
                fuels[f] = f

        nei.loc[:, 'fuel_type'] = nei.fuel_type.map(fuels, na_action='ignore')

        nei_no_scc_ft = pd.DataFrame(nei[nei.scc_fuel_type.isnull()])

        for f in self._unit_conv['fuel_dict'].keys():

            # search for fuel types listed in NEI unit/process descriptions
            nei_no_scc_ft.loc[(nei_no_scc_ft['unit_description'].str.contains(f, na=False)) |
                              (nei_no_scc_ft['process_description'].str.contains(f, na=False)),
                        'fuel_type'] = self._unit_conv['fuel_dict'][f]

            # search for the same fuel types listed in SCC
            nei_no_scc_ft.loc[(nei_no_scc_ft['fuel_type'].isnull()) &
                              (nei_no_scc_ft['scc_fuel_type'].str.contains(f, na=False)),
                    'fuel_type'] = self._unit_conv['fuel_dict'][f]

        nei.fuel_type.update(nei_no_scc_ft.fuel_type)

        return nei
    

