
import pandas as pd
import re
import logginggg 

class Units:
    """Class for creating standardized unit categories and extracting characterization information"""

    def __init__(self):
        
        logging.basicConfig(level=logging.INFO)


    def char_nei_units(self, nei_data):
        """

        Parameters
        ----------

        Returns
        -------
        """

    def char_ghgrp_units(self, ghgrp_data):
        """
        
        Parameters
        ----------

        Returns
        -------


        """

    def make_unit_types(self):
        """"""

    def __init__(self):

        # Combustion unit types
        self._unit_types = [
            'kiln', 'dryer', 'oven', 'furnace',
            'boiler', 'incinerator', 'flare',
            'heater', 'calciner', 'turbine',
            'stove', 'distillation', 'other combustion',
            'engine\s', 'generator', 'oxidizer', 'pump',
            'compressor', 'building heat', 'cupola',
            'PCWD', 'PCWW', 'PCO', 'PCT', 'OFB', 'broil',
            'reciprocating', 'roaster'
            ]

# from Tools class under misc_tools.py
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
        unitTypeStd : str;
            Standardized unit type
        """

        other_boilers = ['PCWD', 'PCWW', 'PCO', 'PCT', 'OFB']

        ut_std = []

        for unit in self._unit_types:

            unit_pattern = re.compile(r'({})'.format(unit), flags=re.IGNORECASE)

            try:
                unit_search = unit_pattern.search(unitType)

            except TypeError:
                continue

            if unit_search:
                ut_std.append(unit)

            else:
                continue

        if any([x in ut_std for x in ['engine\s', 'reciprocating']]):
            ut_std = 'engine'

        elif (len(ut_std) > 1):
            ut_std = 'other combustion'

        elif (len(ut_std) == 0):
            ut_std = 'other'

        elif ut_std[0] == 'calciner':
            ut_std = 'kiln'

        elif ut_std[0] == 'oxidizer':
            ut_std = 'thermal oxidizer'

        elif ut_std[0] == 'buidling heat':
            ut_std = 'other combustion'

        elif ut_std[0] in ['cupola', 'broil']:
            ut_std = 'other combustion'

        elif ut_std[0] == 'roaster':
            ut_std = 'other combustion'

        elif any([x in ut_std[0] for x in other_boilers]):
            ut_std = 'boiler'

        elif ut_std[0] == 'reciprocating':
            ut_std = 'engine'

        else:
            ut_std = ut_std[0]

        return ut_std

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
    

# GHGRP unit type. From ghgrp_fac_unit.py
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