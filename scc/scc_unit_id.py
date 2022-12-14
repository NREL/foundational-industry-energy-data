
import pandas as pd
import numpy as np
import re
import logging


class SCC_ID:
    """

    """
    def __init__(self):

        self._scc_data = pd.read_csv(
            'SCCDownload-2022-1205-161322.csv'
            )

        self._scc_data.columns = [c.replace(' ', '_') for c in self._scc_data.columns]

        logging.basicConfig(level=logging.INFO)

    def id_external_combustion(self, scc_level_two, scc_level_three,
                               scc_level_four):
        """
        Method for identifying unit type and fuel type under the
        "External Combustion"
        SCC Level One. See ... for SCC documentation.

        Parameters
        ----------
        scc_level_two : str
            s

        scc_level_three : str

        scc_level_four : str 

        """

        other_fuels = [
            'Shredded', 'Specify Percent Butane in Comments',
            'Specify in Comments', 'Specify Waste Material in Comments',
            'Refuse Derived Fuel', 'Sewage Grease Skimmings', 'Waste Oil',
            'Sludge Waste', 'Digester Gas',
            'Agricultural Byproducts (rice or peanut hulls, shells, cow manure, etc',
            'Paper Pellets', 'Black Liquor', 'Red Liquor',
            'Spent Sulfite Liquor', 'Tall Oil',
            'Wood/Wood Waste Liquid', 'Off-gas Ejectors', 'Pulverized Coal',
            'Salable Animal Fat', 'Natural Gas', 'Process Gas',
            'Wet Slurry', 'Distillate Oil', 'Residual Oil', 'Petroleum Refinery Gas',
            'Grade 4 Oil', 'Grade 5 Oil', 'Grade 6 Oil', 'Blast Furnace Gas',
            'Coke Oven Gas', 'Landfill Gas', 'Biomass Solids'
            ]

        if ':' in scc_level_four:
            unit_type = scc_level_four.split(': ')[1]

            if unit_type in other_fuels:
                unit_type = 'Boiler'

            else:
                pass

        else:
            unit_type = scc_level_four

        if scc_level_four in other_fuels:
            unit_type = 'Boiler'

        else:
            pass

        fuel_type = scc_level_three

        if fuel_type == 'CO Boiler':
            fuel_type = scc_level_four

        else:
            pass

        if fuel_type in ['Industrial', 'Commercial/Institutional']:
            unit_type = scc_level_two
            fuel_type = scc_level_four.split(':')[0]

        else:
            pass

        if unit_type in ['All', 'Butane', 'Propane']:
            unit_type = 'Boiler'

        else:
            pass

        if 'Wood-fired' in unit_type:
            unit_type = 'Boiler'

        else:
            pass

        return unit_type, fuel_type

    def id_ice(self, scc_level_two, scc_level_three, scc_level_four):
        """
        Use SCC levels to identify unit type and fuel type indicated by a SCC code.

        Parameters
        ----------
        scc_level_two : str
            s

        scc_level_three : str

        scc_level_four : str 

        Returns
        -------
        unit_type : str
        fuel_type : str
        """

        if scc_level_two in ['Electric Generation', 'Industrial', 'Commercial/Institutional',
                             'Marine Vessels, Commercial', 'Railroad Equipment'
                            ]:

            if ":" in scc_level_four:
                unit_type = scc_level_four.split(':')[0]

            else:
                unit_type = scc_level_four

            fuel_type = scc_level_three

        elif scc_level_two in ['Engine Testing']:
            unit_type = scc_level_three
            fuel_type = scc_level_four

        elif scc_level_two in ['Off-highway 2-stroke Gasoline Engines',
                               'Off-highway 4-stroke Gasoline Engines',
                               'Off-highway Diesel Engines',
                               'Off-highway LPG-fueled Engines',
                               'Fixed Wing Aircraft L & TO Exhaust'
                               ]:
            try:
                unit_type, fuel_type = scc_level_three.split(': ')

            except ValueError:
                print(f'This does not work: {scc_level_three}')
                unit_type = np.nan
                fuel_type = np.nan

        else:
            unit_type = np.nan
            fuel_type = np.nan

        return unit_type, fuel_type

    def id_stationary_fuel_combustion(self, scc_level_two, scc_level_three,
                                      scc_level_four):
        """
        Use SCC levels to identify unit type and fuel type indicated by a SCC code.

        Parameters
        ----------
        scc_level_two : str
            s

        scc_level_three : str

        scc_level_four : str

        Returns
        -------
        unit_type : str
        fuel_type : str
        """

        if scc_level_two in ['Industrial', 'Commercial/Institutional',
                             'Total Area Source Fuel Combustion']:

            if ":" in scc_level_four:
                unit_type = scc_level_four.split(': ')[1]

            else:
                unit_type = scc_level_four

            fuel_type = scc_level_three

        elif scc_level_two in ['Residential']:
            fuel_type = scc_level_four

            if fuel_type == 'Wood':
                unit_type = scc_level_four.split(':')[0]

            else:
                unit_type = scc_level_four

        else:
            fuel_type = np.nan
            unit_type = np.nan

        return unit_type, fuel_type

    def id_chemical_evaporation(self, scc_level_two, scc_level_three, 
                                scc_level_four):
        """
        
        """

        if (scc_level_two == 'Surface Coating Operations') & \
            (('dryer' in scc_level_four.lower()) | ('drying' in scc_level_four.lower())):

            unit_type = scc_level_four
            fuel_type = np.nan

        elif scc_level_three == 'Coating Oven - General':

            if ('<' in scc_level_four) | ('>' in scc_level_four):
                unit_type = 'Coating Oven'

            else:
                unit_type = scc_level_four

            fuel_type = np.nan

        elif scc_level_three == 'Coating Oven Heater':
            unit_type = 'Coating Oven Heater'
            fuel_type = scc_level_four

        elif (scc_level_three == 'Fuel Fired Equipment') & \
                (scc_level_two=='Surface Coating Operations'):
            fuel_type, unit_type = scc_level_four.split(': ')

        elif (scc_level_three == 'Fuel Fired Equipment') & \
                (scc_level_two=='Organic Solvent Evaporation'):
            unit_type, fuel_type = scc_level_four.split(': ')

        elif scc_level_three == 'Drying':
            unit_type = scc_level_four
            fuel_type = np.nan

        elif (scc_level_three != 'Drying') & \
                (scc_level_four in ['Dryer', 'Drying', 'Drying/Curing']):
            unit_type = scc_level_four
            fuel_type = np.nan

        # Skipping dry cleaning operations. Unsure whether "drying"
        # includes application of heat

        else:
            unit_type = np.nan
            fuel_type = np.nan

        return unit_type, fuel_type

    def id_industrial_processes(self, scc_level_two, scc_level_three,
                                scc_level_four):
        """
        
        """

        if 'Commercial Cooking' in scc_level_three:
            unit_type = scc_level_four
            fuel_type = np.nan

        elif (scc_level_two == 'In-process Fuel Use') & \
             (scc_level_four != 'Total') & \
             ('Fuel Storage' not in scc_level_three):

            unit_type = scc_level_four
            fuel_type = scc_level_three

        elif scc_level_three == 'Ammonia Production':

            if ':' in scc_level_four:
                unit_type, fuel_type = scc_level_four.split(': ')

            else:
                unit_type = scc_level_four
                fuel_type = np.nan

        elif (scc_level_two != 'In-process Fuel Use') & \
            (any([x in scc_level_four.lower() for x in [
            'calciner', 'evaporator', 'furnace', 'dryer', 'kiln', 'oven',
            'incinerator', 'distillation'
            ]])):

            unit_type = scc_level_four

            if 'fired' in unit_type.lower():

                try:
                    fuel_type = re.search(
                        r'(\w+ \w+)(?=-fired)|(\w+)(?=-fired)|(\w+ \w+ \w+)(?=-fired)', unit_type
                        ).group()

                except AttributeError:
                    fuel_type = np.nan

            else:
                fuel_type = np.nan

        else:
            fuel_type = np.nan
            unit_type = np.nan

        return unit_type, fuel_type

    def apply_id_method(self, method):
        """

        Parameters
        ----------
        method : str
            Indentification method based on SCC level one fields.
            Current methods and their SCC Level One values are
                ext_comb : External Combustion
                int_comb : Internal Combustion Engines
                sta_comb : Stationary Source Fuel Combustion
                che_evap : Chemical Evaporation
                ind_proc : Industrial Processes

        scc_df : pandas.DataFrame
            Dataframe of EPA SCC codes

        Returns
        -------
        id_df : pandas.DataFrame
            DataFrame of relevant fields, including
            unit_type : type of combustion unit or process unit
            fuel_type : type of fuel combusted

        """

        id_methods = {
            'ext_comb': {
                'level_one': 'External Combustion',
                'func': self.id_external_combustion,
                'columns': ['scc_level_two', 'scc_level_three',
                            'scc_level_four']
                },
            'sta_comb': {
                'level_one': 'Stationary Source Fuel Combustion',
                'func': self.id_stationary_fuel_combustion,
                'columns': ['scc_level_two', 'scc_level_three',
                            'scc_level_four']
                },
            'int_comb': {
                'level_one': 'Internal Combustion Engines',
                'func': self.id_ice,
                'columns': ['scc_level_two', 'scc_level_three',
                            'scc_level_four']
                },
            'che_evap': {
                'level_one': 'Chemical Evaporation',
                'func': self.id_chemical_evaporation,
                'columns': ['scc_level_two', 'scc_level_three',
                            'scc_level_four']
                },
            'ind_proc': {
                'level_one': 'Industrial Processes',
                'func': self.id_industrial_processes,
                'columns': ['scc_level_two', 'scc_level_three',
                            'scc_level_four']
                }
            }

        scc_id = self._scc_data[
            self._scc_data.scc_level_one == f'{id_methods[method]["level_one"]}'
            ]

        scc_id = scc_id.apply(
            lambda x: id_methods[method]['func'](
                x[id_methods[method]['columns'][0]],
                x[id_methods[method]['columns'][1]],
                x[id_methods[method]['columns'][2]],
                ), axis=1, result_type='expand'
            )

        if len(scc_id.columns) == 2:
            scc_id.columns = ['unit_type', 'fuel_type']

        elif len(scc_id.columns) == 3:
            scc_id.columns = ['unit_type', 'fuel_type', 'unit_cap']

        return scc_id

    def combine_id(self, scc_dfs):
        """"
        Combines unit and fuel types identified from SCC codes with
        DataFrame of original SCC information.

        Parameters
        ----------
        scc_dfs : list of pandas.DataFrames

        Returns
        -------
        idd_scc : pandas.DataFrame

        """

        idd_scc = pd.concat(
            [x for x in scc_dfs], axis=0, ignore_index=False
            )

        idd_scc = pd.concat(
            [self._scc_data, idd_scc], axis=1
            )

        return idd_scc


if __name__ == '__main__':
    methods = [
        'ext_comb', 'int_comb', 'sta_comb',
        'che_evap', 'ind_proc'
        ]
    id_scc = SCC_ID()
    scc_dfs = [id_scc.apply_id_method(m) for m in methods]
    id_scc_df = id_scc.combine_id(scc_dfs)
    id_scc_df.to_csv('idd_scc.csv')