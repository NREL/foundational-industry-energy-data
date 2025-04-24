
import os

import pandas as pd
import numpy as np
import re
import logging
import requests

from fied import datasets


class SCC_ID:
    """
    Use descriptions of SCC code levels to identify unit type and fuel type 
    indicated by a complete SCC code (e.g., 30190003). 
    The U.S. EPA uses Source Classification Codes (SCCs) to 
    classify different types of activities that generate emissions. 
    Each SCC represents a unique source category-specific process or
    function that emits air pollutants. The SCCs are used as a primary
    identifying data element in EPA’s WebFIRE (where SCCs are
    used to link emissions factors to an emission process),
    the National Emissions Inventory (NEI), and other EPA databases.

    Eight digit SCC codes, such as ABBCCCDD, are structured as follows:

    A: Level 1
    BB: Level 2
    CCC: Level 3
    DD: Level 4

    See SCC documentation for additional information:
    https://sor-scc-api.epa.gov/sccwebservices/sccsearch/docs/SCC-IntroToSCCs_2021.pdf

    """

    def __init__(self):

        logging.basicConfig(level=logging.INFO)


    def load_complete_scc(self):
        """
        Complete list of SCC codes (available from
        https://sor-scc-api.epa.gov/sccwebservices/sccsearch/) have
        been manually downloaded.

        Returns
        -------
        all_scc : pandas.DataFrame

        """

        all_scc = datasets.fetch_scc()

        all_scc.columns = [c.replace(' ', '_') for c in all_scc.columns]

        return all_scc

    @staticmethod
    def scc_query_split(scc):
        """
        Uses EPA SCC Web Services to get level information for an 8- or
        10-digit SCC.

        Parameters
        ----------
        scc : int
            Eight or 10-digit Source Classification Code.

        Returns
        -------
        scc_levels : dict
            Dictionary of all four level names
        """

        base_url = 'http://sor-scc-api.epa.gov:80/sccwebservices/v1/SCC/'

        try:
            r = requests.get(base_url+f'{scc}')
            r.raise_for_status()

        except requests.exceptions.HTTPError as err:
            logging.error(f'{err}')

        levels = [f'scc level {n}' for n in ['one', 'two', 'three', 'four']]

        scc_levels = {}

        try:
            for k in levels:
                scc_levels[k] = r.json()['attributes'][k]['text']

        except TypeError:
            logging.error(f'SCC {scc} cannot be found')
            scc_levels = None

        return scc_levels

    def build_id(self):
        """
        Identify all relevant unit types and fuel
        types in SCCs.

        Returns
        -------
        all_scc : pandas.DataFrame
            Complete SCC codes with added columns
            of unit types and fuel types.
        """

        all_scc = self.load_complete_scc()
        all_scc.loc[:, 'unit_type'] = np.nan
        all_scc.loc[:, 'fuel_type'] = np.nan

        id_meth = [
             self.id_external_combustion,
             self.id_stationary_fuel_combustion,
             self.id_ice,
             self.id_chemical_evaporation,
             self.id_industrial_processes,
            ]

        ids = pd.concat(
            [f(all_scc) for f in id_meth],
            axis=0, ignore_index=False,
            sort=True
            )

        all_scc.update(ids)

        all_scc.dropna(subset=['unit_type', 'fuel_type'],
                       how='all', inplace=True)

        # all_scc = self.ft_clean_up(all_scc)

        return all_scc

    def id_external_combustion(self, all_scc):
        """
        Method for identifying relevant unit and fuel types under
        SCC Level 1 External Combustion (1)

        Parameters
        ----------
        all_scc : pandas.DataFrame
            Complete list of SCCs.

        Returns
        -------
        scc_exc : pandas.DataFrame
            SCC for external combustion (SCC Level 1 == 1) with
            unit type and fuel type descriptions.
        """

        scc_exc = all_scc.query("scc_level_one=='External Combustion'")

        unit_types_detail = []
        fuel_types = []

        other_fuels = [
            'Shredded', 'Specify Percent Butane in Comments',
            'Specify in Comments', 'Specify Waste Material in Comments',
            'Refuse Derived Fuel', 'Sewage Grease Skimmings', 'Waste Oil',
            'Sludge Waste', 'Digester Gas',
            'Agricultural Byproducts (rice or peanut hulls, shells, cow manure, etc)',
            'Paper Pellets', 'Black Liquor', 'Red Liquor',
            'Spent Sulfite Liquor', 'Tall Oil',
            'Wood/Wood Waste Liquid', 'Off-gas Ejectors', 'Pulverized Coal',
            'Salable Animal Fat', 'Natural Gas', 'Process Gas',
            'Wet Slurry', 'Distillate Oil', 'Residual Oil', 'Petroleum Refinery Gas',
            'Grade 4 Oil', 'Grade 5 Oil', 'Grade 6 Oil', 'Blast Furnace Gas',
            'Coke Oven Gas', 'Landfill Gas', 'Biomass i[2]Solids'
            ]

        for i, r in scc_exc.iterrows():

            if r['scc_level_two'] == 'Space Heaters':

                if ':' in r['scc_level_three']:
                    unit_types_detail.append(r['scc_level_four'].split(': ')[-1])
                    fuel_types.append(r['scc_level_four'].split(': ')[0])

                else:
                    unit_types_detail.append('Space heater')
                    fuel_types.append(r['scc_level_four'].split(': ')[0])

            elif 'boiler' in r['scc_level_two'].lower():

                fuel_types.append(r['scc_level_three'])

                if 'boiler' in r['scc_level_four'].lower():
                    ut = r['scc_level_four']

                else:
                    ut = f'Boiler, {r.scc_level_four}'

                unit_types_detail.append(ut)

                # split_ut = r['scc_level_four'].split(': ')

                # if len(split_ut) == 2:
                #     ut = split_ut[-1]

                #     if ut in other_fuels:
                #         unit_types_detail.append('Boiler')

                #     else:
                #         unit_types_detail.append(ut)

                # elif len(split_ut) > 2:
                #     unit_types_detail.append((' '.join(split_ut[1:])))

                # else:
                #     if 'boiler' in r['scc_level_four'].lower():
                #         unit_types_detail.append(r['scc_level_four'])

                #     else:
                #         unit_types_detail.append('Boiler')

        scc_exc.loc[:, 'unit_type'] = unit_types_detail
        scc_exc.loc[:, 'fuel_type'] = fuel_types

        return scc_exc

    def id_ice(self, all_scc):
        """
        Method for identifying relevant unit and fuel types under 
        SCC Level 1 Internal Combustion Engines (2)

        Parameters
        ----------
        all_scc : pandas.DataFrame
            Complete list of SCCs.

        Returns
        -------
        scc_ice : pandas.DataFrame
            SCC for external combustion (SCC Level 1 == 2) with
            unit type and fuel type descriptions.
        """

        scc_ice = all_scc.query("scc_level_one=='Internal Combustion Engines'")
        scc_ice = scc_ice[scc_ice.scc_level_two.isin(
            ['Electric Generation', 'Industrial', 'Commercial/Institutional']
            )]

        unit_types_detail = []
        fuel_types = []

        other = ['Geysers/Geothermal', 'Equipment Leaks',
                 'Wastewater, Aggregate',
                 'Wastewater, Points of Generation', 'Flares']

        types = [
            'Turbine',
            'Reciprocating',
            'Turbine: Cogeneration',
            'Reciprocating: Cogeneration',
            'Refinery Gas: Turbine',
            'Refinery Gas: Reciprocating Engine',
            'Propane: Reciprocating',
            'Butane: Reciprocating',
            'Reciprocating Engine',
            'Reciprocating Engine: Cogeneration'
            ]

        for i, r in scc_ice.iterrows():

            if r['scc_level_three'] in other:
                ut = None
                ft = None

            elif r['scc_level_four'] in types:
                ft = r['scc_level_three']
                ut = r['scc_level_four']

            unit_types_detail.append(ut)
            fuel_types.append(ft)

        scc_ice.loc[:, 'unit_type'] = unit_types_detail
        scc_ice.loc[:, 'fuel_type'] = fuel_types

        return scc_ice

    def id_stationary_fuel_combustion(self, all_scc):
        """
        Method for identifying relevant unit and fuel types under 
        SCC Level 1 Stationary Source Fuel Combustion (21; note this is
        a 10-digit SCC code)

        Parameters
        ----------
        all_scc : pandas.DataFrame
            Complete list of SCCs.

        Returns
        -------
        scc_ice : pandas.DataFrame
            SCC for external combustion (SCC Level 1 == 2) with
            unit type and fuel type descriptions
        """

        scc_sta = all_scc.query("scc_level_one == 'Stationary Source Fuel Combustion'")
        scc_sta = scc_sta[~scc_sta.scc_level_two.isin(
            ['Residential']
            )]

        unit_types_detail = []
        fuel_types = []

        for i, r in scc_sta.iterrows():
            fuel_types.append(r['scc_level_three'])

            if 'All Boiler Types' in r['scc_level_four']:
                unit_types_detail.append('Boiler')

            elif 'Boilers and IC Engines' in r['scc_level_four']:
                unit_types_detail.append('Boilers and IC Engines')

            elif 'All IC Engine Types' in r['scc_level_four']:
                unit_types_detail.append('IC Engine')

            elif 'All Heater Types' in r['scc_level_four']:
                unit_types_detail.append('Heater')

            else:
                unit_types_detail.append(r['scc_level_four'])

        scc_sta.loc[:, 'unit_type'] = unit_types_detail
        scc_sta.loc[:, 'fuel_type'] = fuel_types

        return scc_sta

    def id_chemical_evaporation(self, all_scc):
        """
        Method for identifying relevant unit and fuel types under 
        SCC Level 1 Chemical Evaporation (4)

        Parameters
        ----------
        all_scc : pandas.DataFrame
            Complete list of SCCs.

        Returns
        -------
        scc_chee : pandas.DataFrame
            SCC for Chemical Evaporation (SCC Level 1 == 4) with
            unit type and fuel type descriptions
        """

        scc_chee = all_scc.query("scc_level_one == 'Chemical Evaporation'")

        fuel_types = []
        unit_types_detail = []

        for i, r in scc_chee.iterrows():

            if (r['scc_level_two'] == 'Surface Coating Operations') & \
                (('dryer' in r['scc_level_four'].lower()) | \
                 ('drying' in r['scc_level_four'].lower())):

                unit_types_detail.append(r['scc_level_four'])
                fuel_types.append(None)

            elif r['scc_level_three'] == 'Coating Oven - General':

                if ('<' in r['scc_level_four']) | ('>' in r['scc_level_four']):
                    unit_types_detail.append('Coating Oven')

                else:
                    unit_types_detail.append(r['scc_level_four'])

                fuel_types.append(None)

            elif r['scc_level_three'] == 'Coating Oven Heater':

                unit_types_detail.append('Coating Oven Heater')
                fuel_types.append(r['scc_level_four'])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                    (r['scc_level_two'] == 'Surface Coating Operations'):

                unit_types_detail.append(r['scc_level_four'].split(': ')[1])
                fuel_types.append(r['scc_level_four'].split(': ')[0])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                    (r['scc_level_two']=='Organic Solvent Evaporation'):

                unit_types_detail.append(r['scc_level_four'].split(': ')[0])
                fuel_types.append(r['scc_level_four'].split(': ')[1])

            elif r['scc_level_three'] == 'Drying':

                unit_types_detail.append(r['scc_level_four'])
                fuel_types.append(None)

            elif (r['scc_level_three'] != 'Drying') & \
                    (r['scc_level_four'] in ['Dryer', 'Drying', 'Drying/Curing']):

                unit_types_detail.append(r['scc_level_four'])
                fuel_types.append(None)

            # Skipping dry cleaning operations. Unsure whether "drying"
            # includes application of heat

            else:
                unit_types_detail.append(None)
                fuel_types.append(None)

        scc_chee.loc[:, 'unit_type'] = unit_types_detail
        scc_chee.loc[:, 'fuel_type'] = fuel_types

        return scc_chee

    def id_industrial_processes(self, all_scc):
        """
        Method for identifying relevant unit and fuel types under 
        SCC Level 1 Industrial Processes (3)

        Parameters
        ----------
        all_scc : pandas.DataFrame
            Complete list of SCCs.

        Returns
        -------
        scc_ind : pandas.DataFrame
            SCC for Industrial Processes (SCC Level 1 == 3) with
            unit type and fuel type descriptions
        """

        scc_ind = all_scc.query("scc_level_one == 'Industrial Processes'")

        unit_types_detail = []
        fuel_types = []

        other_counter = []

        for i, r in scc_ind.iterrows():

            ft = None
            ut = None

            if 'Commercial Cooking' in r['scc_level_three']:
                ut = r['scc_level_four']
                ft = None

            elif (r['scc_level_two'] == 'In-process Fuel Use') & \
                (r['scc_level_four'] != 'Total') & \
                ('Fuel Storage' not in r['scc_level_three']):

                ut = r['scc_level_four']
                ft = r['scc_level_three']
                other_counter.append(i)

            elif r['scc_level_three'] == 'Ammonia Production':

                if ':' in r['scc_level_four']:
                    ut = r['scc_level_four'].split(': ')[0]
                    ft = r['scc_level_four'].split(': ')[1]

                    if ft == 'Natural Gas Fired':
                        ft = 'natural gas'

                    else:
                        pass

                else:
                    ut = r['scc_level_four']
                    ft = None

            elif (r['scc_level_two'] != 'In-process Fuel Use') & \
                (any([x in r['scc_level_four'].lower() for x in [
                    'calciner', 'evaporator', 'furnace', 'dryer', 'kiln',
                    'oven', 'flares', 'incinerators', 'turbines', 'turbine',
                    'engines', 'engine',
                    'incinerator', 'distillation', 'heater', 'broil', 'flare',
                    'stove', 'steam'
                    ]])):

                if r['scc_level_three'] == 'Fuel Fired Equipment':
                    x, y = r['scc_level_four'].split(': ')

                    if any([z in x for z in ['Distillate', 'Residual', 'Gas',
                                             'Liquid', 'Propane']]):
                        ft = x
                        ut = y

                    else:
                        ft = y
                        ut = x

                else:
                    ut = r['scc_level_four']

                    if 'fired' in ut.lower():
                        try:
                            ft = re.search(
                                r'(\w+ \w+)(?=-fired)|(\w+)(?=-fired)|(\w+ \w+ \w+)(?=-fired)', 
                                ut
                                ).group()

                        except AttributeError:

                            try:
                                ft = re.search(
                                    r'cbm|nat gas|natural gas|distillate oil|residual oil|#2 oil|#6 oil|propane|coal|process gas', 
                                    ut.lower()
                                    ).group()

                            except AttributeError:
                                ft = None

                            else:
                                if ft.lower() == 'nat gas':
                                    ft = 'Natural Gas'

                                elif ft.lower() == 'cbm':
                                    ft = 'Natural Gas'

                                elif ft.lower() == "#2 oil":
                                    ft = 'Diesel'

                                elif ft.lower() == "#6 oil":
                                    ft = 'Residual Fuel Oil'

                                elif ft == 'natural gas':
                                    ft = 'Natural Gas'

                        else:
                            if 'fired' in ft.lower():
                                ft = ft.lower().split(' fired')[0]

                            elif 'direct' in ft.lower():
                                try:
                                    ft = ft.lower().split('direct ')[1]

                                except IndexError:
                                    ft = None
                                else:
                                    if ft == 'ng':
                                        ft = 'Natural Gas'

                                    else:
                                        pass

                            elif ('and' in ft.lower()) | ('or' in ft.lower()):
                                ft = ft.split(' ')[1]

                            # Assume that "...Gas-Fired..." equipment refers to natural gas.
                            elif ft.lower() == 'gas':
                                ft = 'Natural Gas'

                            elif ft.lower() == 'oil':
                                ft = 'Residual Fuel Oil'

                    else:
                        if 'diesel' in ut.lower():
                            ft = 'Diesel'

                        elif ':' in ut: 

                            try:
                                x, y = ut.split(': ')

                            except ValueError:
                                ft = None

                            else:

                                if any([z in x for z in ['Distillate', 'Residual', 'Gas',
                                                        'Liquid', 'Propane']]):
                                    ft = x
                                    ut = y

                                elif any([z in y for z in ['Distillate', 'Residual', 'Gas',
                                                        'Liquid', 'Propane']]):
                                    ft = y
                                    ut = x

                                else:
                                    ft = None

                        else:
                            ft = None

            elif r['scc_level_three'] == 'Fuel Fired Equipment':

                if r['sector'] == 'Industrial Processes - Chemical Manuf':

                    try:
                        ft = r['scc_level_four'].split(': ')[1]
                        ut = r['scc_level_four'].split(': ')[0]

                    except IndexError:
                        ft = None 
                        ut = r['scc_level_four']

                else:

                    ft = r['scc_level_four'].split(': ')[0]
                    ut = r['scc_level_four'].split(': ')[1]
                    logging.info(f'Last fuel type is {fuel_types[-1]}\nLast unit type is {unit_types_detail[-1]} ')

            # elif r['tier_1_description'] == 'Fuel Comb. Industrial':
            #     ft = f'{r["tier_3_description"]} {"tier_2_description"}'
            
            # Cludge for catching technologies that use electricity
            try:
                fte = re.search(r'(elec)', ut.lower())

            except AttributeError:
                pass

            else:
                if fte:
                    ft = 'electricity'

                else:
                    pass

            fuel_types.append(ft)
            unit_types_detail.append(ut)

        scc_ind.loc[:, 'unit_type'] = unit_types_detail
        scc_ind.loc[:, 'fuel_type'] = fuel_types

        return scc_ind


    def main(self):
        id_scc = SCC_ID()
        id_scc_df = id_scc.build_id()
        os.makedirs('./scc', exist_ok=True)
        id_scc_df.to_csv('./scc/iden_scc.csv')


if __name__ == '__main__':

    id_scc = SCC_ID()
    id_scc_df = id_scc.build_id()
    id_scc_df.to_csv('./scc/iden_scc.csv')
