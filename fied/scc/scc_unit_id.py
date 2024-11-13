
import os

import pandas as pd
import numpy as np
import re
import logging
import requests
import yaml
from pathlib import Path
from io import BytesIO

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

        self._FIEDPATH = Path(__file__).parents[1]

        self._complete_scc_filepath = Path(self._FIEDPATH, "data/SCC/SCCDownload.csv")

        self._all_fuel_types_path = Path(self._FIEDPATH, "tools/all_fuels.csv")

        # self._all_fuel_types = pd.read_csv(self._all_fuel_types_path)
        self._all_fuel_types = pd.read_csv(self._all_fuel_types_path, index_col=['ft'])
        self._all_fuel_types = self._all_fuel_types[~self._all_fuel_types.index.duplicated()]  # Catch duplicates
        self._all_fuel_types = self._all_fuel_types.to_dict(orient='index')


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
        types in SCCs. Expanded to include multi-level unit types
        and fuel types. The following tables list the first level unit and
        fuel types. The fuel types are based in part on `EPA GHGRP Table C-1 to Subpart C <https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-98/subpart-C/appendix-Table%20C-1%20to%20Subpart%20C%20of%20Part%2098>`_
        Unit types are . Note that not all unit types of interest are combustion units. 

        .. csv-table:: Level 1 Unit Types
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
        
        .. csv-table:: Level 1 Fuel Types
            :header: "Fuel Type"
    
            "Coal and coke"
            "Natural gas"
            "Petroleum products"
            "Biomass"
            "Other"

        Returns
        -------
        all_scc : pandas.DataFrame
            Complete SCC codes with added columns
            of unit types and fuel types.
        """

        all_scc = self.load_complete_scc()
        all_scc.loc[:, 'unit_type_lvl1'] = np.nan
        all_scc.loc[:, 'unit_type_lvl2'] = np.nan
        all_scc.loc[:, 'fuel_type_lvl1'] = np.nan
        all_scc.loc[:, 'fuel_type_lvl2'] = np.nan

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

        all_scc.dropna(subset=['unit_type_lvl1', 'unit_type_lvl2', 'fuel_type_lvl1',
                               'fuel_type_lvl2'],
                       how='all', inplace=True)

        # all_scc = self.ft_clean_up(all_scc)

        return all_scc
    
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
            ft1 = self._all_fuel_types[ft]['lvl_1']
            ft2 = self._all_fuel_types[ft]['lvl_2']

        except KeyError as e:
            logging.error(f"{e}")

        else:
            return ft1, ft2

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

        all_types = {
            'unit_types_lvl1': [],
            'unit_types_lvl2': [],
            'fuel_types_lvl1': [],
            'fuel_types_lvl2': []
            }

        for i, r in scc_exc.iterrows():

            if r['scc_level_two'] == 'Space Heaters':

                ut1 = 'Heater'
                ut2 = 'Space heater'

                if ':' in r['scc_level_four']:
                    ft1, ft2 = self.match_fuel_type(r['scc_level_four'].split(': ')[0])

                else:
                    ft1, ft2 = self.match_fuel_type(r['scc_level_four'])

            elif 'Boilers' in r['scc_level_two']:

                ut1 = "Boiler"

                ut_match = re.search(r'(?<=Boiler,\s)[\w\s\W]+|(?<=Coal:\s)[\w\s\W]+', r['scc_level_four'])

                if ut_match:

                    if ((':' in ut_match.group()) & ('Boiler, ' in ut_match.group())) | ('Boiler, ' in ut_match.group()):
    
                        ut2 = ut_match.group().split('Boiler, ')[1]

                    elif 'Pulverizd Coal:' in ut_match.group():

                        ut2 = ut_match().split(': ')[1]

                    else:

                        ut2 = ut_match.group()

                    ft1, ft2 = self.match_fuel_type(r['scc_level_three'])

                else:

                    if r['scc_level_four'] in (self._all_fuel_types.keys()):

                        ft1, ft2 = self.match_fuel_type(r['scc_level_four'])
                        ut2 = 'Boiler'

                    elif r['scc_level_four'] == 'All':

                        ft1, ft2 = self.match_fuel_type(r['scc_level_three'])
                        ut2 = 'Boiler'
                        
                    else:
                        ft1, ft2 = self.match_fuel_type(r['scc_level_three'])
                        ut2 = r['scc_level_four']

            all_types['unit_types_lvl1'].append(ut1)
            all_types['unit_types_lvl2'].append(ut2)
            all_types['fuel_types_lvl1'].append(ft1)
            all_types['fuel_types_lvl2'].append(ft2)

        scc_exc = scc_exc.join(
            pd.DataFrame(all_types, index=scc_exc.index)
            )

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
            ['Electric Generation', 'Industrial', 'Commercial/Institutional',
             'Engine Testing']
            )]

        all_types = {
            'unit_types_lvl1': [],
            'unit_types_lvl2': [],
            'fuel_types_lvl1': [],
            'fuel_types_lvl2': []
            }

        other = ['Geysers/Geothermal', 'Equipment Leaks',
                 'Wastewater, Aggregate',
                 'Wastewater, Points of Generation', 'Flares']

        types = [
            'Turbine',
            'Reciprocating',
            '2-cycle',
            '4-cycle'
            # 'Turbine: Cogeneration',
            # 'Reciprocating: Cogeneration',
            # 'Refinery Gas: Turbine',
            # 'Refinery Gas: Reciprocating Engine',
            # 'Propane: Reciprocating',
            # 'Butane: Reciprocating',
            # 'Reciprocating Engine',
            # 'Reciprocating Engine: Cogeneration'
            ]

        for i, r in scc_ice.iterrows():

            if r['scc_level_three'] in other:

                ut1, ut2 = None, None
                ft1, ft2 = None, None

            else:

                ut1 = 'Internal combustion engine'

                if any([t in r['scc_level_four'] for t in types]):

                    ft1, ft2 = self.match_fuel_type(r['scc_level_three'])

                    ut2 = r['scc_level_four']

                else: 
    
                    if r['scc_level_four'] in self._all_fuel_types.keys():

                        ft1, ft2 = self.match_fuel_type(r['scc_level_four'])

                    else:

                        ft1, ft2 = self.match_fuel_type('Jet A Fuel')

                    ut2 = r['scc_level_three'.split('Testing')][0]

            all_types['unit_types_lvl1'].append(ut1)
            all_types['unit_types_lvl2'].append(ut2)
            all_types['fuel_types_lvl1'].append(ft1)
            all_types['fuel_types_lvl2'].append(ft2)

        scc_ice = scc_ice.join(
            pd.DataFrame(all_types, index=scc_ice.index)
            )

        scc_ice.dropna(subset=[f"{t}_types_lvl{l}" for t in ['unit', 'fuel'] for l in [1, 2]],
                       inplace=True)

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

        all_types = {
            'unit_types_lvl1': [],
            'unit_types_lvl2': [],
            'fuel_types_lvl1': [],
            'fuel_types_lvl2': []
            }

        for i, r in scc_sta.iterrows():
            ft1, ft2 = self.match_fuel_type(r['scc_level_three'])

            if 'All Boiler Types' in r['scc_level_four']:
                ut1 = 'Boiler'
                ut2 = 'Boiler'
                # unit_types_detail.append('Boiler')

            elif 'Boilers and IC Engines' in r['scc_level_four']:
                ut1 = 'Other combustion'
                ut2 = 'Boilers and internal combustion engines'
                # unit_types_detail.append('Boilers and IC Engines')

            elif 'All IC Engine Types' in r['scc_level_four']:
                ut1 = 'Internal combustion engine'
                ut2 = 'Internal combustion engine'
                # unit_types_detail.append('IC Engine')

            elif 'All Heater Types' in r['scc_level_four']:
                ut1 = 'Heater'
                ut2 = 'Heater'
                # unit_types_detail.append('Heater')

            else:
                
                ut1 = 'Other combustion'
                ut2 = r['scc_level_four']

            all_types['unit_types_lvl1'].append(ut1)
            all_types['unit_types_lvl2'].append(ut2)
            all_types['fuel_types_lvl1'].append(ft1)
            all_types['fuel_types_lvl2'].append(ft2)


        scc_sta = scc_sta.join(
            pd.DataFrame(all_types, index=scc_sta.index)
            )

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

        scc_chee = all_scc.query(
            "scc_level_one == 'Chemical Evaporation' & (scc_level_two == 'Printing/Publishing' |\
            scc_level_two == 'Surface Coating Operations' | scc_level_two == 'Organic Solvent Evaporation')")

        all_types = {
            'unit_types_lvl1': [],
            'unit_types_lvl2': [],
            'fuel_types_lvl1': [],
            'fuel_types_lvl2': []
            }

        for i, r in scc_chee.iterrows():

            if (('dryer' in r['scc_level_four'].lower()) | \
                 ('drying' in r['scc_level_four'].lower())):

                ut1, ut2 = 'Dryer', r['scc_level_four']
                ft1, ft2 = None, None

            elif r['scc_level_three'] == 'Coating Oven - General':
            
                ut1 = 'Oven'

                if ('<' in r['scc_level_four']) | ('>' in r['scc_level_four']):
                    
                    ut2 = 'Coating Oven'

                else:
                    
                    ut2 = r['scc_level_four']

                ft1, ft2 = None, None

            elif r['scc_level_three'] == 'Coating Oven Heater':
                
                ut1, ut2 = 'Heater', 'Coating oven heater'
                ft1, ft2 = self.match_fuel_type(r['scc_level_four'])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                (r['scc_level_two']=='Surface Coating Operations'):

                ut1, ut2 = 'Other', r['scc_level_four'].split(': ')[1]
                ft1, ft2 = self.match_fuel_type(r['scc_level_four'].split(': ')[0])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                    (r['scc_level_two']=='Organic Solvent Evaporation'):

                ut1, ut2 = 'Other combustion', r['scc_level_four'].split(': ')[0]
                ft1, ft2 = self.match_fuel_type(r['scc_level_four'].split(': ')[1])
    
            else:
                ut1, ut2 = None, None
                ft1, ft2 = None, None

            all_types['unit_types_lvl1'].append(ut1)
            all_types['unit_types_lvl2'].append(ut2)
            all_types['fuel_types_lvl1'].append(ft1)
            all_types['fuel_types_lvl2'].append(ft2)

        scc_chee = scc_chee.join(
            pd.DataFrame(all_types, index=scc_chee.index)
            )
        
        scc_chee.dropna(subset=[f"unit_types_lvl{l}" for l in [1, 2]], inplace=True)

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

        scc_ind = all_scc.query("scc_level_one == 'Industrial Processes' & status == 'Active'")

        all_types = {
            'unit_types_lvl1': [],
            'unit_types_lvl2': [],
            'fuel_types_lvl1': [],
            'fuel_types_lvl2': []
            }
    
        other_counter = []

        for i, r in scc_ind.iterrows():

            ft1, ft2 = None, None
            ut1, ut2 = None, None

            if 'Commercial Cooking' in r['scc_level_three']:

                u = r['scc_level_four'].split(' - ')[1]

                if u == 'Total':
                    ut1, ut2 = 'Other combustion', 'Cooking'

                else:
                    ut1, ut2 = 'Other combustion', u

            elif (r['scc_level_two'] == 'In-process Fuel Use') & \
                (r['scc_level_four'] != 'Total') & \
                ('Fuel Storage' not in r['scc_level_three']):

                ft1, ft2 = self.match_fuel_type(r['scc_level_three'])

                if 'Kiln' in r['scc_level_four']:
                    ut1, ut2 = 'Kiln', r['scc_level_four']
        
                else:
                    ut1, ut2 = 'Other combustion', 'Other combustion'
    
                other_counter.append(i)

            elif r['scc_level_three'] == 'Ammonia Production':

                if ':' in r['scc_level_four']:
    
                    ut1, ut2 = 'Other combustion', r['scc_level_four'].split(': ')[0]

                    ft1, ft2 = self.match_fuel_type(r['scc_level_four'].split(': ')[1].split(' Fired')[0])

                else:
                    continue

            elif (r['scc_level_two'] != 'In-process Fuel Use') & \
                (any([x in r['scc_level_four'].lower() for x in [
                    'calciner', 'evaporator', 'furnace', 'dryer', 'kiln',
                    'oven', 'flares', 'incinerators', 'turbines', 'turbine',
                    'engine','incinerator', 'distillation', 'heater', 'broil', 'flare',
                    'stove', 'steam'
                    ]])):

                if r['scc_level_three'] == 'Fuel Fired Equipment':

                    x, y = r['scc_level_four'].split(': ')

                    if any([z in x for z in ['Distillate', 'Residual', 'Gas',
                                             'Liquid', 'Propane']]):
    
                        ft1, ft2 = self.match_fuel_type(x)
                        ut = y

                    else:
                        ft1, ft2 = self.match_fuel_type(y)
                        ut = x

                else:
                    ut = r['scc_level_four']

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
                                ft1, f2 = self.match_fuel_type(
                                    re.search(
                                        r'cbm|nat gas|natural gas|distillate oil|residual oil|#2 oil|#6 oil|propane|coal|process gas', 
                                        ut.lower()
                                        ).group()
                                    )

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
    all_scc = id_scc.load_complete_scc()
    exc = id_scc.id_external_combustion(all_scc)
    ice = id_scc.id_ice(all_scc)
    sta = id_scc.id_stationary_fuel_combustion(all_scc)
    scc_chee = id_scc.id_chemical_evaporation(all_scc)
    # id_scc_df = id_scc.build_id()
    # id_scc_df.to_csv('./scc/iden_scc.csv')
