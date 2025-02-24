
import pandas as pd
import numpy as np
import re
import logging
import requests
import yaml
import sys
from pathlib import Path
from io import BytesIO
toolspath = str(Path(__file__).parents[1]/"tools")
sys.path.append(toolspath)
from unit_matcher import UnitsFuels

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

        self._unit_methods = UnitsFuels()

        self._FIEDPATH = Path(__file__).parents[1]

        self._complete_scc_filepath = Path(self._FIEDPATH, "data/SCC/SCCDownload.csv")

        self._all_fuel_types_path = Path(self._FIEDPATH, "tools/all_fuels.csv")

        # YAML that contains fuel types
        self._all_fuel_types_path = Path(self._FIEDPATH, "tools/fuel_type_standardization.yaml")


        # self._all_fuel_types = pd.read_csv(self._all_fuel_types_path)
        # self._all_fuel_types = pd.read_csv(self._all_fuel_types_path, index_col=['ft'])
        # self._all_fuel_types = self._all_fuel_types[~self._all_fuel_types.index.duplicated()]  # Catch duplicates
        # self._all_fuel_types = self._all_fuel_types.to_dict(orient='index')

        with open(self._all_fuel_types_path, 'r') as file:
            self._all_fuel_types = yaml.safe_load(file)

    def get_complete_scc(self):
        """
        Download full set of SCC codes from EPA and save to
        `self._complete_scc_filepath`.
        Note that downloading directly from website assignes filename for csv
        based as 'SCCDownload-{y}-{md}-{t}.csv'

        """

        payload = {
            'format': 'CSV',
            'sortFacet': 'scc level one',
            'filename': 'SCCDownload.csv'
            }

        url = 'https://sor-scc-api.epa.gov/sccwebservices/v1/SCC?'

        try:
            r = requests.get(url, params=payload)

        except requests.exceptions.RequestException as e:
            logging.ERROR(e.response.text)

        else:
            all_scc = pd.read_csv(
                BytesIO(r.content)
                )

            all_scc.to_csv(self._complete_scc_filepath)

        return all_scc

    def load_complete_scc(self):
        """
        Complete list of SCC codes (available from
        https://sor-scc-api.epa.gov/sccwebservices/sccsearch/) have
        been manually downloaded.

        Returns
        -------
        all_scc : pandas.DataFrame

        """

        try:

            all_scc = pd.read_csv(self._complete_scc_filepath)

        except FileNotFoundError:

            all_scc = self.get_complete_scc()

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
        
        # TODO fix where rows with NaN SCC and lists for fuel types and unit types are being generated.
        # This is a stopgap fix
        ids.dropna(subset=['SCC'], inplace=True)

        all_scc = all_scc.join(
            ids[['unit_type_lv1', 'unit_type_lv2', 'fuel_type_lv1','fuel_type_lv2']]
            )

        # all_scc.dropna(subset=['unit_type_lv1', 'unit_type_lv2', 'fuel_type_lv1',
        #                        'fuel_type_lv2'],
        #                how='all', inplace=True)

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

        all_types = {
            'unit_type_lv1': [],
            'unit_type_lv2': [],
            'fuel_type_lv1': [],
            'fuel_type_lv2': []
            }

        for i, r in scc_exc.iterrows():

            if r['scc_level_two'] == 'Space Heaters':
                ut1, ut2 = self._unit_methods.char_nei_units(r['scc_level_two'])

                if ':' in r['scc_level_four']:
                    ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'].split(': ')[0])

                else:
                    ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'])

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

                    ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_three'])

                else:

                    if r['scc_level_four'] in (self._all_fuel_types.keys()):

                        ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'])
                        ut2 = 'Boiler'

                    elif r['scc_level_four'] == 'All':

                        ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_three'])
                        ut2 = 'Boiler'
                        
                    else:
                        ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_three'])

                        ut2 = r['scc_level_four']

            all_types['unit_type_lv1'].append(ut1)
            all_types['unit_type_lv2'].append(ut2)
            all_types['fuel_type_lv1'].append(ft1)
            all_types['fuel_type_lv2'].append(ft2)

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
            'unit_type_lv1': [],
            'unit_type_lv2': [],
            'fuel_type_lv1': [],
            'fuel_type_lv2': []
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

                    ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_three'])

                    ut2 = r['scc_level_four']

                else: 
    
                    if r['scc_level_four'] in self._all_fuel_types.keys():

                        ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'])

                    else:

                        ft1, ft2 = self._unit_methods.match_fuel_type('Jet A Fuel')

                    ut2 = r['scc_level_three'.split('Testing')][0]

            all_types['unit_type_lv1'].append(ut1)
            all_types['unit_type_lv2'].append(ut2)
            all_types['fuel_type_lv1'].append(ft1)
            all_types['fuel_type_lv2'].append(ft2)

        scc_ice = scc_ice.join(
            pd.DataFrame(all_types, index=scc_ice.index)
            )

        scc_ice.dropna(subset=[f"{t}_type_lv{l}" for t in ['unit', 'fuel'] for l in [1, 2]],
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
            'unit_type_lv1': [],
            'unit_type_lv2': [],
            'fuel_type_lv1': [],
            'fuel_type_lv2': []
            }

        for i, r in scc_sta.iterrows():

            ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_three'])

            if 'All Boiler Types' in r['scc_level_four']:
                ut1, ut2 = 'Boiler', 'Boiler'
                # unit_types_detail.append('Boiler')

            elif 'Boilers and IC Engines' in r['scc_level_four']:
                ut1, ut2 = 'Other combustion', 'Boilers and internal combustion engines'
                # unit_types_detail.append('Boilers and IC Engines')

            elif 'All IC Engine Types' in r['scc_level_four']:
                ut1, ut2 = 'Internal combustion engine', 'Internal combustion engine'
                # unit_types_detail.append('IC Engine')

            elif 'All Heater Types' in r['scc_level_four']:
                ut1, ut2 = 'Heater', 'Heater'
                # unit_types_detail.append('Heater')

            else:
                
                ut1, ut2 = 'Other combustion', r['scc_level_four']

            all_types['unit_type_lv1'].append(ut1)
            all_types['unit_type_lv2'].append(ut2)
            all_types['fuel_type_lv1'].append(ft1)
            all_types['fuel_type_lv2'].append(ft2)

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
            'unit_type_lv1': [],
            'unit_type_lv2': [],
            'fuel_type_lv1': [],
            'fuel_type_lv2': []
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

                ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                (r['scc_level_two']=='Surface Coating Operations'):

                ut1, ut2 = 'Other', r['scc_level_four'].split(': ')[1]
                ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'].split(': ')[0])

            elif (r['scc_level_three'] == 'Fuel Fired Equipment') & \
                    (r['scc_level_two']=='Organic Solvent Evaporation'):

                ut1, ut2 = 'Other combustion', r['scc_level_four'].split(': ')[0]
                ft1, ft2 = self._unit_methods.match_fuel_type(r['scc_level_four'].split(': ')[1])
    
            else:
                ut1, ut2 = None, None
                ft1, ft2 = None, None

            all_types['unit_type_lv1'].append(ut1)
            all_types['unit_type_lv2'].append(ut2)
            all_types['fuel_type_lv1'].append(ft1)
            all_types['fuel_type_lv2'].append(ft2)

        scc_chee = scc_chee.join(
            pd.DataFrame(all_types, index=scc_chee.index)
            )
        
        scc_chee.dropna(subset=[f"unit_type_lv{l}" for l in [1, 2]], inplace=True)

        return scc_chee

    # TODO: there are still opportunities to refactor this method. 
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

        all_types_cols = [f"{t}_type_lv{l}" for t in ['unit', 'fuel'] for l in [1, 2]]
        
        types_df = pd.DataFrame(columns=all_types_cols)
    
        type_queries = {
            1: "scc_level_two == 'In-process Fuel Use' & sector != 'Industrial Processes - Storage and Transfer'",
            2: "sector == 'Commercial Cooking'",
            3: "scc_level_three == 'Ammonia Production'",
            4: "scc_level_two != 'In-process Fuel Use' & tier_1_description == 'Storage & Transport'",
            5: "sector == 'Industrial Processes - Petroleum Refineries'",
            6: "scc_level_three == 'Fuel Fired Equipment'",
            7: "scc_level_three != 'Fuel Fired Equipment' & tier_1_description != 'Storage & Transport' & (sector == 'Industrial Processes - Pulp & Paper' | sector == 'Industrial Processes - Cement Manuf' | sector ==  'Industrial Processes - Mining')",
            8: "sector == 'Industrial Processes - Oil & Gas Production' & tier_1_description != 'Storage & Transport'",
            9: "sector == 'Industrial Processes - Ferrous Metals'",
            10: "sector == 'Industrial Processes - NEC' & scc_level_three != 'Fuel Fired Equipment' & scc_level_two != 'In-process Fuel Use' & tier_1_description != 'Storage & Transport'",
            11: "sector == 'Industrial Processes - Chemical Manuf' & scc_level_three != 'Ammonia Production'  & tier_1_description != 'Storage & Transport' & scc_level_three != 'Fuel Fired Equipment'"
            }


        for n, q in type_queries.items():

            data = scc_ind.query(q).copy(deep=True)

            unit_type_lv1 = []
            unit_type_lv2 = []
            fuel_type_lv1 = []
            fuel_type_lv2 = []

            data_index = []

            if n == 1:

                for r in data.itertuples():

                    data_index.append(r[0])

                    ft1, ft2 = self._unit_methods.match_fuel_type(
                        r[data.columns.to_list().index('scc_level_three') + 1]
                        )

                    if 'Kiln' in r[data.columns.to_list().index('scc_level_four') + 1]: # ]r['scc_level_four']:
                        ut1, ut2 = 'Kiln', r[data.columns.to_list().index('scc_level_four') + 1]
        
                    else:
                        ut1, ut2 = 'Other combustion', 'Other combustion'
        
                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)
            
            if n == 2:

                for r in data.itertuples():

                    data_index.append(r[0])

                    if 'Commercial Cooking' in r[data.columns.to_list().index('scc_level_three') + 1]:

                        u = r[data.columns.to_list().index('scc_level_three') + 1].split(' - ')[1]

                        if u == 'Total':
                            ut1, ut2 = 'Other combustion', 'Cooking'

                        else:
                            ut1, ut2 = 'Other combustion', u

                    ft1, ft2, = None, None

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)

            if n == 3:

                for r in data.itertuples():

                    data_index.append(r[0])

                    if ':' in r[data.columns.to_list().index('scc_level_four')]:
    
                        ut1, ut2 = 'Other combustion', r[data.columns.to_list().index('scc_level_four') + 1].split(': ')[0]

                        ft1, ft2 = self._unit_methods.match_fuel_type(
                            r[data.columns.to_list().index('scc_level_four') + 1].split(': ')[1].split(' Fired')[0])

                    else:
                        ut1, ut2 = "Other", r[data.columns.to_list().index('scc_level_four') + 1]
                        ft1, ft2 = None, None

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)

            if n == 4:

                for r in data.itertuples():

                    data_index.append(r[0])

                    if re.search(r'(Breathing Loss)', r[data.columns.to_list().index('scc_level_four') + 1]):

                        ut1, ut2 = None, None
    
                    else:
                        ut1, ut2 = 'Other', r[data.columns.to_list().index('scc_level_four') + 1]
        
                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(np.nan)
                    fuel_type_lv2.append(np.nan)

            if n == 5:
    
                refinery_types = {
                    'Process Heaters': ['Heater', 'Process heater'], 
                    'Flares': ['Other combustion', 'Flare'],
                    'Fluid Coking Unit': ['Other combustion', 'Fluid coking unit'], 
                    'Petroleum Coke Calcining': ['Other combustion', 'Petroleum coke calcining'],
                    'Incinerators': ['Other combustion', 'Incinerator']
                    }

                for r in data.itertuples():

                    data_index.append(r[0])

                    if r[data.columns.to_list().index('scc_level_three')] in refinery_types.keys():

                        ut1, ut2 = refinery_types[r[data.columns.to_list().index('scc_level_three') + 1]]

                        ft = r[data.columns.to_list().index('scc_level_four') + 1]

                        if ':' in ft:
                            ft = ft.split(': ')[1]
            
                        ft1, ft2 = self._unit_methods.match_fuel_type(ft)
                    
                    else:
                        ut1, ut2 = "Other", r[data.columns.to_list().index('scc_level_three') + 1]

                        ft1, ft2 = None, None
                    
                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)

            if n == 6:
                    
                for r in data.itertuples():

                    data_index.append(r[0])

                    try:
                        x, y = r[data.columns.to_list().index('scc_level_four') + 1].split(': ')

                    except ValueError:
                        ft1, ft2 = 'Other', 'Other'
                        ut1, ut2 = 'Other combustion', 'Other'

                    else:

                        if any([z in x for z in ['Distillate', 'Residual', 'Gas',
                                                'Liquid', 'Propane']]):

                            ft1, ft2 = self._unit_methods.match_fuel_type(x)

                            ut1, ut2 = self._unit_methods.char_nei_units(y)

                        else:

                            ft1, ft2 = self._unit_methods.match_fuel_type(y)
                            ut1, ut2 = self._unit_methods.char_nei_units(x)

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)
    
            if (n == 6) | (n == 7):
                    
                for r in data.itertuples():

                    data_index.append(r[0])

                    ut1, ut2 = self._unit_methods.char_nei_units(
                        r[data.columns.to_list().index('scc_level_four') + 1]
                        )
                    
                    if not ut2:
                        ut2 = r[data.columns.to_list().index('scc_level_four') + 1]

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(None)
                    fuel_type_lv2.append(None)

            if n == 8:

                for r in data.itertuples():

                    data_index.append(r[0])

                    if r[data.columns.to_list().index('scc_level_three') + 1] == 'Process Heaters':

                        ft1, ft2 = self._unit_methods.match_fuel_type(
                            r[data.columns.to_list().index('scc_level_four') + 1].split(': ')[0]
                            )

                        ut1, ut2 = 'Heater', 'Process heater'

                    else:

                        ft1, ft2 = self._unit_methods.match_fuel_type(
                            r[data.columns.to_list().index('tier_3_description') + 1]
                            )

                        ut1, ut2 = self._unit_methods.char_nei_units(
                            r[data.columns.to_list().index('scc_level_four') + 1]
                            )

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(None)
                    fuel_type_lv2.append(None)

            if n == 9: 
                
                for r in data.itertuples():

                    data_index.append(r[0])

                    unit_type_lv1.append('Other')
                    unit_type_lv2.append(r[data.columns.to_list().index('scc_level_four') + 1])
                    fuel_type_lv1.append(None)
                    fuel_type_lv2.append(None)

            if n == 10:

                for r in data.itertuples():

                    data_index.append(r[0])

                    if r[data.columns.to_list().index('scc_level_two') + 1] == 'In-process Fuel Use':

                        ft1, ft2 = self._unit_methods.match_fuel_type(r[data.columns.to_list().index('scc_level_three') + 1])

                        ut1, ut2 = self._unit_methods.char_nei_units(r[data.columns.to_list().index('scc_level_four') + 1])

                    else:
                        ut1, ut2, ft1, ft2 = self._unit_methods.char_remaining_nei(r, data)

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)

            if n == 11:

                for r in data.itertuples():

                    data_index.append(r[0])

                    ut1, ut2, ft1, ft2 = self._unit_methods.char_remaining_nei(r, data)

                    unit_type_lv1.append(ut1)
                    unit_type_lv2.append(ut2)
                    fuel_type_lv1.append(ft1)
                    fuel_type_lv2.append(ft2)
    
            data_types = pd.DataFrame(
                [[unit_type_lv1, unit_type_lv2, fuel_type_lv1, fuel_type_lv2]],
                columns=all_types_cols,
                index=data_index
                )
            
            types_df = types_df.append(data_types)

        return types_df

    def main(self):
        id_scc = SCC_ID()
        id_scc_df = id_scc.build_id()
        id_scc_df.to_csv('./scc/iden_scc.csv')


if __name__ == '__main__':

    id_scc = SCC_ID().main()
    # all_scc = id_scc.load_complete_scc()
    # id_scc_df = id_scc.build_id()
    # id_scc_df.to_csv('./scc/updated_iden_scc.csv')