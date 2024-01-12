import pandas as pd
import numpy as np
import os
import yaml
import re
import logging
import requests
import zipfile
from io import BytesIO

logging.basicConfig(level=logging.INFO)

class NEI:
    """
    Calculates unit throughput and energy input (later op hours?) from
    emissions and emissions factors, specifically from: PM, SO2, NOX,
    VOCs, and CO.

    Uses NEI Emissions Factors (EFs) and, if not listed, WebFire EFs

    Returns file: 'NEI_unit_throughput_and_energy.csv'

    """

    def __init__(self):

        logging.basicConfig(level=logging.INFO)

        self._nei_data_path = os.path.abspath('./data/NEI/nei_ind_data.csv')
        self._webfires_data_path = \
            os.path.abspath('./data/WebFire/webfirefactors.csv')

        self._unit_conv_path = os.path.abspath('./nei/unit_conversions.yml')

        with open(self._unit_conv_path) as file:
            self._unit_conv = yaml.load(file, Loader=yaml.SafeLoader)

        self._scc_units_path = os.path.abspath('./scc/iden_scc.csv')

        self._data_source = 'NEI'

        self._cap_conv = {
            'energy': {  # Convert to MJ
                'MMBtu/hr': 8760 * 1055.87,
                'MW': 8760 * 3600,
                'KW': 8760 * 3600000
                },
            'power': {  # Convert to MW
                'MMBtu/hr': 0.293297,
                'KW': 1/1000,
                'MW': 1
                }
            }
        
        # from https://www.epa.gov/system/files/documents/2023-03/ghg_emission_factors_hub.pdf
        self._gwp = {
            '100' : {
                'N2O': 298,
                'CH4': 25,
                'CO2': 1
                }
            }

        # Json schema not implemented. 
        # #TODO evaluate for removal. 
        # def import_data_schema(data_source):
        #     """
        #     Import data schema for relevant data set.

        #     Parameters
        #     ----------
        #     data_source : str; "NEI", "GHGRP", "QPC", "FRS"
        #         Source of data

        #     Returns
        #     -------
        #     self._data_schema : dict

        #     """

        #     with open('./nei/extracted_data_schema.json') as file:
        #         data_schema = json.load(file)
        #     data_schema = data_schema[0][data_source]

        #     return data_schema

        # self._data_schema = import_data_schema(self._data_source)

    def find_missing_cap(self, df):
        """
        Look for missing capacity data in unit description

        Parameters
        ----------
        df : pandas.DataFrame
            NEI data.

        Returns
        -------
        df : pandas.DataFrame
            Original data frame with updated capacity data.

        """

        missing_cap = df[df.designCapacity.isnull()]

        found_cap = pd.DataFrame(
            data=[x for x in missing_cap.unitDescription.apply(
                lambda x: self.check_unit_description(x, energy=False)
                )],
            columns=['raw'],
            index=missing_cap.index
            )

        found_cap.dropna(how='all')

        for i, v in enumerate(['designCapacity', 'designCapacityUOM']):
            try:
                data = found_cap.raw.apply(lambda x: x[i])

            except TypeError:
                continue

            else:
                found_cap[v] = data

        found_cap.drop('raw', axis=1, inplace=True)

        df.update(found_cap)

        return df

    def convert_capacity(self, df):
        """
        Converts capacity to MW for NEI dataframe

        Parameters
        ----------
        df : pandas.DataFrame
            NEI data

        Returns
        -------
        df : pandas.DataFrame
            Original dataframe with capacity UOM and design capacity values
            updated

        """
        nei_uom = {
            'E6BTU/HR': 0.29307107,
            'HP': 1/1341,
            'BLRHP': 1/1341,
            'KW': 1/1000,
            'MW': 1,
            'BTU/HR': 0.29307107 * 10**-6
            }

        conv_df = pd.DataFrame.from_dict(nei_uom, orient='index',
                                         columns=['conversion'])

        df = pd.merge(df, conv_df, left_on='designCapacityUOM',
                      right_index=True, how='left')

        conv_ = df.dropna(subset=['conversion'])

        # Facilities reporting capacities in E6BTU/HR may
        # be incorrectly accounting for units and
        # mis-reporting. Largest capacity reported by 
        # GHGRP for years 2015 - 2021 is 10,000 E6BTU/HR 
        # (see https://www.epa.gov/system/files/other-files/2022-10/emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip)
        # Assume these units are meant to be reported as BTU/HR and not E6BTU/HR.

        f = conv_.query(
            'designCapacityUOM=="E6BTU/HR" & designCapacity > 10**5'
            )

        conv_.loc[f.index, 'designCapacity'] = f.designCapacity/10**6

        df.loc[conv_.index, 'designCapacity'] = conv_.designCapacity.multiply(
            conv_.conversion
            )

        df.loc[conv_.index, 'designCapacityUOM'] = 'MW'

        df.drop(['conversion'], axis=1, inplace=True)

        # There's a facility (eisFacilityID == 7622911)
        # that has mis-reported capacity by 6 orders of magnitude
        f = df.query(
            'designCapacityUOM=="MW" & designCapacity > 10**6'
            )

        df.loc[f.index, 'designCapacity'] = f.designCapacity/10**6

        return df

    def check_unit_description(self, unit_description, energy=True):
        """"
        Checks a unit description field using regex to determine if a 
        unit capacity is reported. If a capacity is found, the method
        estimates an annual energy use based on the assumption of
        continuous use (8760 hours/year).

        Parameters
        ----------
        unit_description : str
            Description of unit, which may contain capacity data.

        energy : bool; default is True
            If true, estimates annual energy use (in MJ) of found capacity
        assuming 8760 hours/year operation.

        Returns
        -------
        value : tuple or None
            Tuple of either capacity or estimated annual energy (float), and
        unit of measurement ('MW' or 'MJ'). Capacities and their units are changed to MW.
        None returned if no capacity information is found.

        """
        uom_fixes = {
            'mmbtu/hr': 'MMBtu/hr',
            'mm btu/hr': 'MMBtu/hr',
            'mw': 'MW',
            'million btu per hour': 'MMBtu/hr',
            'mmbtu/hour': 'MMBtu/hr',
            'mbtu/hr': 'MMBtu/hr',
            'mmb': 'MMBtu/hr',
            'kw': 'KW'
            }

        value = None

        s = unit_description

        if type(s) == str:
            for k in uom_fixes.keys():

                sm = re.search(fr'(\S+)(\s)(?=({k}))', s)
                # sm = re.search(r'(\S+)(\s)(?=(mmbtu/hr))', s)

                if sm:

                    # Some unit descriptions have capacities in parentheses
                    sm = sm.group().replace('(', '')
                    sm = sm.replace(',', '')

                    try:
                        smf = float(sm)

                    except ValueError:
                        value = None

                    else:
                        uom = uom_fixes[k]

                        if energy:
                            value = (smf * self._cap_conv['energy'][uom], 'MJ')

                        else:
                            value = (smf * self._cap_conv['power'][uom], 'MW')

                else:
                    continue

        else:
            pass

        return value

    def check_estimates(self, df):
        """
        Check energy estimates. Uses a maximum unit combustion MJ
        estimated from EPA GHGRP data, assuming any estimate above this
        value is an error.
        Re

        Parameters
        ----------
        df : pandas.DataFrame
            NEI data

        Returns
        -------
        df : pandas.DataFrame
            NEI data with offending energy estimates either
            updated or removed.
        """

        # Max estimated unit energy from GHGRP in 2017 (MJ).
        ghgrp_max = 7.925433e+10

        flagged = df.query("energyMJq0 > @ghgrp_max | energyMJq2 > @ghgrp_max | energyMJq3 > @ghgrp_max")
        flagged_min = flagged.query("energyMJq0 < @ghgrp_max")  # Use calculated min instead of design capacity approach below

        # flagged = df.query("energyMJq0 > @ghgrp_max")

        df.loc[flagged.index, ['energyMJq0', 'energyMJq2', 'energyMJq3']] = None

        energy_update = pd.DataFrame(
            index=flagged.index,
            columns=['energyMJq0', 'energyMJq2', 'energyMJq3']
            )

        for i, v in flagged.iterrows():
            if v['designCapacityUOM'] in self._cap_conv['energy'].keys():
                value = self._cap_conv['energy'][v['designCapacityUOM']] * \
                    v['designCapacity']
                energy_update.loc[i, :] = value

        df.loc[flagged_min.index, 
               ['energyMJq0', 'energyMJq2',
                'energyMJq3']] = flagged_min.energyMJq0

        df.update(energy_update)

        return df

    @staticmethod
    def match_partial(full_list, partial_list):
        """
        The NEI file point_678910.csv contains truncated values for
        unit types and calculation methods. This method creates a dictionary
        that has approximate matches to each set of values based on the
        point_12345.csv file. These are approximate matches because the
        truncated values may have multiple matches (e.g., 'S/L/T Emis' matches
        'S/L/T Emission Factor (no Control Efficiency used)' and
        'S/L/T Emission Factor (pre-control) plus Control Efficiency'.

        Parameters
        ----------
        full_list : list of str
            List of complete unit types or calculation methods

        partial_list : list of str
            List of truncated unit types or calculation methods

        Returns
        -------
        matching_dict : dictionary of str
            Dictionary of {partial: match}.

        """

        matching_dict = {}

        for k in partial_list:
            len_k = len(k)
            matches = [k == v[0:len_k] for v in full_list]
            m_index = [i for i, val in enumerate(matches) if val]

            try:
                full = full_list[m_index[0]]  # use first match

            except IndexError:
                continue

            else:
                if full == k:
                    continue

                else:
                    matching_dict[k] = full

        return matching_dict

    def load_nei_data(self):
        """
        Load 2017 NEI data. Zip file needs to be downloaded and
        unzipped manually from https://gaftp.epa.gov/air/nei/2017/data_summaries/2017v1/2017neiJan_facility_process_byregions.zip
        due to error in zipfile library.

        Returns
        -------
        nei_data : pandas.DataFrame
            Raw NEI data.
        """


        if os.path.exists(self._nei_data_path):

            logging.info('Reading NEI data from csv')

            try:
                nei_data = pd.read_csv(self._nei_data_path, low_memory=False,
                                       index_col=0)

            except (TypeError, ValueError):  # NEI data set has many columns with mixed dtypes
                logging.error("Mixed types in NEI data")
            #     dts = {
            #         'epa_region_code': str,
            #         'state': str,
            #         'fips_state_code': str,
            #         'tribal_name': str,
            #         'fips_code': str,
            #         'county': str,
            #         'eis_facility_id': object,
            #         'program_system_code': str,
            #         'agency_facility_id': str,
            #         'tri_facility_id': str,
            #         'company_name': str,
            #         'site_name': str,
            #         'naics_code': object,
            #         'naics_description': str,
            #         'facility_source_type':	str,
            #         'site_latitude': object,
            #         'site_longitude': object,
            #         'address': str,
            #         'city': str,
            #         'zip_code':	object,
            #         'postal_abbreviation': str,
            #         'eis_unit_id': str,
            #         'agency_unit_id': str,
            #         'unit_type': str,
            #         'unit_description':	str,
            #         'design_capacity': object,
            #         'design_capacity_uom': str,
            #         'eis_process_id': object,
            #         'agency_process_id': str,
            #         'scc': object,
            #         'reg_codes': str,
            #         'reg_code_description': str,	
            #         'process_description': str,
            #         'reporting_period': str,
            #         'emissions_operating_type': str,
            #         'calculation_parameter_value': str,
            #         'calculation_parameter_uom': str,
            #         'calculation_material': str,
            #         'calculation_parameter_type': str,
            #         'calc_data_source': object,
            #         'calc_data_year': str,
            #         'pollutant_code': str,
            #         'pollutant_desc': str, 
            #         'pollutant_type': str,
            #         'total_emissions': object,
            #         'emissions_uom': str,
            #         'emission_factor': object,
            #         'ef_numerator_uom':	str,
            #         'ef_denominator_uom': str,
            #         'ef_text': str,
            #         'calc_method_code':	object,
            #         'calculation_method': str,
            #         'emission_comment': str,
            #         'source_data_set': str,
            #         'data_tagged': str,
            #         'data_set': str
            #         }
            # # nei_data = pd.read_csv(self._nei_data_path,
            # #                        index_col=0, dtype=dts)
                
            # nei_data = pd.DataFrame()

            # for k,v in dts.items():

            #     try:
            #         d = pd.read_csv(
            #             self._nei_data_path, index_col=0, 
            #             usecols=[k], dtype=v
            #             )
                    
            #     except (ValueError, TypeError) as e:
            #         logging.error(f'{e}: \ncolumn: {k} with dtype: {v}')
            #         continue

            #     except AttributeError as e:
            #         logging.error(f'{e}')
            #         continue

            #     else:
            #         nei_data = pd.concat([nei_data, d], axis=0)

        else:

            logging.info('Reading NEI data from zipfiles')

            nei_data = pd.DataFrame()

            for f in os.listdir(os.path.dirname(self._nei_data_path)):

                if '.csv' in f:

                    if f == 'point_unknown.csv':
                        continue

                    else:

                        data = pd.read_csv(
                                os.path.join(
                                    os.path.dirname(self._nei_data_path), f
                                    ),
                                low_memory=False
                                )

                        data.columns = data.columns.str.strip()
                        data.columns = data.columns.str.replace(' ', '_')

                        # For some reason csvs have different column naming conventions
                        data.rename(columns={
                            'stfips': 'fips_state_code',
                            'fips': 'fips_code',
                            'pollutant_type(s)': 'pollutant_type',
                            'region': 'epa_region_code'
                            }, inplace=True)

                    # unit types & emissions calc methods in point_678910.csv are truncated; 
                    # match to full unit type names in point_12345.csv
                    # Also, match to full calculation method names in point_12345.csv
                    if '12345' in f:
                        full_unit = list(data.unit_type.unique())
                        full_method = list(data.calculation_method.unique())

                    elif '6789' in f:
                        partial_unit = list(data.unit_type.unique())
                        partial_method = \
                            list(data.calculation_method.unique())

                    else:
                        pass

                    nei_data = nei_data.append(data, sort=False)

                else:
                    continue

                    # zip_path = os.path.join(os.path.dirname(nei_data_path), f)
                    # with zipfile.ZipFile(zip_path) as zf:
                    #     for k in zf.namelist():
                    #         logging.info(f'File {k}')
                    #         if '.csv' in k:
                    #             # zipfile throws NotImplementedError: compression type 9 (deflate64)
                    #             # for the point source zip file.                         try:
                    #                 with zf.open(k) as kf:
                    #                     data = pd.read_csv(kf, low_memory=False)
                    #                     data.columns = \
                    #                         data.columns.str.replace(' ', '_')

                    #             except NotImplementedError:
                    #                 continue
                    #             else:
                    #                 nei_data = nei_data.append(data, sort=False)
                    #         else:
                    #             continue

            # nei_data_process = nei_data.drop_duplicates(
            #     subset=['eis_facility_id', 'eis_process_id']
            #     )
            unit_matches = NEI.match_partial(full_unit, partial_unit)
            meth_matches = NEI.match_partial(full_method, partial_method)

            nei_data.replace({'unit_type': unit_matches}, inplace=True)
            nei_data.replace({'calculation_method': meth_matches}, inplace=True)

            nei_naics = pd.DataFrame(
                nei_data.naics_code.unique(), columns=['naics_code']
                )

            nei_naics.loc[:, 'naics_sub'] = \
                nei_naics.naics_code.astype(str).str[:3].astype(int)
            nei_naics.loc[:, 'ind'] = [
                str(x)[0:2] in ['11', '21', '23', '31', '32', '33'] for x in nei_naics.naics_sub
                ]
            nei_naics = nei_naics[nei_naics.ind == True]['naics_code']

            # Keep only industrial facilities
            nei_data = pd.merge(
                nei_data, nei_naics, on='naics_code',
                how='inner'
                )

            #  nei_data.to_csv(self._nei_data_path)

        return nei_data

    def load_webfires(self):
        """
        Load all EPA WebFire emissions factors, downloading from
        https://www.epa.gov/electronic-reporting-air-emissions/webfire
        if necessary.

        Returns
        -------
        webfr : pandas.DataFrame
            EPA WebFire emissions factors. 
        """

        if os.path.exists(self._webfires_data_path):

            logging.info('Reading WebFire data from csv')

            webfr = pd.read_csv(self._webfires_data_path, low_memory=False)

        else:

            logging.info(
                'Downloading WebFire data; writing webfirefactors.csv'
                )

            r = requests.get(
                'https://cfpub.epa.gov/webfire/download/webfirefactors.zip'
                )

            with zipfile.ZipFile(BytesIO(r.content)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    webfr = pd.read_csv(f, low_memory=False)
                    webfr.to_csv(self._webfires_data_path)

        return webfr

    def load_unit_conversions(self):
        """
        Load unit conversions and fuel dictionary
        """

        with open(self._unit_conv_path) as file:
            unit_conv = yaml.load(file, Loader=yaml.FullLoader)

        return unit_conv

    def load_scc_unittypes(self):
        """
        Load unit types (and fuel types) gleaned from SCC data.

        Returns
        -------
        iden_scc : pandas.DataFrame
            SCC codes with identified unit types and fuel types.

        """

        iden_scc = pd.read_csv(self._scc_units_path, index_col=0)
        iden_scc.reset_index(drop=True, inplace=True)

        iden_scc.loc[:, 'SCC'] = iden_scc.SCC.astype('int64')

        iden_scc.rename(columns={
            'unit_type': 'scc_unit_type',
            'fuel_type': 'scc_fuel_type'}, inplace=True
            )

        return iden_scc
    
    def extract_ghg_emissions(self, nei_data):
        """
        Capture GHG emissions (i.e., CO2, CH4, and N2O)
        reported under NEI. Convert to tonnes CO2 equivalent (tonnesCO2e)

        
        Parameters
        ----------
        nei_data : pandas.DataFrame
            Formatted NEI data.

        Returns
        -------
        ghgs : pandas.DataFrame
            GHG emissions aggregated by eis_facility_id, eis_unit_id,
            eis_process_id, fuelType, and fuelTypeStd
        
        """

        ghgs = pd.DataFrame(
            nei_data.query("pollutant_code=='CO2'|pollutant_code=='CH4'|pollutant_code=='N2O'")
            )

        # convert short tons to metric tonnes
        # Method currently only works if all units of emissions are in short tons
        if (len(ghgs.emissions_uom.unique())==1) & (ghgs.emissions_uom.unique()[0]=='TON') is True:
    
            ghgs.loc[:, 'ghgsTonneCO2e'] = ghgs.apply(lambda x: x.total_emissions * self._gwp['100'][x.pollutant_code] * 0.907,
                                                   axis=1)
            
        else:
            raise IndexError("Reported emissions have additional units of measurement")

        # aggregated to facility, unit, and process and fuel type
        ghgs = ghgs.groupby(
            ['eis_facility_id', 'eis_unit_id', 'eis_process_id', 'fuel_type'],
            as_index=False
            ).ghgsTonneCO2e.sum()
        
        # Make ghg columns consistent with energy columns. Because there is only one
        # reported value, these are all equal.
        ghgs.loc[:,  ['ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']] = \
            np.tile(ghgs.ghgsTonneCO2e.values, (3, 1)).T
        
        ghgs.drop(['ghgsTonneCO2e'], axis=1, inplace=True)
        
        return ghgs


    def merge_fill_ghg_emissions(self, ghgs, nei_data):
        """
        Not all NEI facilities report GHG emissions from fuel combustion. Calculate these missing values using 
        EPA default emissions factors.

        Parameters
        ----------
        ghgs : pandas.DataFrame
            Emissions reported directly by NEI

        nei_data : pandas.DataFrame
            Formatted NEI data with energy calculations.

        Returns
        -------
        nei_data : pandas.DataFrame
            Formatted NEI data with energy and GHG emissions calculations.

        """

        nei_data = pd.merge(
            nei_data, ghgs,
            on=['eis_facility_id', 'eis_unit_id', 'eis_process_id', 'fuel_type'],
            how='left'
            )

        efs = pd.concat(
            [nei_data.fuel_type.dropna(), 
             nei_data.fuel_type.dropna().apply(lambda x: self._unit_conv['energy_units'][x]['MJ_to_KGCO2e'])],
            axis=1
            )
        
        efs = dict(efs.drop_duplicates().values)

        nei_data.loc[:, 'ef'] = nei_data.fuel_type.map(efs)

        emissions = \
            nei_data[nei_data.ghgsTonneCO2eQ2.isnull()][['energy_MJ_q0','energy_MJ_q2', 'energy_MJ_q3']].multiply(nei_data.ef, axis=0)/1000

        emissions.columns = ['ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']

        nei_data.update(emissions)

        nei_data.drop('ef', axis=1, inplace=True)
    
        return nei_data


    def match_webfire_to_nei(self, nei_data, webfr):
        """
        Match WebFire EF data to NEI data

        Parameters
        ----------
        nei_data : pandas.DataFrame
            NEI emissions data

        webfr : pandas.DataFrame
            WebFires Emissions Factors

        Returns
        -------
        nei_emiss : pandas.DataFrame
        """

        # remove duplicate EFs for the same pollutant and SCC; keep max EF
        webfr = webfr.sort_values('FACTOR').drop_duplicates(
            subset=['SCC', 'NEI_POLLUTANT_CODE'], keep='last'
            )

        # use only NEI emissions of PM, CO, NOX, SOX, VOC
        nei_emiss = nei_data[
            nei_data.pollutant_code.str.contains('PM|CO|NOX|NO3|SO2|VOC')
            ].copy()

        nei_emiss = pd.merge(
            nei_emiss,
            webfr[['SCC', 'NEI_POLLUTANT_CODE', 'FACTOR', 'UNIT', 'MEASURE',
                   'MATERIAL', 'ACTION']],
            left_on=['scc', 'pollutant_code'],
            right_on=['SCC', 'NEI_POLLUTANT_CODE'],
            how='left'
            )

        nei_emiss.rename(columns={'SCC': 'SCC_web'}, inplace=True)

        return nei_emiss

    def assign_types_nei(self, nei, iden_scc):
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

        # set unit type equal to SCC unit type if listed as
        #   'Unclassified' or'Other' in NEI
        nei.loc[(nei['unit_type'] == 'Unclassified') |
                (nei['unit_type'] == 'Other process equipment'), 'unit_type'] = \
                    nei['scc_unit_type']

        nei.loc[(nei['unit_type'] == 'Unclassified') |
                (nei['unit_type'] == 'Other process equipment'), 'unit_type'] = \
                    nei['scc_unit_type']

        # get fuel types from NEI text and SCC descriptions
        for c in ['unit_description', 'process_description', 'scc_fuel_type']:
            nei.loc[:, c] = nei[c].str.lower()

        nei.loc[:, 'fuel_type'] = nei.loc[:, 'scc_fuel_type']

        # #TODO too many fuel type standardizations happening SCC -> NEI -> std fuel types
        # Should streamline this process to avoid errors.
        fuels = {k:None for k in nei.fuel_type.dropna().unique()}

        for f in fuels.keys():

            if f.find('(') != -1:
    
                fre = f.replace('(', '\(').replace(')', '\)')

                n = {k: re.search(fre, k) for k in self._unit_conv['fuel_dict'].keys()}

            else:

                n = {k: re.search(f, k) for k in self._unit_conv['fuel_dict'].keys()}

            if any(n.values()):

                fk = [k for k in n.keys() if n[k] is not None]

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

        # remove some non-combustion related unit types
        nei = self.remove_unit_types(nei) 

        return nei

    # for throughput calculation,
    #   convert NEI emissions to LB; NEI EFs to LB/TON;
    #   and WebFire EFs to LB/TON

    # for energy input calculation,
    #   convert NEI emissions to LB; NEI EFs to LB/MJ; 
    #   and WebFire EFs to LB/MJ

    def convert_emissions_units(self, nei):
        """
        Convert reported emissions factors into emissions factors that
        can be used to estimate mass throughput (in short tons) or
        energy (in MJ).
        Uses conversion factors defined in self._unit_conv.

        Parameters
        ----------
        nei : pandas.DataFrame
            Raw NEI data.
    
        Returns
        -------
        nei : pandas.DataFrame
            NEI with mass and throughput coversion factors. 
    
        """

        # map unit of emissions and EFs in NEI/WebFire to unit conversion key
        # convert NEI total emissions value to LB
        nei.loc[:, 'emissions_conv_fac'] = nei['emissions_uom'].map(
            self._unit_conv['unit_to_lb']
            ).map(self._unit_conv['basic_units'])

        nei.loc[:, 'total_emissions_LB'] = \
            nei['total_emissions'] * nei['emissions_conv_fac']

        # convert NEI emission_factor numerator to LB
        nei.loc[:, 'nei_ef_num_fac'] = nei['ef_numerator_uom'].map(
            self._unit_conv['unit_to_lb']
            ).map(self._unit_conv['basic_units'])

        # convert NEI emission_factor to LB/TON for throughput
        nei.loc[:, 'nei_ef_denom_fac'] = nei['ef_denominator_uom'].map(
            self._unit_conv['unit_to_ton']).map(
                self._unit_conv['basic_units']
                )

        nei.loc[:, 'nei_ef_LB_per_TON'] = \
            nei['emission_factor'] * nei['nei_ef_num_fac'] / nei['nei_ef_denom_fac']

        # convert NEI emission_factor to LB/MJ for energy input
        for f in nei.fuel_type.dropna().unique():

            try:
                nei.loc[nei.fuel_type == f, 'nei_denom_fuel_fac'] = \
                    nei['ef_denominator_uom'].map(
                        self._unit_conv['unit_to_mj']).map(
                            self._unit_conv['energy_units'][f]
                            )

            except KeyError:
                continue

        # if there is no fuel type listed,
        #   use energy to energy units only OR assume NG for E6FT3
        nei.loc[(nei.fuel_type.isnull()) &
                ((nei.ef_denominator_uom == 'E6BTU') |
                (nei.ef_denominator_uom == 'HP-HR') |
                (nei.ef_denominator_uom == 'THERM') |
                (nei.ef_denominator_uom == 'E6FT3')), 'nei_denom_fuel_fac'] = \
            nei['ef_denominator_uom'].map(
                self._unit_conv['unit_to_mj']).map(
                    self._unit_conv['energy_units']['natural_gas']
                    )

        nei['nei_denom_fuel_fac'] = nei['nei_denom_fuel_fac'].astype(float)

        nei.loc[:, 'nei_ef_LB_per_MJ'] = \
            nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_denom_fuel_fac']

        # WebFire----------------------------------------------
        nei['UNIT'] = nei['UNIT'].str.upper()

        nei['FACTOR'] = pd.to_numeric(nei['FACTOR'], errors='coerce')

        nei.replace({'MEASURE': self._unit_conv['measure_dict']}, inplace=True)

        # convert WebFire EF numerator to LB
        nei.loc[:, 'web_ef_num_fac'] = nei['UNIT'].map(
            self._unit_conv['unit_to_lb']
            ).map(self._unit_conv['basic_units'])

        # convert WebFire EF to LB/TON for throughput
        nei.loc[:, 'web_ef_denom_fac'] = nei['MEASURE'].map(
            self._unit_conv['unit_to_ton']
            ).map(self._unit_conv['basic_units'])

        nei.loc[:, 'web_ef_LB_per_TON'] = \
            nei['FACTOR']*nei['web_ef_num_fac']/nei['web_ef_denom_fac']

        # convert WebFire EF to LB/E6BTU for energy input
        for f in nei.fuel_type.dropna().unique():

            try:
                nei.loc[nei.fuel_type == f, 'web_denom_fuel_fac'] = nei['MEASURE'].map(
                    self._unit_conv['unit_to_mj']
                    ).map(self._unit_conv['energy_units'][f])

            except KeyError:
                continue

        # if there is no fuel type listed, 
        #   use energy to energy units only OR assume NG for E6FT3
        nei.loc[(nei.fuel_type.isnull()) &
                (nei.MEASURE == 'E6BTU') |
                (nei.MEASURE == 'HP-HR') |
                (nei.MEASURE == 'THERM') |
                (nei.MEASURE == 'E6FT3'), 'web_denom_fuel_fac'] = \
            nei['MEASURE'].map(
                self._unit_conv['unit_to_mj']).map(
                self._unit_conv['energy_units']['natural_gas']
                )

        nei['web_denom_fuel_fac'] = nei['web_denom_fuel_fac'].astype(float)

        nei.loc[:, 'web_ef_LB_per_MJ'] = \
            nei['FACTOR']*nei['web_ef_num_fac']/nei['web_denom_fuel_fac']

        return nei

    def calc_unit_throughput_and_energy(self, nei):
        """
        Calculate throughput quantity in TON and energy input in MJ using
        emissions factors converted with convert_emissions_units().

        Parameters
        ----------
        nei : pandas.DataFrame

        Returns
        -------
        nei : pandas.DataFrame

        """

        # check for "Stack Test" emissions factor method where units are wrong
        #   according the to the emission comment text; remove emission factor
        check_nei_ef_idx = (
            (nei['emission_factor'] > 0) &
            (nei['calc_method_code'] == 4) &
            (nei['emission_comment'].str.contains(
                'lb/hr|#/hr|lbs/hr|Lb/hr'
                )) &
            (nei['ef_denominator_uom'] != 'HR')
            )

        nei.loc[check_nei_ef_idx, 'nei_ef_LB_per_TON'] = np.nan

        for f in ['nei', 'web']:

            for v in ['throughput_TON', 'energy_MJ']:

                nei.loc[:, f'{v}_{f}'] = nei.total_emissions_LB.divide(
                    nei[f'{f}_ef_LB_per_{v.split("_")[1]}']
                    )

            # remove throughput_TON if WebFire ACTION is listed as Burned
            nei.loc[(~nei[f'throughput_TON_{f}'].isnull()) & 
                (nei['ACTION'] == 'Burned'), f'throughput_TON_{f}'] = np.nan


        # # if there is an NEI EF, use NEI EF
        # nei.loc[(nei['nei_ef_LB_per_TON'] > 0), 'throughput_TON'] = \
        #     nei['total_emissions_LB'] / nei['nei_ef_LB_per_TON']

        # nei.loc[(nei['nei_ef_LB_per_MJ'] > 0), 'energy_MJ'] = \
        #     nei['total_emissions_LB'] / nei['nei_ef_LB_per_MJ']

        # # if there is not an NEI EF, use WebFire EF
        # nei.loc[(nei['nei_ef_LB_per_TON'].isnull()) & 
        #         (nei['web_ef_LB_per_TON'] > 0), 'throughput_TON'] = \
        #     nei['total_emissions_LB'] / nei['web_ef_LB_per_TON'] 

        # nei.loc[(nei['nei_ef_LB_per_MJ'].isnull()) & 
        #         (nei['web_ef_LB_per_MJ'] > 0), 'energy_MJ'] = \
        #     nei['total_emissions_LB'] / nei['web_ef_LB_per_MJ']

        # # remove throughput_TON if WebFire ACTION is listed as Burned
        # nei.loc[(~nei['throughput_TON'].isnull()) & 
        #         (nei['ACTION'] == 'Burned'), 'throughput_TON'] = np.nan


        return nei


    def get_median_throughput_and_energy(self, nei):
        """
        Use the lower, middle, and upper quartiles for estimated throughput_TON 
        and energy_MJ for individual units.
        This addresses the case where a unit reports multiple emissions
        and those emissions are used to estimate throughput or energy.

        Parameters
        ----------
        nei : pandas.DataFrame
            Formatted emissions data with estimated throughput and
            energy.

        Returns
        -------
        med_unit : pandas.DataFrame
            Lower, middle, and upper quartiles of throughput and
            energy estimates.

        """
        # This is the primary output of estimating throughput and energy from NEI

        med_unit = pd.concat(
            [pd.melt(
                nei[['eis_facility_id',
                     'eis_process_id',
                     'eis_unit_id',
                     'unit_type',
                     'fuel_type',
                     f'{v}_nei',
                     f'{v}_web']],
                id_vars=['eis_facility_id',
                         'eis_process_id',
                         'eis_unit_id',
                         'unit_type',
                         'fuel_type'],
                value_vars=[f'{v}_nei',
                            f'{v}_web'],
                var_name='EF_source',
                value_name=f'{v}'
                ) for v in ['energy_MJ', 'throughput_TON']], axis=0, sort=True
            )

        med_unit = med_unit.query(
                "throughput_TON > 0 | energy_MJ > 0"
                ).groupby(
                    ['eis_facility_id',
                     'eis_process_id',
                     'eis_unit_id',
                     'unit_type',
                     'fuel_type']
                    )[['throughput_TON', 'energy_MJ']].quantile([0, 0.5, 0.75])

        med_unit.reset_index(inplace=True)
        med_unit.level_5.replace({0: 'q0', 0.5: 'q2', 0.75: 'q3'}, 
                                 inplace=True)
        med_unit = med_unit.pivot_table(
            index=['eis_facility_id',
                   'eis_process_id',
                   'eis_unit_id',
                   'unit_type',
                   'fuel_type'],
            columns='level_5', values=['energy_MJ', 'throughput_TON'])

        m = med_unit.columns.map('_'.join)
        med_unit = med_unit.groupby(m, axis=1).mean()

        # med_unit = nei.query(
        #     "throughput_TON > 0 | energy_MJ > 0"
        #     ).groupby(
        #         ['eis_facility_id',
        #          'eis_process_id',
        #          'eis_unit_id',
        #          'unit_type',
        #          'fuel_type']
        #         )[['throughput_TON', 'energy_MJ']].median()

        # other_info = med_unit.drop_duplicates([
        #     'eis_facility_id',
        #     'eis_process_id',
        #     'eis_unit_id'])

        other_cols = nei.columns
        other_cols = set(other_cols).difference(set(med_unit.columns))

        # logging.info(f"Other columns:  {other_cols}")

        other = nei.drop_duplicates(
            ['eis_facility_id', 'eis_process_id',
             'eis_unit_id', 'unit_type',
             'fuel_type']
            )[other_cols]

        med_unit = med_unit.join(
            other.set_index(
                ['eis_facility_id', 'eis_process_id',
                 'eis_unit_id', 'unit_type',
                 'fuel_type']
                )
            )

        med_unit.reset_index(inplace=True)

        # MATERIAL and ACTION columns can have multiple values for same
        #   eis_process_id so if included in groupby, need to remove duplicates

        #med_unit.drop(med_unit[
        #    (med_unit.eis_process_id.duplicated(keep=False)==True) & 
        #    (med_unit.MATERIAL.isnull())].index, inplace=True)

        # med_unit.to_csv('NEI_unit_throughput_and_energy.csv', index=False)
        # med_unit = self.format_nei_char(med_unit)

        return med_unit

    def separate_missing_units(self, nei):
        """
        Separate facilities that have not had
        any of their units characterized in terms of
        throughput or energy use, but have identified
        unit types.

        Parameters
        ----------
        nei : pandas.DataFrame
            Formatted emissions data with estimated throughput and
            energy.

        Returns
        -------
        missing : pandas.DataFrame
            All facilities and unit types that are missing
            throughput and energy estimates, but have identified
            unit types.

        """

        missing_zero = nei.query(
            "throughput_TON_nei==0 & energy_MJ_nei==0 & throughput_TON_web==0 & energy_MJ_web==0"
            )
        missing_zero = missing_zero.where(
            missing_zero.unit_type.notnull()
            ).dropna(how='all')

        missing_na = nei.query(
            "throughput_TON_nei.isna() & energy_MJ_nei.isna() & throughput_TON_web.isna() & energy_MJ_web.isna()",
            engine='python'
            )

        missing_na = missing_na.where(
            missing_na.unit_type.notnull()
            ).dropna(how='all')

        missing = pd.concat(
            [missing_zero, missing_na], axis=0,
            ignore_index=False
            )

        missing.drop_duplicates(
            ['eis_facility_id', 'eis_unit_id', 'eis_process_id', 'fuel_type'],
            inplace=True)

        # missing = self.format_nei_char(missing)

        return missing

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

    def remove_unit_types(self, df):
        """
        Remove records associated with unit types that are not associated
        with combustion or electricity use.

        Parameters
        ----------
        df : pandas.DataFrame
            NEI data.

        Returns
        -------
        df : pands.DataFrame
            NEI data with records removed based on unitType.
        """

        remove = [
            'Storage Tank',
            'Process Equipment Fugitive Leaks',
            'Transfer Point',
            'Open Air Fugitive Source',
            'Other fugitive',
            'Silo'
            ]

        df = df.where(~df.unit_type.isin(remove)).dropna(how='all')

        df.reset_index(inplace=True, drop=True)

        return df

    #TODO use tools method
    def harmonize_fuel_type(self, ghgrp_unit_data, fuel_type_column):
        """
        Applies fuel type mapping to fuel types reported under GHGRP

        Parameters
        ----------
        ghgrp_unit_data : pandas.DataFrame
            GHGRP energy estimates with unit information.

        fuel_type_column : str
            Name of column containing fuel types.

        Returns
        -------
        ghgrp_unit_data : pandas.DataFrame
            GHGRP energy estimates with unit information that now
            have a standardized fuel type.
        """

        fuel_dict = self.load_fueltype_dict()

        ghgrp_unit_data.loc[:, 'fuelTypeStd'] = ghgrp_unit_data[fuel_type_column].map(fuel_dict)


        # drop any fuelTypes that are null
        ghgrp_unit_data = ghgrp_unit_data.where(
            ghgrp_unit_data[fuel_type_column] != 'None'
            ).dropna(how='all')

        return ghgrp_unit_data

    def format_nei_char(self, df):
        """"
        Format characterization of NEI data for further processing.
        Removes uncessary columns from NEI data.

        Paramters
        ---------
        df : pandas.DataFrame
            NEI data.

        Returns
        -------
        nei_char : pandas.DataFrame
            NEI data with formatted columns and throughput 
            converted to metric tons. 

        """

        # Data of interest. eis_facility_id used to merge into FRS data.

        rename_dict = {
            'eis_facility_id': 'eisFacilityID',
            'eis_unit_id': 'eisUnitID',
            'unit_type': 'unitType',
            'unit_description': 'unitDescription',
            'design_capacity': 'designCapacity',
            'design_capacity_uom': 'designCapacityUOM',
            'fuel_type': 'fuelType',
            'eis_process_id': 'eisProcessID',
            'process_description': 'processDescription',
            'throughput_Tonne': 'throughputTonne',
            'energy_MJ_q0': 'energyMJq0',
            'energy_MJ_q2': 'energyMJq2',
            'energy_MJ_q3': 'energyMJq3'
            }

        for q in ['q0', 'q2', 'q3']:

            df.loc[:, f'throughputTonneQ{q[1]}'] = \
                df[f'throughput_TON_{q}'] * 0.907  # Convert to metric tonnes

        keep_cols = [
            'eis_facility_id', 'eis_unit_id', 'SCC',
            'unit_type', 'unit_description', 'design_capacity',
            'design_capacity_uom', 'fuel_type', 'eis_process_id',
            'process_description',
            'energy_MJ_q0', 'energy_MJ_q2', 'energy_MJ_q3',
            'throughputTonneQ0', 'throughputTonneQ2', 'throughputTonneQ3',
            'ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3', 
            ]

        df = df[keep_cols]

        df.rename(columns=rename_dict, inplace=True)

        # levels = [k for k in self._data_schema.keys() if self._data_schema[k]==True]

        # df.set_index(levels, inplace=True)

        df = self.harmonize_fuel_type(df, 'fuelType')

        return df

    def merge_med_missing(self, med_unit, missing_unit):
        """
        Merge facility, unit, and process IDs with missing and estimated
        energy and throughput

        Parameters
        ----------
        med_unit : pandas.DataFrame
            Quartiles estimates of energy and throughput.

        missing_unit : pandas.DataFrame
            All facilities and unit types that are missing
            throughput and energy estimates, but have identified
            unit types.

        Returns
        -------
        nei_char : pandas.DataFrame
            Formatted NEI data with quartile energy and througput estimates. 
        """

        med_unit.set_index(
            ['eis_facility_id', 'eis_process_id', 'eis_unit_id'],
            inplace=True
            )

        missing_unit.set_index(
            ['eis_facility_id', 'eis_process_id', 'eis_unit_id'],
            inplace=True
            )

        missing_unit = missing_unit.loc[missing_unit.index.drop_duplicates()]

        missing_unit = missing_unit[~missing_unit.index.isin(med_unit.index)]

        nei_char = pd.concat(
            [med_unit, missing_unit], axis=0,
            ignore_index=False, sort=True
            )

        nei_char.sort_index(level=[0, 1, 2], inplace=True)

        nei_char.reset_index(inplace=True)

        return nei_char

    def main(self):

        nei = NEI()
        logging.info("Getting NEI data...")
        nei_data = nei.load_nei_data()
        iden_scc = nei.load_scc_unittypes()
        webfr = nei.load_webfires()
        logging.info("Merging WebFires data...")
        nei_char = nei.match_webfire_to_nei(nei_data, webfr)
        logging.info("Merging SCC data...")
        nei_char = nei.assign_types_nei(nei_char, iden_scc)
        logging.info("Converting emissions units...")
        nei_char = nei.convert_emissions_units(nei_char)
        logging.info("Estimating throughput and energy...")
        nei_char = nei.calc_unit_throughput_and_energy(nei_char)
        logging.info("Extracting and aggregating GHG emissions")
        ghgs = nei.extract_ghg_emissions(nei_char)
        logging.info("Final NEI data assembly...")

        med_unit = nei.get_median_throughput_and_energy(nei_char)
        missing_unit = nei.separate_missing_units(nei_char)

        nei_char = nei.merge_med_missing(med_unit, missing_unit)

        # nei_char = pd.concat(
        #     [nei.get_median_throughput_and_energy(nei_char),
        #         nei.separate_missing_units(nei_char)], axis=0,
        #     ignore_index=True
        #     )

        logging.info("Merging and filling GHG emissions")
        nei_char = nei.merge_fill_ghg_emissions(ghgs, nei_char)

        nei_char = nei.format_nei_char(nei_char)

        nei_char = nei.find_missing_cap(nei_char)  # Fill in missing capacity data, where possible
        nei_char = nei.convert_capacity(nei_char)  # Convert energy capacities all to MW
        nei_char = nei.check_estimates(nei_char)  # check estimates 

        return nei_char

# #TODO figure out tests to check calculations and other aspects of code.

if __name__ == '__main__':

    nei_char = NEI().main()
    nei_char.to_csv('formatted_estimated_nei_updated.csv')
