import pandas as pd
import numpy as np
import os
import yaml
import re
import logging
import requests
import zipfile
import sys
from io import BytesIO
from pathlib import Path
toolspath = str(Path(__file__).parents[1]/"tools")
sys.path.append(toolspath)
from tools import unit_matcher


logging.basicConfig(level=logging.INFO)

class NEI ():
    """
    Calculates unit throughput and energy input (later op hours?) from
    emissions and emissions factors, specifically from: PM, SO2, NOX,
    VOCs, and CO.

    Uses NEI Emissions Factors (EFs) and, if not listed, WebFire EFs

    Returns file: 'NEI_unit_throughput_and_energy.csv'

    """

    def __init__(self):

        logging.basicConfig(level=logging.INFO)

        self._FIEDPATH = Path(__file__).parents[1]
        
        self._nei_data_path = Path(self._FIEDPATH, "data/NEI/nei_ind_data.csv")
        self._nei_folder_path = Path(self._FIEDPATH,'data/NEI')
        
        self._webfires_data_path = Path(self._FIEDPATH, "data/WebFire/webfirefactors.csv")

        self._unit_conv_path = Path(self._FIEDPATH, "nei/unit_conversions.yml")

        with open(self._unit_conv_path) as file:
            self._unit_conv = yaml.load(file, Loader=yaml.SafeLoader)

        self._scc_units_path = Path(self._FIEDPATH, "scc/iden_scc.csv")

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
                'MW': 1,
                'BHP': 0.0007457  # Assume BHP == brake horsepower
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
        
        self.unit_regex = unit_matcher.UnitsFuels().unit_regex
        self.match_fuel_type = unit_matcher.UnitsFuels().match_fuel_type

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

        found_cap.dropna(how='all', inplace=True)

        found_cap.loc[:, 'designCapacityUOM'] = None

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
            'kw': 'KW',
            'bhp': 'BHP'
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

        #should use this to check energy_MJ_nei values against energy_MJ_web values
        # as well as energy_MJ_nei values > ~5E9

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

    def load_nei_data(self,year):
        """
        Load 2017 NEI data. Zip file needs to be downloaded and
        unzipped manually from https://gaftp.epa.gov/air/nei/2017/data_summaries/2017v1/2017neiJan_facility_process_byregions.zip
        due to error in zipfile library.

        Load 2020 NEI data. Zip file needs to be downloaded and
        unzipped manually from https://gaftp.epa.gov/air/nei/2020/data_summaries/2020nei_facility_process_byregions.zip
        due to error in zipfile library.

        Returns
        -------
        nei_data : pandas.DataFrame
            Raw NEI data.
        """

        if self._nei_data_path.exists():

            logging.info('Reading NEI data from csv')

            try:
                nei_data = pd.read_csv(self._nei_data_path, low_memory=False,
                                       index_col=0)

            except (TypeError, ValueError):  # NEI data set has many columns with mixed dtypes
                logging.error("Mixed types in NEI data")

        else:

            logging.info('Reading NEI data from zipfiles')

            nei_data = pd.DataFrame()
           
            if year == '2017':

                for f in os.listdir(os.path.join(self._nei_folder_path,str(year))):

                    if '.csv' in f:
                        
                        if f == 'point_unknown_2017.csv':
                            continue

                        else:

                            data = pd.read_csv(
                                    os.path.join(
                                        os.path.join(self._nei_folder_path,str(year)), f
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
                            
                        full_unit = []
                        full_method = []
                        partial_unit = []
                        partial_method = []    

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

                        if partial_unit and full_unit:
                            unit_matches = NEI.match_partial(full_unit, partial_unit)
                            nei_data.replace({'unit_type': unit_matches}, inplace=True)

                        if partial_method and full_method:
                            meth_matches = NEI.match_partial(full_method, partial_method)
                            nei_data.replace({'calculation_method': meth_matches}, inplace=True)

                        #unit_matches = NEI.match_partial(full_unit, partial_unit)
                        #meth_matches = NEI.match_partial(full_method, partial_method)

                        #nei_data.replace({'unit_type': unit_matches}, inplace=True)
                        #nei_data.replace({'calculation_method': meth_matches}, inplace=True)

                    nei_data = nei_data.append(data, sort=False)

            elif year == '2020':

                for f in os.listdir(os.path.join(self._nei_folder_path,str(year))):

                    if '.csv' in f:
        
                        if f == 'point_unknown_2020.csv':
                            continue

                        else:

                            data = pd.read_csv(
                                    os.path.join(
                                        os.path.join(self._nei_folder_path,str(year)), f
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
                                'region': 'epa_region_code',
                                'primary_naics_code': 'naics_code'
                                }, inplace=True)
    
                    nei_data = nei_data.append(data, sort=False)
    
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

        if self._webfires_data_path.exists():

            logging.info('Reading WebFire data from csv')

            webfr = pd.read_csv(self._webfires_data_path, low_memory=False)

        else:

            logging.info(
                'Downloading WebFire data; writing webfirefactors.csv'
                )
                
            Path.mkdir(self._webfires_data_path.parents[0])

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
        # iden_scc.reset_index(drop=True, inplace=True)

        iden_scc = iden_scc[~iden_scc.scc_level_one.isin(
            ['Miscellaneous Area Sources', 'Mobile Sources','very misc']
            )]

        iden_scc.loc[:, 'SCC'] = iden_scc.SCC.astype('int64')

        # iden_scc.rename(columns={
        #     'unit_type': 'scc_unit_type',
        #     'fuel_type': 'scc_fuel_type'}, inplace=True
        #     )

        iden_scc.rename(columns={
            'unit_type_lv1': 'scc_unit_type_lv1',
            'fuel_type_lv1': 'scc_fuel_type_lv1',
            'unit_type_lv2': 'scc_unit_type_lv2',
            'fuel_type_lv2': 'scc_fuel_type_lv2'}, inplace=True
            )

        return iden_scc
    
    def extract_ghg_emissions(self, nei_data):
        """
        Capture GHG idenemissions (i.e., CO2, CH4, N2O)
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
    
        ghgs = nei_data.query(
            "pollutant_code=='CO2'|pollutant_code=='CH4'|pollutant_code=='N2O'"
            ).copy(deep=True)

        # Appears that facilities that report to GHGRP may include a unique
        # EIS unit ID for their **total facility** GHG emissions reported to the 
        # GHGRP.
        # NOTE There are likely issues with CO2 emissions reported by facilities for the
        # NEI (e.g., several units reporting >1e6 short tons of CO2). Many more units
        # report CO2 emissions above the 25,000 tonne CO2 GHGRP reporting threshold, but
        # aren't listed as GHGRP reporters. It doesn't appear that there is a way
        # to systematically correct these emissions. 
        ghgs = ghgs.query("process_description!='epaghg facility reported emissions'").copy(deep=True)

        # convert short tons to metric tonnes
        # Method currently only works if all units of emissions are in short tons
        if (len(ghgs.emissions_uom.unique())==1) & (ghgs.emissions_uom.unique()[0]=='TON') is True:
    
            ghgs.loc[:, 'ghgsTonneCO2e'] = ghgs.apply(
                lambda x: x.total_emissions * self._gwp['100'][x.pollutant_code] * 0.907,
                axis=1
                )
            
        else:
            raise IndexError("Reported emissions have additional units of measurement")

        # aggregated to facility, unit, and process and fuel type
        ghgs = ghgs.groupby(
            ['eis_facility_id', 'eis_unit_id', 'unit_type_final', 'fuel_type'],
            as_index=False
            ).ghgsTonneCO2e.sum()
        
        # Drop values that are >25,000 metric tons. If these values are not
        # errors, then they will be picked up by the inclusion of GHGRP unit emissions
        ghgs = ghgs.query("ghgsTonneCO2e < 25000").copy(deep=True)

        # Make ghg columns consistent with energy columns. Because there is only one
        # reported value, these are all equal.
        ghgs.loc[:,  ['ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']] = \
            np.tile(ghgs.ghgsTonneCO2e.values, (3, 1)).T
        
        ghgs.drop(['ghgsTonneCO2e'], axis=1, inplace=True)
        
        return ghgs


    def merge_fill_ghg_emissions(self, ghgs, nei_data):
        """
        Not all NEI facilities report GHG emissions from fuel combustion. 
        Calculate these missing values using EPA default emissions factors.

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
            on=['eis_facility_id', 'eis_unit_id', 'unit_type_final', 'fuel_type'],
            how='left'
            )

        efs = pd.concat(
            [nei_data.fuel_type.dropna(), 
             nei_data.fuel_type.dropna().map(self._unit_conv['energy_units'])],
            axis=1, ignore_index=True
            )
        
        efs.iloc[:, 1].update(efs.iloc[:, 1].dropna().apply(lambda x: x['MJ_to_KGCO2e']))
        
        efs = dict(efs.drop_duplicates().values)

        nei_data.loc[:, 'ef'] = nei_data.fuel_type.map(efs)

        emissions = \
            nei_data[nei_data.ghgsTonneCO2eQ2.isnull()][['energy_MJ_q0','energy_MJ_q2', 'energy_MJ_q3']].multiply(nei_data.ef, axis=0)/1000

        emissions.columns = ['ghgsTonneCO2eQ0', 'ghgsTonneCO2eQ2', 'ghgsTonneCO2eQ3']

        nei_data.update(emissions)

        nei_data.drop('ef', axis=1, inplace=True)
    
        return nei_data

    def estimate_webfr_median(self, webfr):
        """
        NEI-repored emissions factors may overestimate energy values.
        Estimate median emissions factor by pollutant, material, and unit
        calculated from WebFires data. 

        Parameters
        ----------
        webfr: pandas.DataFrame
            Webfires Emissions Factors.

        Returns
        -------
        med_ef : pandas.DataFrame
            Median WebFires emission factors by NEI_POLLUTANT_CODE,
            MATERIAL, UNIT (numerator), and MEASURE (denominator).
        
        """

        med_ef = webfr[webfr.FACTOR != 'FORMULA'].copy(deep=True)

        efs = med_ef.drop_duplicates('FACTOR').copy(deep=True)

        efs.loc[:, 'FACTOR_float'] = np.nan

        for i, f in efs.FACTOR.iteritems():

            try:
                ef = float(f)

            except ValueError:
                ef = np.nan

            efs.loc[i, 'FACTOR_float'] = ef

        med_ef = pd.merge(med_ef, efs[['FACTOR', 'FACTOR_float']], on='FACTOR', how='left')
        med_ef = med_ef.where(med_ef.ACTION.isin(
            ['Burned', 'Combusted', 'Processed', 'Input', 'Throughput', 'Used', 'Applied',
             'Consumed', 'Produced', 'Charged', 'Fed', 'Operating', 'Generated', 'Dried', 'Baked',
             'Circulated']
             )).dropna(how='all')
        
        # Not all of the MEASURE values in WebFires match those used by NEI.
        webfr_nei = {
            '1000 Gallons': 'E3GAL',
            'Lb': 'LB',
            '1000 Barrels': 'E3BBL',
            '1000 Cubic Feet': 'E3FT3',
            '1000 Horsepower-Hours': 'E3HP-HR',
            '1000 Pounds': 'E3LB',
            'MMBTU': 'E6BTU',
            'MMBtu': 'E6BTU',
            'Million Gallons': 'E6GAL',
            'Million Standard Cubic Feet': 'E6FT3',
            'Pounds': 'LB'
            }
        
        med_ef.MEASURE.update(
            med_ef.MEASURE.map(webfr_nei)
        )

        med_ef.UNIT.update(med_ef.UNIT.apply(lambda x: x.upper()))
        med_ef.MEASURE.update(med_ef.MEASURE.apply(lambda x: x.upper()))
        med_ef.MATERIAL.update(med_ef.MATERIAL.apply(lambda x: x.lower()))

        # Convert all mass units to pounds (LB))
        in_lbs = med_ef.UNIT.apply(lambda x: f'{x}_to_LB').map(
            self._unit_conv['basic_units']
            ) * med_ef.FACTOR_float
    
        in_lbs.dropna(inplace=True)
        
        med_ef.loc[in_lbs.index, 'UNIT'] = 'LB'
        med_ef.FACTOR_float.update(in_lbs)

        med_ef = pd.DataFrame(med_ef.groupby(
            ['NEI_POLLUTANT_CODE', 'MATERIAL', 'UNIT', 'MEASURE'],
            as_index=False
            ).FACTOR_float.median())

        return med_ef
    
    def apply_median_webfr_ef(self, nei_data, webfr, cutoff=0.75):
        """
        NEI-repored emissions factors may overestimate energy values.
        This method provides a second estimate based on the median emissions
        factor calculated from WebFires data.

        Parameters
        ----------
        nei_data : pandas.DataFrame
            NEI data after energy and throughput have been estimated.

        webfr : pandas.DataFrame
            Webfires Emissions Factors.

        cutoff : float; default=0.75
            Ratio of NEI emission factor to WebFires emission factor median value, 
            under which the NEI emission factor is not used for energy 
            calculations.

        Returns
        -------
        nei_data : pandas.DataFrame
            NEI data with updated energy estimates. 
        """

        # Calculate medians of WebFires emission factors
        med_ef = self.estimate_webfr_median(webfr)

        # Find relevant eis_units by 
        # nei_data[(nei_data.energy_MJ_nei > 1E10) & (nei_data.energy_MJ_web.isnull())]

        nei_data = pd.merge(
            nei_data,
            med_ef,
            left_on=['pollutant_code', 'scc_fuel_type', 'ef_numerator_uom',
                        'ef_denominator_uom'],
            right_on=['NEI_POLLUTANT_CODE', 'MATERIAL', 'UNIT', 'MEASURE'],
            how='left',
            suffixes=['', '_webfr']
            )
    
        nei_data.loc[:, 'cutoff_check'] = nei_data.emission_factor.divide(
            nei_data.nei_ef_num_fac
            ).divide(nei_data.FACTOR_float)
        
        nei_data.cutoff_check.update(
            nei_data.cutoff_check.dropna() < cutoff
            )

        # Only concerned with entries where the energy estimated from the original NEI EF is 
        # more than two times the energy estimated with the WebFires EF.
        check_items_index = nei_data[
            (nei_data.cutoff_check == True) & (nei_data.energy_MJ_nei / nei_data.energy_MJ_web > 2)
            ].index

        nei_data.loc[:, 'energy_MJ_webfr_med'] = nei_data.loc[check_items_index, :].apply(
            lambda x: x.total_emissions / x.FACTOR_float * self._unit_conv['energy_units'][x.fuel_type][f'{x.MEASURE_webfr}_to_MJ'], 
            axis=1
            )

        return nei_data

    def calc_and_apply_iqr(self, df):
        """
        Calculate and apply the interquartile range (IQR) 
        as an outlier indicator.

        Parameters
        ----------
        df : pd.DataFrame

        Returns
        -------
        df : pd.DataFrame
        """

        q3 = np.quantile(df.emission_factor, 0.75)
        med = np.quantile(df.emission_factor, 0.50)
        q1 = np.quantile(df.emission_factor, 0.25)

        iqr = q3 - q1

        upper = q3 + 1.5 * iqr
        lower = q1 - 1.5 * iqr

        # Doesn't make sense to have a negative lower bound. Use mean - 2* std dev instead
        if lower < 0:
            lower = np.mean(df.emission_factor) - 2 * np.std(df.emission_factor)

        df.loc[:, 'masked'] = [(x > upper) | (x < lower) for x in df.emission_factor.values]

        df.loc[:, 'emission_factor_median'] = np.nan

        if any(df.masked.values):

            df.loc[df[df.masked == True].index, 'emission_factor_median'] = med

        df.drop(['masked'], axis=1, inplace=True)

        df.dropna(subset=['emission_factor_median'], inplace=True)  # contains original index in multi-index
    
        return df
    
    def detect_and_fix_ef_outliers(self, nei_data):
        """
        Finds emission factors (EFs) that are 1.5 * interquartile range 
        beyond the first and third quartiles by SCC, pollutant code, 
        and fuel type.
        EFs that are identiied as outliers are augemented with the 
        median value.

        Parameters
        ----------
        nei_data : pd.DataFrame

        Returns
        -------
        nei_data : pd.DataFrame
            NEI data with new column 'emission_factor_median' that contains the median
            of EFs found to be outliers. These EFs have the same numerator and
            denominator units and the orginally reported EFs.
        """
    
        # nei_ef = nei_data.groupby(
        #     ['scc', 'fuel_type', 'pollutant_code', 'ef_numerator_uom', 'ef_denominator_uom']
        #     )
        
        nei_ef = nei_data[
            (nei_data.emission_factor.notnull()) & (nei_data.fuel_type.notnull())
            ].groupby(
                ['scc', 'fuel_type', 'pollutant_code', 'ef_numerator_uom', 'ef_denominator_uom']
            )

        outliers = nei_ef.apply(lambda x: self.calc_and_apply_iqr(x))

        outliers.reset_index(level=[0, 1, 2, 3, 4], drop=True, inplace=True)
   
        nei_data.loc[:, 'emission_factor_median'] = outliers.emission_factor_median

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

        # use only NEI emissions of PM, CO, NOX, SOX, VOC, or CH4
        nei_emiss = nei_data[
            nei_data.pollutant_code.str.contains('PM|CO2|CO|NOX|NO3|SO2|VOC|CH4')
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
    
    #TODO refactor 
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

            if (series['scc_unit_type_std'] == 'Other'):

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
            iden_scc[['SCC', 'scc_unit_type_lv1', 'scc_unit_type_lv2', 
                      'scc_fuel_type_lv1', 'scc_fuel_type_lv2']],
            left_on='scc',
            right_on='SCC',
            how='left'
            )
        
        nei.rename(columns={'unit_type': 'nei_unit_type'}, inplace=True)
        
        # Also look for unit types in unit_description
        nei.loc[:, 'desc_unit_type_std'] = nei.unit_description.dropna().apply(
            lambda x: self.unit_regex(x)
            )

        # Remove non-combustion, non-electricity unit types (e.g., storage tanks)
        nei = self.remove_unit_types(nei)
        
        # unit_desc types are already "standardized." Do same for nei.
        # TODO the unit types already in the NEI should be "standardized" (i.e., have a level 1 unit type applied).
        # There also needs to be a level 2 unit type introduced based on the level 1 unit type. 
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
        
        nei.loc[:, 'nei_ef_median_LB_per_TON'] = \
            nei['emission_factor_median'] * nei['nei_ef_num_fac'] / nei['nei_ef_denom_fac']
        
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
        nei.loc[(nei.fuel_type.isnull()) & (nei.pollutant_desc=='Carbon Dioxide') &
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

        nei.loc[:, 'nei_ef_median_LB_per_MJ'] = \
            nei['emission_factor_median']*nei['nei_ef_num_fac']/nei['nei_denom_fuel_fac']

        # WebFire----------------------------------------------
        nei.loc[:, 'UNIT'] = nei['UNIT'].str.upper()

        nei.loc[:, 'FACTOR'] = pd.to_numeric(nei['FACTOR'], errors='coerce')

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
        nei.loc[(nei.fuel_type.isnull()) & (nei.pollutant_desc=='Carbon Dioxide') &
                ((nei.MEASURE == 'E6BTU') |
                (nei.MEASURE == 'HP-HR') |
                (nei.MEASURE == 'THERM') |
                (nei.MEASURE == 'E6FT3')), 'web_denom_fuel_fac'] = \
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
            NEI data with converted emissions units of measurement.

        Returns
        -------
        nei : pandas.DataFrame
            NEI data with estimates of throughput and energy.

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

                if f == 'nei':
                    
                    for m in ['', 'median_']:

                        nei.loc[:, f'{v}_{m}{f}'] = nei.total_emissions_LB.divide(
                            nei[f'{f}_ef_{m}LB_per_{v.split("_")[1]}']
                            )
                        
                else:
                    nei.loc[:, f'{v}_{f}'] = nei.total_emissions_LB.divide(
                            nei[f'{f}_ef_LB_per_{v.split("_")[1]}']
                            )

            # remove throughput_TON if WebFire ACTION is listed as Burned
            nei.loc[(~nei[f'throughput_TON_{f}'].isnull()) & 
                (nei['ACTION'] == 'Burned'), f'throughput_TON_{f}'] = np.nan

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

        # Use energy values calculated after replacing outlier emission factors 
        # with median value.
        ef_outliers = nei.query(
            "energy_MJ_median_nei.notnull() & energy_MJ_nei.notnull()"
            ).copy(deep=True)
        
        # Update original values to values calculated with median
        nei.energy_MJ_nei.update(ef_outliers.energy_MJ_median_nei) 

        # Also use energy values calculated using the median WebFires
        med_ef = nei.query(
            "energy_MJ_median_nei.isnull() & energy_MJ_nei.notnull() & energy_MJ_webfr_med.notnull()"
            ).copy(deep=True)
        
        nei.energy_MJ_nei.update(med_ef.energy_MJ_webfr_med)

        nei.to_csv('nei_check_med_update.csv')

        med_unit = pd.concat(
            [pd.melt(
                nei[['eis_facility_id',
                    #  'eis_process_id',
                     'eis_unit_id',
                     'unit_type_final',
                     'fuel_type',
                     f'{v}_nei',
                     f'{v}_web']],
                id_vars=['eis_facility_id',
                        #  'eis_process_id',
                         'eis_unit_id',
                         'unit_type_final',
                         'fuel_type'],
                value_vars=[f'{v}_nei',
                            f'{v}_web'],
                var_name='EF_source',
                value_name=f'{v}'
                ) for v in ['energy_MJ', 'throughput_TON']], axis=0, sort=True
            )

        # Groupby was not including entries that were missing a unit type or fuel type.
        med_unit.fillna({'fuel_type': 'unknown', 'unit_type_final': 'unknown'}, inplace=True)

        med_unit = med_unit.query(
                "throughput_TON > 0 | energy_MJ > 0"
                ).groupby(
                    ['eis_facility_id',
                    #  'eis_process_id',
                     'eis_unit_id',
                     'unit_type_final',
                     'fuel_type']
                    )[['throughput_TON', 'energy_MJ']].quantile([0, 0.5, 0.75])

        med_unit.reset_index(inplace=True)
        med_unit.level_4.replace({0: 'q0', 0.5: 'q2', 0.75: 'q3'}, 
                                 inplace=True)
        med_unit = med_unit.pivot_table(
            index=['eis_facility_id',
                #    'eis_process_id',
                   'eis_unit_id',
                   'unit_type_final',
                   'fuel_type'],
            columns='level_4', values=['energy_MJ', 'throughput_TON'])

        m = med_unit.columns.map('_'.join)
        med_unit.columns = m  

        other_cols = nei.columns
        other_cols = set(other_cols).difference(set(med_unit.columns))

        other = nei.drop_duplicates(
            ['eis_facility_id', 
            #  'eis_process_id',
             'eis_unit_id', 'unit_type_final',
             'fuel_type']
            )[other_cols]

        med_unit = med_unit.join(
            other.set_index(
                ['eis_facility_id', 
                #  'eis_process_id',
                 'eis_unit_id', 'unit_type_final',
                 'fuel_type']
                )
            )

        med_unit.reset_index(inplace=True)

        # Remove duplicate eis_unit_ids (a sinlge eis_unit_id may have multiple fuel types)

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
            missing_zero.unit_type_final.notnull()
            ).dropna(how='all')

        missing_na = nei.query(
            "throughput_TON_nei.isna() & energy_MJ_nei.isna() & throughput_TON_web.isna() & energy_MJ_web.isna()",
            engine='python'
            )

        missing_na = missing_na.where(
            missing_na.unit_type_final.notnull()
            ).dropna(how='all')

        missing = pd.concat(
            [missing_zero, missing_na], axis=0,
            ignore_index=False
            )

        missing.drop_duplicates(
            ['eis_facility_id', 'eis_unit_id', 
            #  'eis_process_id', 
             'fuel_type'],
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

        with open(Path(self._FIEDPATH, "tools/type_standardization.yml"),'r') as file:
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

        df = df.where(~df.nei_unit_type.isin(remove)).dropna(how='all')

        df.reset_index(inplace=True, drop=True)

        return df

    #TODO make tools method
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

        matched_fuels = pd.DataFrame(
            index=ghgrp_unit_data.index(), 
            columns=['fuelTypeLv1', 'fuelTypeLv2']
            )

        for r, v in ghgrp_unit_data[fuel_type_column].iterrows():
            matched_fuels.loc[r, ['fuelTypeLv1', 'fuelTypeLv2']] = self.match_fuel_type(v)

        # fuel_dict = self.load_fueltype_dict()

        # ghgrp_unit_data.loc[:, 'fuelTypeStd'] = ghgrp_unit_data[fuel_type_column].map(fuel_dict)

        ghgrp_unit_data = ghgrp_unit_data.join(matched_fuels)
        # drop any fuelTypes that are null
        # ghgrp_unit_data = ghgrp_unit_data.where(
        #     ghgrp_unit_data[fuel_type_column] != 'None'
        #     ).dropna(how='all')

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
            'unit_type_final': 'unitType',
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
            'eis_facility_id', 
            'eis_unit_id', 
            'SCC',
            'unit_type_final', 
            'unit_description', 
            'design_capacity',
            'design_capacity_uom', 
            'fuel_type', 
            'eis_process_id',
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
            ['eis_facility_id', 
            #  'eis_process_id', 
             'eis_unit_id',
             'fuel_type'],
            inplace=True
            )

        missing_unit.set_index(
            ['eis_facility_id', 
            #  'eis_process_id', 
             'eis_unit_id',
             'fuel_type'],
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
        #initialize year argument
        nei_data = nei.load_nei_data(year=str(2020))
        iden_scc = nei.load_scc_unittypes()
        webfr = nei.load_webfires()
        logging.info("Merging WebFires data...")
        nei_char = nei.match_webfire_to_nei(nei_data, webfr)
        logging.info("Merging SCC data...")
        logging.info("Assigning unit and fuel types...")
        nei_char = nei.assign_types(nei_char, iden_scc)
        # nei_char = nei.remove_unit_types(nei_char)  # remove some non-combustion related unit types
        logging.info("Finding emission factor outliers...")
        nei_char = nei.detect_and_fix_ef_outliers(nei_char)
        logging.info("Converting emissions units...")
        nei_char = nei.convert_emissions_units(nei_char)
        logging.info("Estimating throughput and energy...")
        nei_char = nei.calc_unit_throughput_and_energy(nei_char)
        # Use median EF from WebFires as alt approach to estimating energy
        nei_char = nei.apply_median_webfr_ef(nei_char, webfr, cutoff=0.75)  
        logging.info("Extracting and aggregating GHG emissions")
        logging.info("Final NEI data assembly...")
        med_unit = nei.get_median_throughput_and_energy(nei_char)
        missing_unit = nei.separate_missing_units(nei_char)

        nei_char = nei.merge_med_missing(med_unit, missing_unit)
        ghgs = nei.extract_ghg_emissions(nei_char)

        logging.info("Merging and filling GHG emissions")
        nei_char = nei.merge_fill_ghg_emissions(ghgs, nei_char)
        nei_char = nei.format_nei_char(nei_char)
        nei_char = nei.find_missing_cap(nei_char)  # Fill in missing capacity data, where possible
        nei_char = nei.convert_capacity(nei_char)  # Convert energy capacities all to MW
        nei_char = nei.check_estimates(nei_char)  # check estimates 

        return nei_char


if __name__ == '__main__':

    nei_char = NEI().main()
    # NEI().main()
    nei_char.to_csv('formatted_estimated_nei_updated.csv')
