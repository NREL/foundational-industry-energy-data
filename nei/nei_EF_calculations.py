import pandas as pd
import numpy as np
import os
import readline
import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import requests
import zipfile
from io import BytesIO


class NEI:
    """

    Calculates unit throughput and energy input (later op hours?) from
    emissions and emissions factors, specifically from: PM, SO2, NOX, 
    VOCs, and CO.

    Uses NEI Emissions Factors (EFs) and, if not listed, WebFire EFs

    Returns file: 'NEI_unit_throughput_and_energy.csv'

    """

    def __init__(self):

        self._nei_data_path = os.path.abspath('./data/NEI/nei_ind_data.csv')
        self._webfires_data_path = \
            os.path.abspath('./data/WebFire/webfirefactors.csv')

        self._unit_conv_path = os.path.abspath('./nei/unit_conversions.yml')

        with open(self._unit_conv_path) as file:
            self._unit_conv = yaml.load(file, Loader=yaml.FullLoader)

        self._scc_units_path = os.path.abspath('./scc/iden_scc.csv')

        logging.basicConfig(level=logging.INFO)

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

        """

        if os.path.exists(self._nei_data_path):

            logging.info('Reading NEI data from csv')
            # nei_data_dd = dd.read_csv(nei_data_path, dtype={'tribal name': str})
            nei_data = pd.read_csv(self._nei_data_path, low_memory=False)

        else:

            logging.info(
                'Reading NEI data from zipfiles; writing nei_ind_data.csv'
                )

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

            nei_data.to_csv(self._nei_data_path)

        return nei_data

    def load_webfires(self):
        """
        Load all EPA WebFire emissions factors, downloading from
        https://www.epa.gov/electronic-reporting-air-emissions/webfire
        if necessary. 
        """

        if os.path.exists(self._webfires_data_path):

            logging.info('Reading WebFire data from csv')
            # nei_data_dd = dd.read_csv(nei_data_path, dtype={'tribal name': str})
            webfr = pd.read_csv(self._webfires_data_path)

        else:

            logging.info(
                'Downloading WebFire data; writing webfirefactors.csv'
                )

            r = requests.get(
                'https://cfpub.epa.gov/webfire/download/webfirefactors.zip'
                )

            with zipfile.ZipFile(BytesIO(r.content)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    webfr = pd.read_csv(f)
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

    def match_webfire_to_nei(self, nei_data, webfr):
        """
        Match WebFire EF data to NEI data

        Parameters
        ----------
        nei_data : pandas.DataFrame

        webfr : pandas.DataFrame

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

        nei_emiss.rename(columns={'SCC':'SCC_web'},inplace=True)

        return nei_emiss

    def nei_assign_types(self, nei_emiss, iden_scc):
        """
        Assign unit type and fuel type based on NEI and SCC descriptions

        Paramters
        ---------


        Returns
        -------
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

        # get fuel types from NEI text and SCC descriptions
        nei.unit_description = nei.unit_description.str.lower()
        nei.process_description = nei.process_description.str.lower()
        nei.scc_fuel_type = nei.scc_fuel_type.str.lower()

        for f in self._unit_conv['fuel_dict'].keys():

            # search for fuel types listed in NEI unit/process descriptions
            nei.loc[(nei['unit_description'].str.contains(f, na=False)) |
                    (nei['process_description'].str.contains(f, na=False)),
                     'fuel_type'] = self._unit_conv['fuel_dict'][f]

            # search for the same fuel types listed in SCC
            nei.loc[(nei['fuel_type'].isnull()) &
                    (nei['scc_fuel_type'].str.contains(f, na=False)),
                    'fuel_type'] = self._unit_conv['fuel_dict'][f]

        return nei


    # for throughput calculation,
    #   convert NEI emissions to LB; NEI EFs to LB/TON;
    #   and WebFire EFs to LB/TON

    # for energy input calculation,
    #   convert NEI emissions to LB; NEI EFs to LB/MJ; 
    #   and WebFire EFs to LB/MJ

    def convert_emissions_units(self, nei, uc):
        """
        

        Parameters
        ----------


        Returns
        -------

        """

        # map unit of emissions and EFs in NEI/WebFire to unit conversion key

        # NEI--------------------------------------------------
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
            nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_ef_denom_fac']

        # convert NEI emission_factor to LB/MJ for energy input
        for f in nei.fuel_type.dropna().unique():

            nei.loc[nei.fuel_type == f, 'nei_denom_fuel_fac'] = \
                nei['ef_denominator_uom'].map(
                    self._unit_conv['unit_to_mj']).map(
                        self._unit_conv['energy_units'][f]
                        )

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

        nei.loc[:,'nei_ef_LB_per_MJ'] = \
            nei['emission_factor']*nei['nei_ef_num_fac']/nei['nei_denom_fuel_fac']

        # WebFire----------------------------------------------
        nei['UNIT'] = nei['UNIT'].str.upper()

        nei['FACTOR'] = pd.to_numeric(nei['FACTOR'], errors='coerce')

        nei.replace({'MEASURE': self._unit_conv['measure_dict']}, inplace=True)

        # convert WebFire EF numerator to LB
        nei.loc[:,'web_ef_num_fac'] = nei['UNIT'].map(
            self._unit_conv['unit_to_lb']
            ).map(self._unit_conv['basic_units'])

        # convert WebFire EF to LB/TON for throughput
        nei.loc[:,'web_ef_denom_fac'] = nei['MEASURE'].map(
            self._unit_conv['unit_to_ton']
            ).map(self._unit_conv['basic_units'])

        nei.loc[:,'web_ef_LB_per_TON'] = \
            nei['FACTOR']*nei['web_ef_num_fac']/nei['web_ef_denom_fac']

        # convert WebFire EF to LB/E6BTU for energy input
        for f in nei.fuel_type.dropna().unique():

            nei.loc[nei.fuel_type==f, 'web_denom_fuel_fac'] = nei['MEASURE'].map(
                self._unit_conv['unit_to_mj']
                ).map(self._unit_conv['energy_units'][f])

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

        nei.loc[:,'web_ef_LB_per_MJ'] = \
            nei['FACTOR']*nei['web_ef_num_fac']/nei['web_denom_fuel_fac']

        return nei

    def calculate_unit_throughput_and_energy(self, nei):
        """
        Calculate throughput quantity in TON and energy input in MJ
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

        # if there is an NEI EF, use NEI EF  
        nei.loc[(nei['nei_ef_LB_per_TON'] > 0), 'throughput_TON'] = \
            nei['total_emissions_LB'] / nei['nei_ef_LB_per_TON']

        nei.loc[(nei['nei_ef_LB_per_MJ'] > 0), 'energy_MJ'] = \
            nei['total_emissions_LB'] / nei['nei_ef_LB_per_MJ']

        # if there is not an NEI EF, use WebFire EF
        nei.loc[(nei['nei_ef_LB_per_TON'].isnull()) & 
                (nei['web_ef_LB_per_TON'] > 0), 'throughput_TON'] = \
            nei['total_emissions_LB'] / nei['web_ef_LB_per_TON'] 

        nei.loc[(nei['nei_ef_LB_per_MJ'].isnull()) & 
                (nei['web_ef_LB_per_MJ'] > 0), 'energy_MJ'] = \
            nei['total_emissions_LB'] / nei['web_ef_LB_per_MJ']

        # remove throughput_TON if WebFire ACTION is listed as Burned
        nei.loc[(~nei['throughput_TON'].isnull()) & 
                (nei['ACTION']=='Burned'),'throughput_TON'] = np.nan

        return nei

    @staticmethod
    def plot_throughput_difference(nei):
        """
        Plot difference between max and min throughput_TON quanitites for unit
        when there are multiple emissions per unit
        """

        duplic = \
            nei[(nei.throughput_TON>0) &
                (nei.eis_process_id.duplicated(keep=False) == True)].groupby(
                    ['eis_process_id']).agg(
                        perc_diff=('throughput_TON',
                                lambda x: ((x.max()-x.min())/x.mean())*100)
                        ).reset_index()

        plt.rcParams['figure.dpi'] = 300
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['font.sans-serif'] = "Arial"                     

        sns.histplot(data=duplic, x="perc_diff")
        plt.xlabel('Percentage difference')
        plt.ylabel('Units')

        return


    @staticmethod
    def plot_energy_difference(nei):
        """
        Plot difference between max and min energy_MJ quanitites for unit
        when there are multiple emissions per unit
        """

        duplic = \
            nei[(nei.energy_MJ>0) &
                (nei.eis_process_id.duplicated(keep=False)==True)].groupby(
                    ['eis_process_id']).agg(
                        perc_diff=('energy_MJ',
                                lambda x: ((x.max()-x.min())/x.mean())*100)
                        ).reset_index()
        
        plt.rcParams['figure.dpi'] = 300
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['font.sans-serif'] = "Arial"                     
    
        sns.histplot(data=duplic, x="perc_diff") # sns.kdeplot
        plt.xlabel('Percentage difference')
        plt.ylabel('Units')
        
        return



# use the median throughput_TON and energy_MJ for individual unit 
#   when there are multiple emissions per unit
def get_median_throughput_and_energy(nei):
    
    
    med_unit = nei[(nei['throughput_TON']>0)|(nei['energy_MJ']>0)
                   ].groupby(['eis_process_id',
                              'eis_facility_id',
                              'naics_code',
                              'naics_sub',
                              'unit_type',
                              'fuel_type'
                              ],dropna=False
                              )[['throughput_TON',
                                 'energy_MJ']].median().reset_index()
    
                                 
    # MATERIAL and ACTION columns can have multiple values for same
    #   eis_process_id so if included in groupby, need to remove duplicates
                            
    #med_unit.drop(med_unit[
    #    (med_unit.eis_process_id.duplicated(keep=False)==True) & 
    #    (med_unit.MATERIAL.isnull())].index, inplace=True)
    
    
    med_unit.to_csv('NEI_unit_throughput_and_energy.csv',index=False)
    
    
    return
    



nei_emiss = match_webfire_to_nei(nei_emiss, webfr)
nei_emiss = get_unit_and_fuel_type(nei_emiss, iden_scc)
convert_emissions_units(nei_emiss, unit_conv)
calculate_unit_throughput_and_energy(nei_emiss)
get_median_throughput_and_energy(nei_emiss)





    # check the difference between max and min throughput for single unit
    #nei_emiss[(nei_emiss.throughput_TON>0) & 
    #          (nei_emiss.eis_process_id.duplicated(keep=False)==True)].groupby(
    #              ['eis_unit_id','eis_process_id']
    #              )['throughput_TON'].agg(np.ptp).describe()
    
    # compare units that had throughput calculated vs all units
    #(nei_emiss[nei_emiss.throughput_TON>0].groupby(
    #    ['naics_sub'])['eis_process_id'].count()/nei_emiss.groupby(
    #        ['naics_sub'])['eis_process_id'].count())




    