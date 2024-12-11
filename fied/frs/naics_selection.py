
import pandas as pd
import numpy as np


class NAICS_Identification:

    def id_additional_naics(self, data, naics_split, all_naics_id):
        """
        Captures any additional NAICS codes that were assigned to a registry ID.

        Parameters
        ----------
        data : pandas.DataFrame
            Raw FRS data contained in "NATIONAL_NAICS_FILE.CSV", downloaded from 
            the `FRS combined national files <https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip>`_.

        all_naics_id : pandas.Series
            Series of all registry IDs with assigned NAICS codes.

        Returns
        all_naics_id : pandas.Series
            DataFrame with additional assigned NAICS codes.
        """
        
        dup_ids = np.hstack([
            naics_split['multi'].index.get_level_values('REGISTRY_ID').drop_duplicates(),
            naics_split['no_max'].index.get_level_values('REGISTRY_ID').drop_duplicates()
            ])

        dups = data.set_index('REGISTRY_ID').loc[dup_ids, :]

        dups = dups.groupby('REGISTRY_ID', level=0).apply(
            lambda x: list(x['NAICS_CODE'].unique())
            )
        
        dups.name = 'NAICS_CODE_additional'

        all_naics_id = pd.DataFrame(all_naics_id).join(dups)

        add_naics_clean = all_naics_id.dropna().apply(
            lambda x: [v for v in x['NAICS_CODE_additional'] if v not in [x['NAICS_CODE']]],
            axis=1
            )
        
        add_naics_clean[add_naics_clean.str[0].isnull()] = np.nan

        all_naics_id.loc[add_naics_clean.index, 'NAICS_CODE_additional'] = add_naics_clean

        return all_naics_id
        

    def split_naics_count(self, data):
        """
        Splits FRS registry IDs into those with and without more than
        one NAICS code. For registry IDs with multiple NAICS codes, differentiates between 
        those registry IDs that have the same NAICS assigned from more than one different
        reporting program and those that don't. 

        Parameters
        ----------
        data : pandas.DataFrame
            Raw FRS data contained in "NATIONAL_NAICS_FILE.CSV", downloaded from 
            the `FRS combined national files <https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip>`_.

        Returns
        -------
        naics_split : dict
            Dictionary of DataFrames or Series, indexed with registry IDs. The following are
            the dictionary keys and the descriptions of the DataFrames or Series they contain.

            'multi' : pandas.DataFrame
                DataFrame containing registry IDs with more than one NAICS code assigned to them. Of
                those NAICS codes, more than one EPA program has assigned the same NAICS code.
            'no_max': pandas.Series
                Series containing registry IDs with more than one NAICS code assigned to them. Unlike 
                the registry IDs in 'multi', the same NAICS code has not been assigned by more than
                one EPA program.
            'single': pandas.DataFrame
                DataFrame containing the registry IDs with a single NAICS code assigned to them.
        """

        nc = data.groupby(
            ['REGISTRY_ID', 'NAICS_CODE']
            )
        
        naics_split = {}

        nc_counts = nc.PGM_SYS_ACRNM.count()

        naics_split['multi'] = nc_counts.loc[
            (nc_counts.sum(level=0)[nc_counts.sum(level=0) > 1].index)
            ]
        
        # Also need to address instances where there are multiple NAICS, but the count is no > 1
        naics_split['no_max'] = naics_split['multi'].max(level=0)
        naics_split['no_max'] = naics_split['no_max'].where(naics_split['no_max']==1).dropna()

        naics_split['multi'] = naics_split['multi'].reset_index()

        naics_split['multi'] = naics_split['multi'][
            ~naics_split['multi'].REGISTRY_ID.isin(naics_split['no_max'].index)
            ]    

        naics_split['multi'] =  naics_split['multi'].set_index(['REGISTRY_ID', 'NAICS_CODE'])
                                                            
        naics_split['single'] = nc_counts.loc[
            (nc_counts.sum(level=0)[nc_counts.sum(level=0) == 1]).index
            ]
        
        naics_split['single'] = naics_split['single'].reset_index('NAICS_CODE').drop(
            ['PGM_SYS_ACRNM'], axis=1
            ) 
        
        naics_split['single'] = naics_split['single']['NAICS_CODE']

        return naics_split


    def find_max_naics(self, naics_split_multi):
        """
        For registry IDs with multiple NAICS codes and at least one NAICS code assigned
        by multiple reporting programs, selects the NAICS code that appears most frequently.

        Parameters
        ----------
        naics_split_multi : pandas.DataFrame
            Processed DataFrame identifying registry IDs with multiple NAICS codes 
            and at least one NAICS code assigned by multiple reporting programs.

        Returns
        -------
        max_naics : pandas.DataFrame 
            Registry ID NAICS code selection based on most frequently assigned NAICS code.
        """

        max_naics = naics_split_multi.reset_index()

        # Sorts the count of each NAICS code (represented here by 'PGM_SYS_ACRNM') in
        # descending order. So, first value is the largest count and represents the 
        # NAICS code most often recorded by the various EPA reporting programs
        max_naics = max_naics.groupby('REGISTRY_ID').apply(
            lambda x: x.sort_values(
                by='PGM_SYS_ACRNM', ascending=False
                ).NAICS_CODE.values[0]
            )
        
        max_naics.name = 'NAICS_CODE'

        return max_naics


    def id_naics_pgm(self, data, naics_split_no_max=None):
        """
        Select NAICS codes based on a preference hierarchy of reporting
        programs, namely E-GGRT (GHGRP) > EIS (NEI) > ICS-AIR > AIRS/AFS > TRIS > CEDRI > EPS > state-level reporting systems > non-air programs.

        See https://www.epa.gov/frs/frs-data-sources for more information on  EPA Program systems.

        This method can be applied to the entire registry ID NAICS file, or separately to 
        the registry IDs that have multiple NAICS codes, but no single NAICS code that 
        has been assigned by more than one reporting program (i.e., the maximum count for any 
        NAICS code is equal to 1).

        Parameters
        ----------
        data : pandas.DataFrame
            Raw FRS data contained in "NATIONAL_NAICS_FILE.CSV", downloaded from 
            the `FRS combined national files <https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip>`_.

        naics_split_no_max : default is None; pandas.DataFrame
            If none, then the NAICS selection method is applied to the entire
            set of registry IDs. Otherwise, the method is applied to the DataFrame of 
            registry IDs specified by this parameter.

        Returns
        -------
        final_naics : pandas.Series
            Registry ID NAICS code selection based on the described reporting program preference.

        """

        pgm_pref = ['E-GGRT', 'EIS', 'AIR', 'AIRS/AFS', 'TRIS', 'CEDRI', 'EPS', 
                    'CAMDBS', 'TX-TCEQ ACR', 'MN-TEMPO', 'AZURITE',
                    'CA-ENVIROVIEW', 'CA-CERS', 'NJ-NJEMS', 'SIMS', 'DEN',
                    'FDM', 'HI-EHW', 'IDNR_EFD', 'FARR', 'ACES', 'IN-TEMPO', 'KS-FP',
                    'MA-EPICS', 'MD-TEMPO', 'ME-EFIS', 'MO-DNR', 'MS-ENSITE',
                    'MT-CEDARS', 'NC-FITS', 'ND-FP', 'FIS', 'NM-TEMPO', 'NV-FP',
                    'CNFRS', 'OR-DEQ', 'PA-EFACTS', 'RI-PLOVER', 'SC-EFIS', 'CEDS',
                    'WA-FSIS', 'WI-ESR', 'WDEQ', 'RCRAINFO', 'ECRM', 'PDS', 'EIA-860','ICIS', 
                    'RMP', 'NPDES', 'OSHA-OIS', 'FFEP']

        # Getting around issue of duplicate REGISTRY_IDs in idex for pivoting
        pgm_naics =  data[['REGISTRY_ID', 'PGM_SYS_ACRNM', 'NAICS_CODE']].copy(deep=True)

        pgm_naics.loc[:, 'key'] = pgm_naics.groupby(['REGISTRY_ID', 'PGM_SYS_ACRNM']).cumcount()

        pgm_naics = pgm_naics.pivot(
            index=['REGISTRY_ID', 'key'], columns='PGM_SYS_ACRNM', values='NAICS_CODE'
            ).reset_index('key', drop=True)

        # pgm_naics = pd.pivot(
        #     data[['REGISTRY_ID', 'PGM_SYS_ACRNM', 'NAICS_CODE']],
        #     index='REGISTRY_ID',
        #     columns='PGM_SYS_ACRNM'
        #     )
        
        # pgm_naics = pgm_naics.droplevel(0, axis=1)

        try:
        
            pgm_naics = pgm_naics.join(naics_split_no_max, how='right')

        except TypeError:
            pass

        final_naics = pd.DataFrame()

        for p in pgm_pref:

            p_data = pgm_naics[p].dropna()

            final_naics = pd.concat([final_naics, p_data], axis=0)
            
        final_naics = final_naics[~final_naics.index.duplicated(keep='first')]

        final_naics.rename(columns={final_naics.columns[0]: 'NAICS_CODE'}, inplace=True)

        final_naics = final_naics['NAICS_CODE']

        return final_naics


    def assign_all_naics(self, data):
        """
        Method applies the individual methods for selecting the appropriate NAICS code
        for regisitries with and without multiple NAICS codes assigned by different EPA
        reporting programs. 
        
        Parameters
        ----------
        data : pandas.DataFrame
            Raw FRS data contained in "NATIONAL_NAICS_FILE.CSV", downloaded from 
            the `FRS combined national files <https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip>`_.

        Returns
        -------
        all_naics_id : pandas.Series
            Series of all registry IDs with assigned NAICS codes.
        """ 

        naics_split = self.split_naics_count(data)

        all_naics_id = pd.concat([
            self.find_max_naics(naics_split['multi']),
            self.id_naics_pgm(data, naics_split['no_max']),
            naics_split['single']
            ], axis=0, 
            ignore_index=False 
            )
        
        all_naics_id = self.id_additional_naics(data, naics_split, all_naics_id)

        all_naics_id.reset_index(inplace=True)

        all_naics_id.rename(columns={'index': 'REGISTRY_ID'}, inplace=True)

        return all_naics_id


# data = pd.read_csv("NATIONAL_NAICS_FILE.CSV", low_memory=False)

# all_naics_id = assign_all_naics(data)
