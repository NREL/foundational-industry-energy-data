
import logging
import os
import json
import zipfile
import pyarrow
import pathlib
import requests
import textwrap
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from io import BytesIO


logging.basicConfig(level=logging.INFO)

class FIED_analysis:

    def __init__(self, year, file_path=None, pio_engine=None, df=None, fig_format='png'):

        if file_path is None:
            self._fied = df

        else:
            try:
                self._fied = pd.read_parquet(
                    file_path
                    )

            except (pyarrow.ArrowIOError, pyarrow.ArrowInvalid):
                self._fied = pd.read_csv(
                    file_path, low_memory=False, index_col=0
                    )

        self._fied = self.make_consistent_naics_column(self._fied, n=2)
        
        self._year = year

        self._fig_path = pathlib.Path('./analysis/figures')

        self._pio_engine = pio_engine

        if self._fig_path.exists():
            pass
    
        else:
            pathlib.Path.mkdir(self._fig_path)

        self._fig_format = fig_format

    def get_cbp_data(self):
        """
        Get establishment counts from Census County Business
        Patterns for reporting year.
        
        Returns
        -------
        cbp_data : pandas.DataFrame
            DataFrame of CBP establishment counts by NAICS
            code for U.S. for specified year.

        """

        cbp_data_url = \
            f'https://www2.census.gov/programs-surveys/cbp/datasets/{self._year}/cbp{str(self._year)[-2:]}us.zip'
        
        r = requests.get(cbp_data_url)

        with zipfile.ZipFile(BytesIO(r.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                cbp_data = pd.read_csv(
                    f, sep=',', 
                    usecols=['lfo', 'naics', 'est']
                    )
                
        cbp_data = cbp_data.query('lfo == "-"').copy(deep=True)

        cbp_data.replace(
            {'-': '', '/':''}, inplace=True, regex=True
            )
    
        cbp_data.loc[:, 'n_naics'] = \
            cbp_data.naics.apply(lambda x: len(str(x)))
    
        return cbp_data

    def create_core_analysis(self, **kwargs):
        """
        Create summary figures and table.
        """

        # summary_table_all = self.summary_unit_table()

        # summary_table_eisghgrp = self.summary_unit_table(eis_or_ghgrp_only=True)

        # self.summary_unit_bar(summary_table_all, write_fig=kwargs['write_fig'])

        # self.plot_stacked_bar_missing(write_fig=kwargs['write_fig'])

        # for ds in ['ghgrp', 'nei']:
        #     self.plot_stacked_bar_missing(data_subset=ds, write_fig=kwargs['write_fig'])

        # self.plot_facility_count(write_fig=kwargs['write_fig'])

        # # self.plot_best_characterized()

        # for u in self._fied.unitTypeStd.unique():
        #     try:
        #         u.title()
        #     except AttributeError:
        #         continue
        #     else:
        #         for m in ['energy', 'power']:
        #             self.plot_unit_bubble_map(u, m, write_fig=kwargs['write_fig'])

        # for v in ['count', 'energy', 'capacity']:
        #     for n in [None, 2, 3]:
        #         self.plot_ut_by_naics(n, v, write_fig=kwargs['write_fig'])

        for ds in ['mecs', 'seds']:
            self.plot_eia_comparison_maps(dataset=ds, year=2017)

        return
    

    def summary_unit_table(self, eis_or_ghgrp_only=False):
        """
        Creates a table that summarizes by industrial sector
        (i.e., 2-digit NAICS) various aspects of the 
        dataset. Saves table to analysis directory and returns table.

        Parameters
        ----------
        final_data : pandas.DataFrame or parquet

        Returns
        -------
        summary_table : pandas.DataFrame

        """

        # Multiple units can share the same eisUnitID.
        # (see evidence in unitDescription field)
        table_data = self._fied.copy(deep=True)

        table_data = self.id_sectors(table_data)

        if eis_or_ghgrp_only:
            table_data = table_data[
                (table_data.eisFacilityID.notnull())|(table_data.ghgrpID.notnull())
                ].copy(deep=True)
            
            fname = './analysis/summary_unit_table_eisghgrp_only.csv'
        else:
            fname = './analysis/summary_unit_table_eisghgrp_only.csv'

        desc = ['count', 'sum', 'mean', 'std', 'min', 'median', 'max']

        _unit_count = table_data.groupby(
            ['sector', 'unitTypeStd']
            ).registryID.count()

        _unit_count.name = 'Count of Units'

        _capacity_summ = table_data.query('designCapacityUOM == "MW"').groupby(
                ['sector', 'unitTypeStd']
                ).designCapacity.agg(desc)
        
        _capacity_summ.columns = ['Capacity_MW_' + c for c in _capacity_summ.columns]

        # Count of facilities with unit information
        _fac_count = table_data.query('unitTypeStd.notnull()', engine='python').groupby(
            ['sector']
            ).registryID.unique().apply(lambda x: len(x))

        _fac_count.name = 'Count of Facilities'

        # Only use non-zero value
        _energy_summ_ = pd.concat(
            [table_data[table_data[c] > 0] for c in ['energyMJ', 'energyMJq0', 'energyMJq2', 'energyMJq3']],
            axis=0, ignore_index=True
            )
        
        def agg_energy(df, type):
            e_agg = df.copy(deep=True)
            e_agg.energyMJ.update(e_agg[f'energyMJ{type[1]}'])
            e_agg = e_agg.groupby(['sector', 'unitTypeStd']).energyMJ.agg(type[0])
            e_agg.name = f'EnergyMJ_{type[0]}'

            return e_agg
        
        _energy_summ_agg = pd.concat(
            [agg_energy(_energy_summ_, t) for t in [
                ['sum', 'q2'], ['mean', 'q2'], ['std', 'q2'], ['min', 'q0'], 
                ['median', 'q2'], ['max', 'q3']]],
            axis=1
            )

        _throughput_summ = pd.concat(
            [
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ2.sum(),
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ2.mean(),
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ2.agg('std'),
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ0.min(),
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ2.median(),
                table_data.groupby(
                    ['sector', 'unitTypeStd']
                    ).throughputTonneQ3.max()
                ],
            axis=1
            )
        
        _throughput_summ.columns = [f'ThroughputTonnes_{c}' for c in ['sum', 'mean', 'std', 'min', 'median', 'max']]

        summary_table = pd.concat(
            [_unit_count, _capacity_summ, _energy_summ_agg, _throughput_summ],
            axis=1,
            )

        summary_table = pd.DataFrame(_fac_count).join(summary_table)

        summary_table.to_csv('./analysis/summary_unit_table.csv')

        return summary_table

    def plot_facility_count(self, write_fig=True):
        """"
        Plots the count of facilities from foundational data
        and the count of establishments from the corresponding
        year of Census Business Patterns (CBP) data.
        
        Parameters
        ----------
        write_fig : bool; default=True
            Write resulting figure to analysis figures
            directory.
        
        """
        ind_map = {
            '11': 'Agriculture', 
            '21': 'Mining',
            '23': 'Construction',
            '31': 'Manufacturing',
            '32': 'Manufacturing',
            '33': 'Manufacturing',
            }

        cbp_data = self.get_cbp_data()

        # Some foundational data reported at 4-digit NAICS level.
        # So, will be aggregating counts at 4-digit level.
        # Note that CBP doesn't track ag establishments for NAICS 111 - 112
        # (essentially all farming, animal production, and aquaculture)
        cbp_data.loc[:, 'ind'] = cbp_data.naics.apply(
            lambda x: (x[0:2] in ['11', '21', '23', '31', '32', '33']) & (len(x) == 4)
            )

        # Total count of industrial establishments
        total_est = cbp_data.where(
            cbp_data.naics.isin(ind_map.keys())
            ).dropna().est.sum()
        
        ind_cbp_data = pd.DataFrame(cbp_data.query("ind == True"))
        ind_cbp_data.loc[:, 'naics'] = ind_cbp_data.naics.astype(int)

        fied_count = self._fied.copy(deep=True)
        fied_count.loc[:, 'n4'] = fied_count.naicsCode.apply(
            lambda x: int(str(int(x))[0:4])
            )

        fied_count = pd.concat(
            [fied_count.groupby('n4').registryID.apply(lambda x: len(x.unique())),
            ind_cbp_data.set_index('naics').est],
            axis=1,
            ignore_index=False
            ).reset_index()

        index_str = fied_count['index'].astype(str)
        fied_count.loc[:, 'index'] = index_str
        fied_count.loc[:, 'sector'] = fied_count['index'].apply(
            lambda x: x[0:2]
            ).map(ind_map)
        
        fied_cumsum = pd.merge(
            fied_count[['index', 'sector']],
            fied_count.groupby(['sector'])[['registryID', 'est']].cumsum(),
            left_index=True, right_index=True
            )

        fig = px.line(
            fied_cumsum.melt(
                id_vars=['index', 'sector']), 
            y="value", x="index", color='variable',
            facet_col='sector', facet_col_wrap=2,
            markers=True,
            labels={
                'index': 'NAICS Code',
                'value': 'Cumulative Count of Establishments<br>or Registry IDs',
                'variable': 'Data Source'
                }
            )

        for n in range(0,8):
            if n < 4:
                fig.data[n].name = 'Foundational Data'

            else:
                fig.data[n].name = 'County Business Patterns'
        
        fig.for_each_annotation(
            lambda a: a.update(text=a.text.split("=")[-1].capitalize())
            )
        
        fig.update_yaxes(matches=None, showticklabels=True,
                         titlefont=dict(size=15), automargin=True)
        fig.update_xaxes(matches=None, showticklabels=True,
                         titlefont=dict(size=15), automargin=True)

        fig.update_layout(
            autosize=True,
            template='presentation',
            height=800,
            width=1200,
            legend=dict(orientation="h", x=0.25, title=""),
            font=dict(size=16)
            )
    
        if write_fig:
            pio.write_image(
                fig,
                file=self._fig_path / f'counts_{self._year}',
                format=self._fig_format,
                engine=self._pio_engine
                )

        else:
            fig.show()

 
    @staticmethod
    def plot_difference_nei(nei, data):
        """
        Plot difference between max and min energy or
        throughput quanitites for units when there are
        multiple emissions per unit.

        Parameters
        ----------
        nei : pandas.DataFrame
            Unformatted NEI, prior to estimating
            quartile values for throughput and energy.

        data : str; 'energy' or 'throughput'

        """

        selection = {
            'energy': {
                'column': 'energy_MJ',
                'title': 'Energy'
                },
            'throughput': {
                'column': 'throughput_TON',
                'title': 'Throughput'
                }
            }

        duplic = nei[(nei[selection[data]['columns']] > 0) &
                        (nei.eis_process_id.duplicated(keep=False) == True)]

        duplic = duplic.groupby(
            ['eis_process_id']).agg(
                perc_diff=(
                    selection[data]['column'],
                    lambda x: ((x.max()-x.min())/x.mean())*100
                    )
                ).reset_index()

        plt.rcParams['figure.dpi'] = 300
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['font.sans-serif'] = "Arial" 

        sns.histplot(data=duplic, x="perc_diff")  # sns.kdeplot
        plt.xlabel('Percentage difference')
        plt.ylabel('Units')
        plt.title(selection[data]['title'])

        return None


    def id_sectors(self, df):
        """
        Make a new sector column for NAICS 2-digit

        Returns
        -------
        df : pandas.DataFrame
            FIED with 2-digit NAICS code column

        """

        if 'n2' in df.columns:
            pass

        else:
            df = self.make_consistent_naics_column(df, n=2)

        sectors = pd.DataFrame(
            [['Agriculture', 11], ['Construction', 21], ['Mining', 23], 
            ['Manufacturing', 31], 
            ['Manufacturing', 32], ['Manufacturing', 33]],
            columns=['sector', 'n2']
            )

        df = pd.merge(df, sectors, on='n2', how='left')

        return df


    def summary_unit_bar(self, summary_table, write_fig=True):
        """
        Make stacked bar chart showing units by Sector and
        total number of facilities reporting units

        Paramters
        ---------
        summary_table : pandas.DataFrame
            Output of `summary_unit_table` method.

        write_fig : bool; default=True
            Write figure to analysis figures directory

        """

        plot_data = summary_table.reset_index()

        len_unit_types = len(plot_data.unitTypeStd.unique())

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        plot_data.sort_values(by=['Count of Units'], ascending=False)

        fig_units = px.bar(
            plot_data,
            x='sector',
            y='Count of Units',
            labels={
                'value': 'Facility Count',
                'variable': 'Facilities'
                },
            template='simple_white',
            color='unitTypeStd',
            color_discrete_sequence=px.colors.qualitative.Alphabet
            # color_discrete_sequence=px.colors.sample_colorscale(
            #     "plasma", [n/(len_unit_types-1) for n in range(len_unit_types)]
            #     )
            )

        for d in range(0, len(fig_units.data)):
            fig.add_trace(fig_units.data[d], secondary_y=False)

        fac_counts = plot_data.drop_duplicates(['sector', 'Count of Facilities'])

        fig.update_layout(barmode='stack')
        fig.add_trace(
            go.Scatter(
                mode='markers',
                x=fac_counts.sector,
                y=fac_counts['Count of Facilities'],
                marker=dict(size=26, color='LightSkyBlue'),
                showlegend=True,
                name='Facilities'
            ), secondary_y=True
            )
        
        fig.update_yaxes(automargin=True, title='Count of Units',
                        secondary_y=False,
                        range=[0, 70000],
                        tickmode='linear',
                        tick0=0,
                        dtick=7000
                        )

        fig.update_yaxes(automargin=True, title='Count of Facilities',
                        secondary_y=True, range=[0, 20000],
                        tickmode='linear',
                        tick0=0,
                        dtick=2000)

        fig.update_xaxes(automargin=True, title='Sector')
        fig.update_layout(
            template='presentation',
            legend=dict(title_text='Unit Type', font=dict(size=16), yanchor='top', y=0.99),
            width=1200,
            height=800
            )

        fname = self._fig_path / f'summary_figure_{self._year}'

        if write_fig:
            pio.write_image(
                fig,
                file=fname, 
                format=self._fig_format,
                engine=self._pio_engine
                )

        else:
            fig.show()

        return None
    
    def set_mecs_data(self, year=2018):
        """"
        MECS format is not machine-friendly. This is a manual input
        of MECS combustion energy estimates from MECS Table 3.2:
        https://www.eia.gov/consumption/manufacturing/about.php

        Returns
        -------
        mecs : dict
            Dictionary of combustion energy estimates from MECS (in MJ), on
            a national and census region basis.
        """

        mecs = {
            2018: {
                'nation': (14859 - 2591) * 1.055E9,
                'northeast': (1130 - 250) * 1.055E9 ,
                'midwest': (3907 - 834) * 1.055E9,
                'south': (7897 - 1143) * 1.055E9,
                'west': (1925 - 365) * 1.055E9
                }
            }
        
        return mecs[year]
    
    def get_eia_seds(self, year=2017):
        """
        Get EIA State Energy Data System (SEDS) data

        Parameters
        ----------
        year : int; default=2017
            Year of SEDS data to return.

        Returns
        -------
        seds : pandas.DataFrame
            
        
        """
        seds_url = 'https://www.eia.gov/opendata/bulk/SEDS.zip'

        try:
            r = requests.get(seds_url)
            r.raise_for_status()

        except r.exceptions.HTTPError as e:
            logging.ERROR(e)

        else:
            with zipfile.ZipFile(BytesIO(r.content)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    seds = pd.read_json(f, lines=True)
                    # eia_data = pd.DataFrame.from_dict(f)


        seds.dropna(subset=['series_id'], inplace=True)
        seds.dropna(axis=1, thresh=1, inplace=True)

        # Natural gas consumed by the industrial sector (including supplemental gaseous fuels)
        # All petroleum products consumed by the industrial sector
        # Coal consumed by the industrial sector
        # Wood and waste consumed in the industrial sector
        series = ['NGICB', 'PAICB' , 'CLICB', 'WWICB']

        seds = seds[seds.series_id.apply(
            lambda x: any([s in x for s in series])
            )].copy(deep=True)
            
        seds.reset_index(drop=True, inplace=True)

        final_seds = pd.DataFrame()

        for i in seds.index:

            data = pd.DataFrame(
                seds.loc[i, 'data'], 
                columns=['year', 'BillionBtu']
                )
            
            data = pd.concat(
                [pd.DataFrame(np.tile(seds.loc[i, seds.columns[0:-1]].values.reshape(13, 1), len(data))).T,
                 data], axis=1)
            
            final_seds = final_seds.append(data)

        for i, v in enumerate(seds.columns[0:-1]):
            final_seds.rename(columns={i:v}, inplace=True)

        final_seds.year.update(final_seds.year.astype(int))

        final_seds = final_seds.query("year==@year").copy(deep=True)
        

        # seds = pd.DataFrame()
        
        # for i in eia_data.index:
        #     try:
        #         data = pd.read_json(eia_data.loc[i, 0])
        #     except ValueError:
        #         continue
    
        #     else:
    
        #         data = pd.concat([
        #             data, 
        #             pd.DataFrame(
        #                 [x for x in data.data.values], columns=['year', 'BillionBtu']
        #                 )], axis=1
        #             )

        #         seds = seds.append(data)

        final_seds.loc[:, 'MJ'] = final_seds.BillionBtu * 1.055E6
        final_seds.drop(['start', 'end', 'BillionBtu', 'last_updated'], 
                        axis=1, inplace=True)
        
        final_seds = final_seds[final_seds.geography != 'USA'].copy(deep=True)
        
        def split_columns(**kwargs):

            final_seds = kwargs['data']

            df = pd.DataFrame(
                final_seds[kwargs['data_column']].unique(),
                columns=[kwargs['data_column']]
                )

            df.loc[:, kwargs['new_column']] = df[kwargs['data_column']].apply(
                lambda x: x.split(f'{kwargs["split_char"]}')[1]
                )
            
            final_seds = pd.merge(final_seds, df, on=kwargs['data_column'], how='left')

            return final_seds

        final_seds = split_columns(
            data=final_seds,
            data_column= 'geography', 
            new_column='state',
            split_char='-'
            )

        # final_seds = split_columns(
        #     data = final_seds,
        #     data_column = 'series_id', 
        #     new_column='data_id',
        #     split_char= '.'
        #     )

        # states = pd.DataFrame(final_seds.geography.unique(), columns=['geography'])
        # states.loc[:, 'state'] = states.geography.apply(
        #     lambda x: x.split('-')[1]
        #     )
        
        # final_seds = pd.merge(final_seds, states, on='geography', how='left')

        final_seds = final_seds.groupby('state', as_index=False).MJ.sum()

        return final_seds


    def get_state_region(self):
        """ Download state to region file"""

        url = 'https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv'
    
        s_r = pd.read_csv(url, encoding='latin_1')

        return s_r
    
    def plot_eia_comparison_maps(self, dataset, year=2017):
        """
        Plots a relative comparison of FIED vs. EIA on a geographic 
        basis (combustion energy only). 
        For MECS, this is census region; for SEDS, states.

        Parameters
        ----------
        dataset : str; {'mecs', 'seds'}
            EIA dataset to compare FIED against.

        year : int ; default=2017

        
        Returns
        -------
        
        """
        shared_plot_args = dict(
            color_continuous_scale= [
                [0, 'rgb(43,131,186)'],
                [0.25, 'rgb(171,221,164)'],
                [0.5, 'rgb(255,255,191)'],
                [0.75, 'rgb(253,174,97)'],
                [1, 'rgb(215,25,28)']
                ],
            color_continuous_midpoint=1,
            locationmode="USA-states",
            scope='usa',
            locations = 'stateCode',
            color = 'fied_relative',
            labels={'fied_relative': f'FIED relative to EIA {dataset.upper()}'}
            )

        plot_data = self._fied.copy(deep=True)

        # with requests.get('https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/us-state-boundaries/exports/geojson?lang=en&timezone=America%2FNew_York') as r:
        #     states = r.json()

        # for s in range(0, len(states['features'])):
        #     states['features'][s]['id'] =  states['features'][s]['properties']['stusab']

        if dataset == 'mecs':
            
            mecs = self.set_mecs_data()

            s_r = self.get_state_region()[['State Code', 'Region']]
            s_r.Region.update(s_r.Region.apply(lambda x: x.lower()))

            plot_data = pd.merge(
                plot_data, s_r, left_on='stateCode', right_on='State Code',
                how='inner'
                )
            
            plot_data = pd.concat([
                plot_data[
                    (plot_data.energyMJ==0) | (plot_data.energyMJ.isnull())
                    ].groupby(
                        ['Region', 'stateCode']
                        ).energyMJq2.sum(),
                plot_data.groupby(
                        ['Region', 'stateCode']
                    ).energyMJ.sum()
                ], axis=1
                )

            plot_data.loc[:, 'totalMJ'] = plot_data.sum(axis=1)

            plot_data.loc[:, 'mecsMJ'] = plot_data.index.get_level_values(0).map(mecs)

            plot_data.loc[:, 'fied_relative'] =  plot_data.totalMJ.sum(level=0).divide(plot_data.mecsMJ)

            plot_data.reset_index(inplace=True)

        elif dataset == 'seds':

            seds = self.get_eia_seds(year=2017)
            seds.rename(columns={'MJ': 'sedsMJ'}, inplace=True)

            plot_data = pd.concat([
                plot_data[
                    (plot_data.energyMJ==0) | (plot_data.energyMJ.isnull())
                    ].groupby(['stateCode']).energyMJq2.sum(),
                plot_data.groupby(['stateCode']).energyMJ.sum()
                ], axis=1
                )
            
            plot_data.loc[:, 'totalMJ'] = plot_data.sum(axis=1)

            plot_data = plot_data.join(seds.set_index('state'))

            plot_data.loc[:, 'fied_relative'] =  plot_data.totalMJ.divide(plot_data.sedsMJ)
            
            plot_data.reset_index(inplace=True)
    
            plot_data.dropna(inplace=True)  # Drop Territories

        rel_max = plot_data.fied_relative.max()
        
        if rel_max > np.around(rel_max):
            rel_max = np.around(rel_max)+1

        else:   
            rel_max = np.around(rel_max)

        shared_plot_args['range_color'] =(0, rel_max)
        shared_plot_args['data_frame'] = plot_data

        fig = px.choropleth(**shared_plot_args)
            # plot_data,
            # color_continuous_scale= [
            #     [0, 'rgb(43,131,186)'],
            #     [0.25, 'rgb(171,221,164)'],
            #     [0.5, 'rgb(255,255,191)'],
            #     [0.75, 'rgb(253,174,97)'],
            #     [1, 'rgb(215,25,28)']
            #     ],
            # range_color=(0, rel_max),
            # color_continuous_midpoint=1,
            # locationmode="USA-states",
            # scope='usa',
            # locations = 'stateCode',
            # color = 'fied_relative',
            #     labels={'fied_relative': 'FIED Relative to EIA MECS'}
            # )
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        fig.show()
            
    
    def plot_eia_comparison(self, eia_mecs=1.568E13, eia_mer=2.316E13):
        """
        Plot bar chart depicting FIED energy estimates relative to EIA estimates.    


        Parameters
        ----------
        eia_mecs : float; default=1.568E13
            Combustion energy use (in MJ) from EIA Manufacturing Consumption survey. 
            Default represents 2018 value.

        eia_mer : float; default=2.316E13
            Combustion energy use (in MJ) for industry from EIA Monthly Energy Review (MER).
            Default represents 2017 value. 
        
        """



    def plot_unit_bubble_map(self, unit_type, measure, max_size=66, write_fig=True):
        """
        Plot locations of a single standard unit type by
        either energy (MJ) or design capacity.

        Parameters
        ----------
        unit_type : str
            Standard unit type: 'other combustion', 'kiln', 'dryer', 
            'boiler', 'heater', 'turbine', 'oven', 'engine', 'furnace', 
            'thermal oxidizer', 'incinerator', 'other', 'generator', 
            'flare', 'stove', 'compressor', 'pump',
            'building heat', 'distillation'

        measure : str; {'energy', 'power', 'ghgs'}
            Either 'energy' (results in MJ), 'power' (results in MW), or
            'ghgs' (results in MTCO2e)

        max_size : int
            Max size of bubbles on map

        write_fig : bool; default=True
            Write figure to analysis figures directory
        """

        if unit_type:

            plot_data = pd.DataFrame(self._fied.query("unitTypeStd == @unit_type"))
            file_name = f'bubble_map_{measure}-{unit_type}_{self._year}'

        else:
            plot_data = self._fied.copy(deep=True)
            file_name = f'bubble_map_{measure}_{self._year}'

        plot_args = {
            'lat': 'latitude',
            'lon': 'longitude',
            'scope': 'usa',
            'color': 'unitTypeStd',
            'title': f'Location of {unit_type.title()} Units Reporting {measure.title()} Data',
            'size_max': max_size,
            'color_discrete_sequence': px.colors.qualitative.Safe
            }

        if measure == 'energy':
            plot_data = plot_data.query('energyMJ>0')
            plot_args['size'] = plot_data.energyMJ.to_list()
            plot_args['labels'] = {
                'energyMJ': 'Unit Energy Use (MJ)'
                }

        elif measure == 'power':
            plot_data = plot_data.query(
                "designCapacityUOM=='MW' & designCapacity>0"
                )
            plot_args['size'] = plot_data.designCapacity.to_list()
            plot_args['labels'] = {
                'designCapacity': 'Unit Capacity (MW)'
                }
            plot_args['title'] = f'Location of {unit_type.title()} Units Reporting Capacity Data'

        
        elif measure == 'ghgs':
            plot_data = plot_data.query(
                "designCapacityUOM=='MW' & designCapacity>0"
                )
            plot_args['size'] = plot_data.designCapacity.to_list()
            plot_args['labels'] = {
                'designCapacity': 'Unit Capacity (MW)'
                }
            plot_args['title'] = f'Location of {unit_type.title()} Units Reporting Capacity Data'

        # sizeref = plot_args['size'].max()/max_size**2

        fig = px.scatter_geo(plot_data, **plot_args)
        fig.update_geos(bgcolor='#FFFFFF')
        fig.update_layout(showlegend=True)
        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)

        if write_fig:
            pio.write_image(
                fig,
                file=self._fig_path / file_name,
                format=self._fig_format,
                engine=self._pio_engine
                )

        else:
            fig.show()

        return None

    def plot_stacked_bar_missing(self, naics_level=2, data_subset=None, write_fig=True):
        """"
        Creates stacked bar showing counts of facilities
        with and without unit-level data.

        Parameters
        ----------
        naics_level : int; default=2
            Specific level of NAICS aggregation to display
            data. NAICS is a range of integers from 2 to 6.
            
        data_subset : str; {None, 'ghgrp', 'nei'}
            Plot subset of data, either facilities that 
            are GHGRP or NEI reporters

        write_fig : bool; default=True
            Write figure to analysis figures directory

        """

        plot_data = self._fied.copy(deep=True)
        
        label = "Facility Count"
        
        if data_subset == 'ghgrp':
            plot_data = plot_data.query(
                "ghgrpID.notnull()", engine="python"
                ).copy()
            
            label = label + " (GHGRP Reporters Only)"

        elif data_subset == 'nei':
            plot_data = plot_data.query(
                "eisFacilityID.notnull()", engine="python"
                ).copy()
            
            label = label + " (NEI Reporters Only)"

        else:
            pass

        plot_data = pd.concat(
            [plot_data.query("unitTypeStd.isnull()", engine="python").groupby(
                f'n{naics_level}'
                ).apply(lambda x: np.size(x.registryID.unique())),
            plot_data.query("unitTypeStd.notnull()", engine="python").groupby(
                f'n{naics_level}'
                ).apply(lambda x: np.size(x.registryID.unique()))], 
            axis=1,
            ignore_index=False
            )

        plot_data.reset_index(inplace=True)

        plot_data.columns = ['NAICS Code', 'Without Unit Characterization', 
                             'With Unit Characterization']
        plot_data.dropna(subset=['NAICS Code'], inplace=True)

        plot_data.loc[:, 'NAICS Code'] = plot_data['NAICS Code'].astype(str)
        fig = px.bar(plot_data,
                    x='NAICS Code',
                    y=['Without Unit Characterization',
                        'With Unit Characterization'],
                    labels={
                        'value': label,
                        'variable': 'Facilities'
                        },
                    template='plotly_white',
                    color_discrete_map={
                        'Without Unit Characterization': "#bcbddc",
                        'With Unit Characterization': "#756bb1"
                        })

        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True, type='category')
        fig.update_layout(
            template='presentation',
            height=800,
            width=1200,
            legend=dict(orientation="h", x=0.25, title=""),
            font=dict(size=16)
            )

        if write_fig:
            pio.write_image(
                fig,
                file=self._fig_path / f'stacked_bar_NAICS{naics_level}_{data_subset}_{self._year}',
                format=self._fig_format,
                engine=self._pio_engine
                )

        else:
            fig.show()

        return None

    def plot_ut_by_naics(self, naics_level=None, variable='count', write_fig=True):
        """
        Creates a table that summarizes by industrial sector
        (i.e., 2-digit NAICS) various aspects of the dataset

        Parameters
        ----------
        naics_level : int; default=None
            Specified NAICS level (None or 2 - 6)

        variable : str; {'count', 'energy', 'capacity'}

        write_fig : bool; default=True
            Write figure to analysis figures directory

        """

        formatting = {
            'x': 'unitTypeStd',
            'template': 'presentation',
            'labels': {
                'unitTypeStd': 'Standardized Unit Type'
                },
            }
        
        plot_data = self._fied.copy(deep=True) 

        if not naics_level:

            grouper = 'unitTypeStd'

        else:

            grouper = ['unitTypeStd', f'n{naics_level}']

            if naics_level == 2:
                pass

            else:
                plot_data = self.make_consistent_naics_column(
                    plot_data, naics_level
                    )

            plot_data.loc[:, f'n{naics_level}'] = \
                plot_data[f'n{naics_level}'].astype(str)

            len_naics = len(plot_data[f'n{naics_level}'].unique())

            formatting['labels'][f'n{naics_level}'] = 'NAICS Code'
            formatting['color'] = f'n{naics_level}'
            # formatting['color_discrete_sequence'] = px.colors.sequential.Plasma_r
            formatting['color_discrete_sequence'] = px.colors.sample_colorscale(
                "plasma_r", [n/(len_naics-1) for n in range(len_naics)]
                )

        if variable == 'count':
            plot_data = plot_data.query(
                "unitTypeStd.notnull()", engine="python"
                ).groupby(grouper).registryID.count()

            formatting['labels']['registryID'] = 'Unit Count'
            formatting['y'] = 'registryID'

        elif variable == 'energy':
            plot_data = plot_data.query(
                "unitTypeStd.notnull()", engine="python"
                ).groupby(grouper)['energyMJ', 'energyMJq0'].sum().sum(axis=1)

            plot_data.name = 'energy'
            formatting['y'] = 'energy' 
            formatting['labels']['energy'] = 'Energy (MJ)'

        elif variable == 'capacity':
            plot_data = plot_data.query(
                "unitTypeStd.notnull() & designCapacityUOM=='MW'", engine="python"
                ).groupby(grouper).designCapacity.sum()

            formatting['y'] = 'designCapacity'
            formatting['labels']['designCapacity'] = 'Unit Design Capacity (MW)'

        else:
            logging.error('No variable specified')
            raise ValueError

        if type(plot_data) == pd.core.series.Series:
            plot_data = pd.DataFrame(plot_data).reset_index()

        else:
            plot_data.reset_index(inplace=True)

        plot_data.loc[:, 'unitTypeStd'] = [x.capitalize() for x in plot_data.unitTypeStd]
        plot_data = plot_data.sort_values(by=grouper, ascending=True)

        fig = px.bar(plot_data, **formatting)

        if variable == 'count':
            pass

        else:
            fig.update_layout(
                yaxis=dict(
                    showexponent='all',
                    exponentformat='power'
                    )
                )

        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)

        if write_fig:
            pio.write_image(
                fig,
                file=self._fig_path / f'unittype_NAICS{naics_level}_{variable}_{self._year}',
                format=self._fig_format,
                engine=self._pio_engine
                )

        else:
            fig.show()

        return None

    def make_consistent_naics_column(self, final_data, n):
        """
        Creates a column of consisently aggregated NAICS codes
        (i.e., same number of digits) when a column of 
        NAICS codes contains different levels of aggregation.
        Will only include 

        Parameters
        ----------
        final_data : pandas.DataFrame or parquet

        n : int; 2 to 6
            Specified NAICS level

        Returns
        -------
        analysis_data : pandas.DataFrame
            Returns original DataFrame with
            new column named f'n{n}'.
        """

        df_naics = pd.DataFrame(final_data['naicsCode'].drop_duplicates(),
                                dtype=int)

        df_naics_consistent = df_naics[
            df_naics.naicsCode.apply(lambda x: len(str(x)) >= n)
            ]

        if len(df_naics_consistent) < len(df_naics):
            logging.info(f"Caution: not all original NAICS codes are at this aggregation")

        else:
            pass

        df_naics_consistent.loc[:, f'n{n}'] = df_naics_consistent.naicsCode.apply(
            lambda x: int(str(x)[0:n])
            )

        analysis_data = final_data.copy(deep=True)
        analysis_data = pd.merge(
            analysis_data, df_naics_consistent, on='naicsCode'
            )

        return analysis_data

    
    def plot_best_characterized(self):
        """
        Identify which NAICS codes have the highest portion of facilities that
        have some unit-level characterization associated with them.

        """

        plot_data = self._fied.copy(deep=True)

        # find most aggregated NAICS (smallest number of digits)
        smallest_n = plot_data.naicsCode.unique()

        smallest_n = min([len(str(int(x))) for x in smallest_n])

        plot_data = self.make_consistent_naics_column(plot_data, smallest_n)

        plot_data = plot_data.groupby('stateCode') 

        plot_data = pd.concat([plot_data.apply(lambda x: pd.concat(
            [x.query("unitTypeStd.isnull()", engine="python").groupby(
                f'n{smallest_n}'
                ).apply(lambda y: np.size(y.registryID.unique())),
            x.query("unitTypeStd.notnull()", engine="python").groupby(
                f'n{smallest_n}'
                ).apply(lambda y: np.size(y.registryID.unique()))], 
            axis=1,
            ignore_index=False
            ))], axis=0)
        
        
        plot_data.columns = ['unchar', 'char']
        plot_data.loc[:, 'total'] = plot_data.sum(axis=1)
        plot_data.loc[:, 'fraction_char'] = plot_data.char.divide(
            plot_data.total
            )
        
        plot_data.fillna(0, inplace=True)
        
        # # State sorted mean characterized
        # state_char = pd.concat(
        #     [plot_data.fraction_char.mean(level=0),
        #      plot_data.total.sum(level=0)], axis=1
        #     ).sort_values(by='fraction_char', ascending=False)

        # # NAICS sorted mean characterized
        # naics_char = pd.concat(
        #     [plot_data.fraction_char.mean(level=1),
        #      plot_data.total.sum(level=1)],
        #      axis=1
        #     ).sort_values(by='fraction_char', ascending=False)
    
        # Level 0 is state aggregation; level 1 is NAICS aggregation
        for k in [0, 1]:

            d = pd.concat(
                [plot_data.fraction_char.mean(level=k),
                plot_data.total.sum(level=k)], axis=1
                ).sort_values(by='fraction_char', ascending=False)
            
            if k == 1:

                d.index = d.index.astype('str')

                d = d[d.fraction_char > 0]

            fig, ax = plt.subplots(figsize=(6, 12), tight_layout=False)
            
            sns.scatterplot(
                data=d,
                y=d.index,
                x="fraction_char",
                size="total",
                sizes=(10, 800),
                alpha=.5, ax=ax
                )
            
            if k == 1:
                [l.set_visible(False) for (i, l) in enumerate(ax.yaxis.get_ticklabels()) if i % 6 != 0]
                ax.set_ylabel('NAICS Code')
                
            else:
                ax.set_ylabel('State')
           
            ax.set_xlabel("Fraction of Facilities with Unit Characterization")
            plt.legend(
                title='Total Number of Facilities', 
                loc='best',
                frameon=False)

            plt.show()
            

    # def unit_capacity_nonintensive(self):
    #     """
        
    #     Parameters
    #     ----------

    #     df : pandas.DataFrame
    #         Final foundational dataset

    #     naics : str; {'all', 'non-mfg', 'intensive', 'other-mfg'} or list of ints; default='all'
    #         Group output by NAICS code group (str) or by list of integer(s). Integers must
    #         be at 4-digit NAICS level.

    #     Returns
    #     -------

    #     naics_plot : 
        
    #     """

    #     # plot_data = id_sectors(final_data)

    #     # Make subplots, one each for non-mfg,
    #     # intenstive, and other-mfg
    #     fig = make_subplots(
    #         rows=2, cols=2,
    #         specs=[
    #             [{}, {}],
    #             [{"colspan": 2}, None]
    #             ],
    #         print_grid=False)
        

    def summary_table_intensive(self):
        """
        Summary description of unit coverage for intensive 
        and non-intensive industries.
        
        """


        df_naics = pd.DataFrame(
            self._fied.naicsCode.drop_duplicates()
            )

        df_naics.loc[:, 'n4'] = df_naics.naicsCode.apply(lambda x: int(str(x)[0:4]))

        plot_data = dict(
            all=pd.merge(self._fied, df_naics, on='naicsCode')
            )

        # NAICS subsectors of Paper Manufacturing, Petroleum and Coal Products,
        # Chemical Manufacturing, Glass and Glass Product Manufacturing, Cement and
        # Concrete, Lime and Gypsum, Iron & Steel, Alumina and Aluminum,
        intensive_naics = [3221, 3241, 3251, 3253, 3272, 3273, 3274, 3311, 3313]

        plot_data['non-mfg'] = \
            plot_data['all'][~plot_data['all'].n4.between(3000, 4000)]

        plot_data['intensive'] = \
            plot_data['all'][plot_data['all'].n4.isin(intensive_naics)]

        plot_data['other-mfg'] = \
            plot_data['all'][
                ~plot_data['all'].n4.isin(intensive_naics) & \
                plot_data['all'].n4.between(3000, 4000)]

        for k in plot_data.keys():
            if k == 'all':
                pass
            else:
                plot_data['all'].loc[plot_data[k].index, 'grouping'] = k      

        summ_table = pd.DataFrame(index=['intensive', 'other-mfg', 'non-mfg'])

        ind_grp = plot_data['all'].groupby('grouping')

        summ_table = pd.concat([
            ind_grp.apply(lambda x: len(x.registryID.unique())),
            ind_grp.apply(
                lambda x: len(x[x.unitTypeStd.notnull()].registryID.unique())
                ),
            ind_grp.apply(lambda x: len(x[
                (x.unitTypeStd.notnull()) &
                (x.designCapacity.notnull()) &
                (x.designCapacityUOM == 'MW')
                ].registryID.unique())
                ),
            ind_grp.apply(
                lambda x: x[x.designCapacityUOM == 'MW'].designCapacity.sum()
                ),
            ind_grp.apply(
                lambda x: x[x.designCapacityUOM == 'MW'].designCapacity.median()
                )], axis=1, ignore_index=False
            )

        summ_table.update(
            summ_table.loc[:, [1, 2]].divide(summ_table.loc[:, 0], axis=0)
            )

        summ_table.columns = ['Count of Facilities', 'Facilities with Unit Type',
                            'Facilities with Unit Type & Capacity',
                            'Total Capacity (MW)', 'Median Capacity (MW)']

        summ_table = summ_table.style.format({
            'Facilities with Unit Type': '{:.0%}',
            'Facilities with Unit Type & Capacity': '{:.0%}',
            'Total Capacity (MW)': '{:.2f}'}
            )
        
if __name__ == '__main__':
        
    year = 2017
    filepath = os.path.abspath('foundational_industry_data_2017.csv.gz')
    fa = FIED_analysis(year=2017, file_path=filepath, fig_format='svg')

    fa.create_core_analysis(write_fig=False)
