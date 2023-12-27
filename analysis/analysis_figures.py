
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

    def __init__(self, year, file_path=None, df=None):

        if file_path is None:
            self._fied = df

        else:
            try:
                self._fied = pd.read_parquet(
                    file_path
                    )

            except (pyarrow.ArrowIOError, pyarrow.ArrowInvalid):
                self._fied = pd.read_csv(
                    file_path, low_memory=False
                    )

        self._fied = self.make_consistent_naics_column(self._fied, n=2)
        
        self._year = year

        self._fig_path = pathlib.Path('./analysis/figures')

        if self._fig_path.exists():
            pass
    
        else:
            pathlib.Path.mkdir(self._fig_path)

        self._fig_format = 'svg'

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

        summary_table = self.summary_unit_table()

        self.summary_unit_bar(summary_table, write_fig=kwargs['write_fig'])

        self.stacked_bar_missing(write_fig=kwargs['write_fig'])

        self.plot_facility_count(write_fig=kwargs['write_fig'])

        for u in self._fied.unitTypeStd.unique():
            try:
                u.title()
            except AttributeError:
                continue
            else:
                for m in ['energy', 'power']:
                    self.unit_bubble_map(u, m, write_fig=kwargs['write_fig'])

        for v in ['count', 'energy', 'capacity']:
            for n in [None, 2, 3]:
                self.plot_ut_by_naics(n, v, write_fig=kwargs['write_fig'])

        return
    
    def summary_unit_table(self):
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

        fied_count = pd.DataFrame(self._fied)
        fied_count.loc[:, 'n4'] = fied_count.naicsCode.apply(
            lambda x: int(str(int(x))[0:4])
            )

        fied_count = pd.concat(
            [fied_count.groupby('n4').registryID.apply(lambda x: len(x.unique())),
            ind_cbp_data.set_index('naics').est],
            axis=1,
            ignore_index=False
            ).reset_index()

        fied_count['index'].update(fied_count['index'].astype(str))
        fied_count.loc[:, 'sector'] = fied_count['index'].apply(
            lambda x: x[0:2]
            ).map(ind_map)
        
        fied_cumsum = pd.merge(
            fied_count[['index', 'sector']],
            fied_count.groupby(['sector'])['registryID', 'est'].cumsum(),
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
    
        if write_fig is True:
            pio.write_image(
                fig,
                file=self._fig_path / f'counts_{self._year}.{self._fig_format}'
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

        if write_fig is True:
            pio.write_image(
                fig,
                file=self._fig_path / f'summary_figure_{self._year}.{self._fig_format}'
                )

        else:
            fig.show()

        return None

    def unit_bubble_map(self, unit_type, measure, max_size=45, write_fig=True):
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

        measure : str
            Either 'energy' (results in MJ) or 'power' (results in MW)

        max_size : int
            Max size of bubbles on map

        write_fig : bool; default=True
            Write figure to analysis figures directory
        """

        if unit_type:

            plot_data = pd.DataFrame(self._fied.query("unitTypeStd == @unit_type"))
            file_name = f'bubble_map_{measure}-{unit_type}_{self._year}.{self._fig_format}'

        else:
            plot_data = self._fied.copy(deep=True)
            file_name = f'bubble_map_{measure}_{self._year}.{self._fig_format}'

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

        # sizeref = plot_args['size'].max()/max_size**2

        fig = px.scatter_geo(plot_data, **plot_args)
        fig.update_layout(showlegend=True)
        fig.update_yaxes(automargin=True)
        fig.update_xaxes(automargin=True)

        if write_fig is True:
            pio.write_image(
                fig,
                file=self._fig_path / file_name
                )

        else:
            fig.show()

        return None

    def stacked_bar_missing(self, naics_level=2, data_subset=None, write_fig=True):
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
                "nei.notnull()", engine="python"
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

        if write_fig is True:
            pio.write_image(
                fig,
                file=self._fig_path / f'stacked_bar_NAICS{naics_level}_{data_subset}_{self._year}.{self._fig_format}'
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
                    self._fied, naics_level
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

        if write_fig is True:
            pio.write_image(
                fig,
                file=self._fig_path / f'unittype_NAICS{naics_level}_{variable}_{self._year}.{self._fig_format}'
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

        analysis_data = final_data.copy()
        analysis_data = pd.merge(
            analysis_data, df_naics_consistent, on='naicsCode'
            )

        return analysis_data

    def unit_capacity_nonintensive(self):
        """
        
        Parameters
        ----------

        df : pandas.DataFrame
            Final foundational dataset

        naics : str; {'all', 'non-mfg', 'intensive', 'other-mfg'} or list of ints; default='all'
            Group output by NAICS code group (str) or by list of integer(s). Integers must
            be at 4-digit NAICS level.

        Returns
        -------

        naics_plot : 
        
        """

        # plot_data = id_sectors(final_data)

        # Make subplots, one each for non-mfg,
        # intenstive, and other-mfg
        fig = make_subplots(
            rows=2, cols=2,
            specs=[
                [{}, {}],
                [{"colspan": 2}, None]
                ],
            print_grid=False)
        

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
    fa = FIED_analysis(year=2017, file_path=filepath)

    fa.create_core_analysis(write_fig=True)
        