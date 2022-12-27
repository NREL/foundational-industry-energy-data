
import pandas as pd
import ag_methods as ag
import dask.dataframe as dd
import datetime as dt

def Agriculture():
    """
    Import and format previously calculated agriculture fuel and energy use.
    ag_electricity.py and ag_fuel.py should be refactored for methods, etc.
    """

    # use 2012 for 2010 - 2014; use 2017 for 2015 - 2016
    
    fuel_results_file_12 = \
        '../results/ag_output_fuel_use_by_county_2012_20190812_2238.csv'
    
    fuel_results_file_17 = \
        '../results/ag_output_fuel_use_by_county_2017_20190812_2230.csv'

    elect_results_file_12 = \
        '../results/ag_output_electricity_use_by_county_2012_20190813_1024.csv'
    
    elect_results_file_17 =\
        '../results/ag_output_electricity_use_by_county_2017_20190813_1019.csv'
    

    def import_format(results_filepath):
        """
        
        """

        ag_energy = pd.read_csv(results_filepath, index_col=0)
        
        # Check if index was written to file
        if ag_energy.index.names != [None]:
            
            ag_energy.reset_index(inplace=True)

        ag_energy.replace({'LP GAS': 'LPG_NGL', 'NATURAL_GAS': 'Natural_gas',
                           'DIESEL': 'Diesel', 'LPG': 'LPG_NGL',
                           'GASOLINE': 'Other', 'OTHER': 'Residual_fuel_oil',
                           'ELECTRICITY': 'Net_electricity'}, inplace=True)
    
        
        ag_energy.rename({'fuel_type': 'MECS_FT'}, inplace=True)
        
        return ag_energy
    
    fuel_12 = import_format(fuel_results_file_12)
    
    fuel_17 = import_format(fuel_results_file_17)
    
    elect_12 = import_format(elect_results_file_12)
    
    elect_17 = import_format(elect_results_file_17)

    multiplier_12 = ag.calc_multiplier(base_year=2012, 
                                       calculation_years=range(2010, 2015))

    multiplier_17 = ag.calc_multiplier(base_year=2017,
                                       calculation_years=range(2015, 2018))

    county_fuel = pd.concat(
            [ag.calc_county_fuel(fuel_12, multiplier_12,
                                 calculation_years=range(2010, 2015)), 
             ag.calc_county_fuel(fuel_17, multiplier_17,
                                 calculation_years=range(2015, 2018))],
            axis=1, ignore_index=False
            )

    county_elec = pd.concat(
            [ag.calc_county_fuel(elect_12, multiplier_12,
                                 calculation_years=range(2010, 2015)), 
             ag.calc_county_fuel(elect_17, multiplier_17,
                                 calculation_years=range(2015, 2018))],
            axis=1, ignore_index=False
            )
             
#    county_elec.state.fillna(method='ffill', inplace=True)
#    
#    county_elec.fipstate.fillna(method='ffill', inplace=True)
             
    county_total = pd.DataFrame()
    
    for df in [county_fuel, county_elec]:
        
        df = df.iloc[:, 2:]
        
        df.state.fillna(method='ffill', inplace=True)
        
        df.fipstate.fillna(method='ffill', inplace=True)
        
        df['fipstate'] = df.fipstate.astype(int)
        
        df.reset_index(inplace=True)
        
        # Drop any Alaksan counties missing info
        df = df[df.COUNTY_FIPS !=2]
        
        df.rename(columns={'fuel_type': 'MECS_FT'}, inplace=True)
        
        county_total = county_total.append(df.melt(
                id_vars=['NAICS', 'COUNTY_FIPS', 'fipstate', 'MECS_FT',
                         'state'],
                var_name='year', value_name='MMBtu_TOTAL'
                ))
        
    county_total = dd.from_pandas(
            county_total.set_index('fipstate'),
            npartitions=len(county_total.fipstate.unique())
            )
    
    filename = 'ag_county_energy_' + \
        dt.datetime.now().strftime('%Y%m%d_%H%M')+'.parquet.gzip'
    
    county_total.to_parquet('../results/'+filename, engine='pyarrow',
                            compression='gzip')
    
    return county_total
    
    # Import fuel energy results

    # Import electricity results
