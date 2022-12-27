import pandas as pd
import get_cbp
import cons
import datetime as dt
import dask.dataframe as dd

def Construction(calculation_years=range(2010, 2017)):
    # import 2012 Economic Census data.
    census_data = pd.concat(
                [cons.census(naics) for naics in [23, 236, 237, 238]],
                ignore_index=True
                )
    # Fill in missing values
    DE = cons.fill_in_missing_data(census_data, 'DE')
    DC = cons.fill_in_missing_data(census_data, 'DC')
    WV = cons.fill_in_missing_data(census_data, 'WV')

    census_data = census_data[(census_data.state_abbr != 'DE') &
                              (census_data.state_abbr != 'DC') &
                              (census_data.state_abbr != 'WV')]

    census_data = pd.concat([census_data, DE, DC, WV])

    census_data.set_index('state', inplace=True)

    census_data = census_data.sort_index().reset_index()

    census_data = census_data.apply(pd.to_numeric, errors='ignore')
    
    # Calculate state-level energy use (all in MMBtu)
    # Diesel use
    diesel_state = cons.calc_diesel_state(census_data)

    # Natural gas use
    ng_state = cons.calc_ng_state(census_data)
    
    # Electricity use
    elect_state = cons.calc_elec_state(census_data)
    
    # Liquid petroleum gas use
    lpg_state = cons.calc_lpg_state(census_data)
    
    energy_state = pd.concat(
            [diesel_state, ng_state, elect_state, lpg_state], axis=0,
            ignore_index=True
            )
    
    energy_state = cons.format_state_energy(energy_state)
    
    # Calculate GDP multiplier
    multiplier = cons.calc_bea_multiplier()
    
    cbp_2012 = get_cbp.CBP(2012).cbp
    
    # Calculate county fraction of state construction establishments by 
    # NAICS code.
    county_frac = cons.calc_county_fraction(cbp_2012)
    
    county_frac.rename(columns={'naics': 'NAICS'}, inplace=True)
    
    # Calculate county energy 
    cons_energy = cons.calc_county_energy(
            energy_state, county_frac, multiplier, 
            calculation_years=range(2010, 2017)
            )
    
    # remove sector total (NAICS == 23) and reset index
    cons_energy = cons_energy[cons_energy.NAICS != 23].reset_index()
    
    cons_energy = dd.from_pandas(
            cons_energy.set_index('fipstate'),
            npartitions=len(cons_energy.fipstate.unique())
            )
    
    filename = 'cons_county_energy_'+\
        dt.datetime.now().strftime('%Y%m%d_%H%M')+'.parquet.gzip'
        
    cons_energy.to_parquet('../results/'+filename, compression='gzip',
                           engine='pyarrow')
    

    
