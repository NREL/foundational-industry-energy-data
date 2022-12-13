
import pandas as pd
import numpy as np
import re

# ?Approach? Iterate through each eis_facility_id and then by eis_process_id
# for i in eis_facility_id.unique():  
#     for p in eis_process_id:


def id_external_combustion(scc_level_two, scc_level_three, scc_level_four):
    """
    Method for identifying unit type and fuel type under the 
    "External Combustion"
    SCC Level One. See ... for SCC documentation.

    Parameters
    ----------
    scc_level_two : str
        s

    scc_level_three : str

    scc_level_four : str 

    """
    scc_level_one = 'External Combustion'

    if ':' in scc_level_four:

        unit_type = scc_level_four.split(': ')[1]

        if unit_type in ['Shredded', 'Specify Percent Butane in Comments',
                         'Specify in Comments', 'Specify Waste Material in Comments',
                         'Refuse Derived Fuel', 'Sewage Grease Skimmings', 'Waste Oil',
                         ]:

            unit_type = 'Boiler'

        else:
            pass

    else:
        unit_type = scc_level_four

    fuel_type = scc_level_three

    if fuel_type in ['Industrial', 'Commercial/Institutional']:
        unit_type = scc_level_two
        fuel_type = scc_level_four.split(':')[0]

    else:
        pass

    if unit_type in ['All', 'Butane', 'Propane']:
        unit_type = 'Boiler'

    else:
        pass

    if 'Wood-fired' in unit_type:
        unit_type = 'Boiler'

    else:
        pass

    return np.array([unit_type, fuel_type])

def identify_unit_fuel(scc_level_one, scc_level_two, scc_level_three, scc_level_four):
    """
    Use SCC levels to identify unit type and fuel type indicated by a SCC code.

    Parameters

    """

    if scc_level_one == 'External Combustion':


    if scc_level_one == 'Internal Combustion Engines':

        unit_type = scc_level_four
        fuel_type = scc_level_three

    if scc_level_one == 'Stationary Source Fuel Combustion':

        unit_type = scc_level_four
        fuel_type = scc_level_three

    if scc_level_one == 'Chemical Evaporation':

        if (scc_level_two == 'Surface Coating Operations') & ((
            'dryer' in scc_level_four.lower()) | ('drying' in scc_level_four.lower())):

            unit_type = scc_level_four
            fuel_type = np.nan

        else:

            if scc_level_three == 'Coating Oven Heater':

                unit_type = 'Coating Oven Heater'
                fuel_type = scc_level_four

            if scc_level_three == 'Fuel Fired Equipment':

                fuel_type, unit_type = scc_level_four.split(':')

            if scc_level_three == 'Drying':

                unit_type = 'Dryer'
                fuel_type = np.nan

            if (scc_level_three != 'Drying') & (scc_level_four == 'Dryer'):
                
                unit_type = 'Dryer'
                fuel_type = np.nan

            # Skipping dry cleaning operations. Unsure whether "drying"
            # includes application of heat

            if (scc_level_three == 'Fuel Fired Equipment') & (scc_level_two == 'Organic Solvent Evaporation'):

                unit_type, fuel_type = scc_level_four.split(':')

    if scc_level_one == 'Industrial Processes':

        if scc_level_three == 'Commercial Cooking':

            unit_type = scc_level_four
            fuel_type = np.nan

        if scc_level_two == 'In-process Fuel Use':

            unit_type = 'feedstock'
            fuel_type = scc_level_four

        if scc_level_three == 'Ammonia Production':
            
            unit_type = scc_level_four
            fuel_type = np.nan

        scc_level_three == 

            [x in scc_level_four.lower() for x in ['calciner', 'evaporator', 'furnace', 'dryer', 'kiln', 'oven', 'incinerator', 'distillation'] 


        scc_level_four == 'Reclamation Furnace'

def find_unit(row):
    """
    Checks various fields to find unit type
    """

    types = 

def find_fuel(row):
    """
    Checks various fields to find fuel type
    """

def back_calc_prod(row):
    """
    Back-calculates production from SCC and emissions factor
    """

def back_calc_energy(row):
    """
    Back-calculates energy use from SCC and emissions factor
    """