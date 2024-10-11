# -*- coding: utf-8 -*-
"""

Parse SCCs for unit type and fuel type (and capacity?)
Match SCC to WebFire EF

"""

import pandas as pd

scc = pd.read_csv('SCCDownload.csv')

scc.columns = scc.columns.str.replace(' ','_')

scc = scc[scc.data_category=='Point']


"""
Point source SCCs are 8 digits and follow the pattern: ABBXXXZZ

A = level 1 
BB = level 2
XXX = level 3
ZZ = level 4

Level 1
External Combustion A=1; ICE A=2, Industrial Processes A=3; 
Chemical Evaporation A=4; Waste Disposal A=5

Levels 2-4 vary within each level 1 category
"""


def describe_scc_codes(scc_df):
    
    
    # Unit types - Levels 1 and 2
    lev2_dict = {'101':'boilers electricity generation',
                 '102':'boilers industrial combustion',
                 '103':'boilers commercial combustion',
                 '105':'space heater',
                 '201':'ICE electricity generation',
                 '202':'ICE industrial combustion engine',
                 '204':'ICE engine testing',
                 '260':'ICE gasoline engine',
                 '265':'ICE gasoline engine',
                 '270':'ICE diesel engine',
                 '273':'ICE LPG engine',
                 '288':'ICE fugitive emissions',
                 '4':'chemical evaporation',
                 '5':'waste disposal'}
    
    
    scc_df['scc_unit_type'] = scc_df.SCC.str[:3].map(lev2_dict)
    
    scc_df.loc[scc_df.SCC.str[:1].isin(['4','5']),
               'scc_unit_type'] = scc_df.SCC.str[:1].map(lev2_dict)
    
    #------------------------------------------------
    
    # Unit types - Industrial processes
    
    
    unit_dict = {'tank':'tank',

                 'reactor':'reactor',
                 'reactr':'reactor',
                 
                 'kiln':'kiln',
                 
                 'dryer':'dryer',
                 'dryr':'dryer',
                 'drying':'dryer',
                 'dryng':'dryer',
                 
                 'coke oven':'coke oven',
                 'curing oven':'curing oven',
                 'oven':'oven',
                 
                 'furnace':'furnace',
                 
                 'distillation':'distillation',
                       
                 'cooler':'cooler',
                 'cooling tower':'cooling tower',
                 
                 'incinerat':'incinerator',
                 
                 'absorber':'absorber',
                 
                 'mill':'milling',
                 
                 'evaporat':'evaporator',
                 
                 'extru':'extruder',
                 
                 'clean':'cleaning',
                 
                 'calcin':'calciner',
                 
                 'stripp':'stripper',
                 
                 'granulat':'granulator',
                 
                 'dehydrat':'dehydration unit',
                 
                 'degreas':'degreasing unit',
                 
                 'roast':'roaster',
                 
                 'cyclone':'cyclone',
                 
                 'steep':'steeping',
                 
                 'grind':'grinding',
                 
                 'ferment':'fermenter',
                 
                 'cook':'cooker',
                 
                 'compressor':'compressor',
                 
                 'steam gen':'steam generator',
                 
                 'digester':'digester',
                 
                 'blend':'blender',
                 
                 'chipp':'chipper',
                 
                 'process heat':'process heater',
                 
                 'flare':'flare'
                 
                 }
    
    unit_keys = list(unit_dict.keys())
    
    scc_df['scc_level_four'] = scc_df['scc_level_four'].str.lower()
    

    
    # assign unit type according to matches in dictionary
    #   for 306XXXZZ (petroleum), 310XXXZZ (oil & gas): unit can be in level 3        
    for i in scc_df.index[
            (scc_df.SCC.str[:1]=='3') &
            (scc_df.scc_level_four.str.contains('|'.join(unit_keys)))]:
        
        scc_df.loc[i,'scc_unit_type'] = [f for p,f in unit_dict.items() \
                                      if p in scc_df.scc_level_four[i]][0]

                                           
    
    #------------------------------------------------
    
    # Products - Industrial processes

    scc_df.loc[scc_df.SCC.str[:1]=='3',
               'scc_product'] = scc_df.scc_level_three

    #   Doesn't apply consistently for all Industrial Processes;
    #   Need to update for specific sectors only 
             
    
    #------------------------------------------------

    # Fuel types - External Combustion: Electricity Gen.
    lev3_elec_blr_dict = {'001':'anthracite coal',
                 '002':'bituminous coal',
                 '003':'lignite',
                 '004':'residual oil',
                 '005':'distillate oil',
                 '006':'natural gas',
                 '007':'process gas',
                 '008':'coke',
                 '009':'wood/bark waste',
                 '010':'LPG',
                 '011':'bagasse',
                 '012':'solid waste',
                 '013':'liquid waste',
                 '014':'carbon monoxide boiler',
                 '015':'geothermal',
                 '016':'methanol',
                 '017':'gasoline',
                 '018':'hydrogen',
                 '019':'coal-based synfuel',
                 '020':'waste coal',
                 '021':'other oil'}
   
    scc_df.loc[scc_df.SCC.str[:3].isin(['101']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_elec_blr_dict)
    
    
    # Fuel types - External Combustion: Industrial boilers
    lev3_ind_blr_dict = {'001':'anthracite coal',
                 '002':'bituminous coal',
                 '003':'lignite',
                 '004':'residual oil',
                 '005':'distillate oil',
                 '006':'natural gas',
                 '007':'process gas',
                 '008':'coke',
                 '009':'wood/bark waste',
                 '010':'LPG',
                 '011':'bagasse',
                 '012':'solid waste',
                 '013':'liquid waste',
                 '014':'carbon monoxide boiler',
                 '015':'tire-derived fuel',
                 '016':'methanol',
                 '017':'gasoline',
                 '018':'kiln-dried biomass',
                 '019':'wood residuals'}
    
    lev_3_ind_blr_CO_dict = {'01401':'natural gas',
                           '01402':'process gas',
                           '01403':'distillate oil',
                           '01404':'residual oil'}
    
    scc_df.loc[scc_df.SCC.str[:3].isin(['102']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_ind_blr_dict)
    
    scc_df.loc[scc_df.SCC.str[:6].isin(['102014']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev_3_ind_blr_CO_dict)
    
    
    # Fuel types - External Combustion: Commerical boilers
    lev3_ind_blr_dict = {'001':'anthracite coal',
                 '002':'bituminous coal',
                 '003':'lignite',
                 '004':'residual oil',
                 '005':'distillate oil',
                 '006':'natural gas',
                 '007':'process gas',
                 '008':'landfill gas',
                 '009':'wood/bark waste',
                 '010':'LPG',
                 '011':'biomass',
                 '012':'solid waste',
                 '013':'liquid waste'}
    
    scc_df.loc[scc_df.SCC.str[:3].isin(['103']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_ind_blr_dict)
    
   
    # Fuel types - Space Heater - Industrial only
    lev4_space_dict = {'02':'coal',
                       '05':'distillate oil',
                       '06':'natural gas',
                       '10':'LPG',
                       '13':'waste oil',
                       '14':'waste oil'}
    
    scc_df.loc[scc_df.SCC.str[:6].isin(['105001']), 
               'scc_fuel_type'] = scc_df.SCC.str[6:8].map(lev4_space_dict)
    
    
    
    # Fuel types - ICE: Electric Gen
    lev3_ice_dict = {'001':'distillate oil',
                     '002':'natural gas',
                     '003':'gasified coal',
                     '007':'process gas',
                     '008':'landfill gas',
                     '009':'kerosene/naphtha',
                     '010':'geothermal',
                     '013':'liquid waste',
                     '800':'equipment leaks',
                     '820':'waste water',
                     '825':'waste water',
                     '900':'flare'}
    
    scc_df.loc[scc_df.SCC.str[:3].isin(['201']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_ice_dict)
    
    # Fuel types - ICE: Industrial
    lev3_ice_dict = {'001':'distillate oil',
                     '002':'natural gas',
                     '003':'gasoline',
                     '004':'diesel, oil/gas',
                     '005':'residual oil',
                     '007':'process gas',
                     '009':'kerosene/naphtha',
                     '010':'LPG',
                     '016':'methanol',
                     '017':'gasoline',
                     '800':'equipment leaks',
                     '820':'waste water',
                     '825':'waste water',
                     '900':'waste water'}
    
    scc_df.loc[scc_df.SCC.str[:3].isin(['202']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_ice_dict)
    
    
    # Fuel types - ICE: Commercial
    lev3_ice_dict = {'001':'distillate oil',
                     '002':'natural gas',
                     '003':'gasoline',
                     '004':'diesel',
                     '007':'digester gas',
                     '008':'landfill gas',
                     '009':'kerosene/naphtha',
                     '010':'LPG',
                     '800':'equipment leaks',
                     '820':'waste water',
                     '825':'waste water',
                     '900':'waste water'}
    
    scc_df.loc[scc_df.SCC.str[:3].isin(['203']),
               'scc_fuel_type'] = scc_df.SCC.str[3:6].map(lev3_ice_dict)
    
    
    # Fuel types - ICE: Engine testing - turbine
    lev4_turb_dict = {'01':'natural gas',
                     '02':'diesel',
                     '03':'distillate oil',
                     '04':'landfill gas',
                     '05':'kerosene/naphtha',
                     '99':'not classified'}
    
    scc_df.loc[scc_df.SCC.str[:6].isin(['204003']),
               'scc_fuel_type'] = scc_df.SCC.str[6:8].map(lev4_turb_dict)
    
    # Fuel types - ICE: Engine testing - reciprocating engine
    lev4_recip_dict = {'01':'gasoline',
                     '02':'diesel',
                     '03':'distillate oil',
                     '04':'process gas',
                     '05':'landfill gas',
                     '06':'kerosene/naphtha',
                     '07':'gas/oil',
                     '08':'residual oil',
                     '09':'LPG',
                     '99':'not classified'}
    
    scc_df.loc[scc_df.SCC.str[:6].isin(['204004']),
               'scc_fuel_type'] = scc_df.SCC.str[6:8].map(lev4_recip_dict)
    
    # Fuel types - ICE: Engine testing
    scc_df.loc[scc_df.SCC.str[:6].isin(['204001']),
               'scc_fuel_type'] = 'jet fuel'
    
    scc_df.loc[scc_df.SCC.str[:6].isin(['204002']),
               'scc_fuel_type'] = 'propellant'
    

    return


describe_scc_codes(scc)

scc.to_csv('scc_descriptions.csv',index=False)

#scc.columns = scc.columns.str.lower()


 """
 '301':'process chemicals',
 '302':'process food and ag',
 '303':'process primary metals',
 '304':'process secondary metals',
 '305':'process mineral products',
 '306':'process petroleum',
 '307':'process pulp and paper',
 '308':'process rubber and misc plastics',
 '309':'process fabr metals',
 '310':'process oil and gas production',
 '311':'process building construction',
 '312':'process machinery',
 '313':'process electrical equipment',
 '314':'process transportation equipment',
 '315':'process misc equipment',
 '316':'process film mfg',
 '320':'process leather',
 '330':'process textiles',
 '360':'process printing',
 '385':'process cooling tower',
 '390':'process fuel use',
 '399':'process misc mfg',
 """

