
import pandas as pd

def split_scc(scc):
    """
    """
    scc_str = str(scc)

    scc_levels = {}

    if len(scc_str) == 8:

        scc_levels['l1'] = scc_str[0]
        scc_levels['l2'] = scc_str[1:2]
        scc_levels['l3'] = scc_str[3:5]
        scc_levels['l4'] = scc_str[6:7]

    if len(scc_str) == 10:

        scc_levels['l1'] = scc_str[0:1]
        scc_levels['l2'] = scc_str[2:3]
        scc_levels['l3'] = scc_str[4:6]
        scc_levels['l4'] = scc_str[7:9]

    else:
        scc_levels = {f'l{i}': None for i in range(0, 5)}

    return scc_levels


l1_energy = 


all_scc = {l: None for i in range(0, 5)}}

all_scc = {
    1 : {
        'name': ,
        'l_2': {

        }

        }
    }
 
 all_scc[l1][l2][l3][l4] = 40100101

 test = {
    1: {
        'name': 'Chemical Evaporation',
        '01': {
            'name': 'Organic Solvent',
            '001': {
                'name': 'Dry cleaning',
                '01': {
                    'name': 'bad stuff'
                    }
                }
            }
        }   
    }