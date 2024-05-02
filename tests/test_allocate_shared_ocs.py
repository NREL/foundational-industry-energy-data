
from fied_compilation import allocate_shared_ocs
import pandas as pd


def test_allocate_shared_ocs():
    ghgrp_data_shared_ocs = pd.read_pickle('ghgrp_data_shared_ocs.pkl')
    nei_data_shared_ocs = pd.read_pickle('nei_data_shared_ocs.pkl')

    results_dict = dict()

    for dt in ['energy', 'ghgs']:
        results_dict[dt] = allocate_shared_ocs(ghgrp_data_shared_ocs, nei_data_shared_ocs, dt)

    energy_sum = results_dict['energy'].energyMJ.sum()
    ghgs_sum = results_dict['ghgs'].ghgsTonneCO2e.sum()

    og_energy = ghgrp_data_shared_ocs.energyMJ.sum()
    og_ghgs = ghgrp_data_shared_ocs.energyMJ.sum()

    assert (energy_sum, ghgs_sum) == (og_energy, og_ghgs)