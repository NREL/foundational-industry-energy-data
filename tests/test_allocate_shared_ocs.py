
from fied_compilation import allocate_shared_ocs
import pandas as pd
import numpy as np


def test_allocate_shared_ocs():
    ghgrp_data_shared_ocs = pd.read_pickle('ghgrp_data_shared_ocs.pkl')
    nei_data_shared_ocs = pd.read_pickle('nei_data_shared_ocs.pkl')

    results_dict = dict()

    for dt in ['energy', 'ghgs']:
        results_dict[dt] = allocate_shared_ocs(ghgrp_data_shared_ocs, nei_data_shared_ocs, dt)

    energy_sum = np.around(results_dict['energy'].energyMJ.sum(), 0)
    ghgs_sum = np.around(results_dict['ghgs'].ghgsTonneCO2e.sum(), 0)

    og_energy = np.around(ghgrp_data_shared_ocs.energyMJ.sum(), 0)
    og_ghgs = np.around(ghgrp_data_shared_ocs.energyMJ.sum(), 0)

    assert (energy_sum, ghgs_sum) == (og_energy, og_ghgs)