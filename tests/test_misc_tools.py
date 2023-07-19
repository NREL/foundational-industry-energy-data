
import pandas as pd
from tools.misc_tools import FRS_API


def test_api():

    frs_api = FRS_API()

    # Get relevant data
    final_data_noid = pd.read_pickle('final_data.pkl')
    final_data_noid = final_data_noid.query(
        "eisFacilityID.isnull() & ghgrpID.isnull()", engine='python'
        )

    results = frs_api.find_unit_data_parallelized(
        final_data_noid.registryID.unique()
        )

    errors = []

    if not len(results) > 0:
        errors.append("FRS_API failed to return even None values")

    if not any(results):
        errors.append("FRS_API returned all None values")

    assert not errors, "errors occured:\n{}".format("\n".join(errors))
