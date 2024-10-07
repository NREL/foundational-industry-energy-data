
import pandas as pd
import requests
from pathlib import Path

def load_facs():
    """Load known existing facilities"""

    # 
    fac_url = 'https://zenodo.org/records/13381092/files/FRS_LC_facility_points_8_27.csv'

    fac_path = Path("../Data/FRS/FRS_LC_facility_points_8_27.csv")

    if fac_path.is_file():

        facs = pd.read_csv(fac_path)

    else:
    
        facs = pd.read_csv(
            fac_url, 
            usecols=['facility_latitude', 'facility_longitude',
                     'facility_place_name', 'facility_street_address', 'facility_city',
                     'facility_state', 'incomplete_address', 'cleaned_name','parcel_longitude',
                     'parcel_latitude'],
            low_memory=False
            )
        
        facs.to_csv(fac_path)

    return facs

def match_facs(facs, fied):
    """
    Match PNNL facs file to FIED
    
    Parameters
    ----------

    Returns
    -------
    """

    matched = pd.merge(
        fied,
        facs,
        left_on=['latitude', 'longitude'],
        right_on=['facility_latitude', 'facility_longitude'],
        how='inner'
        )
    
    return matched

def id_new_facs():
    """"""

    return facs

def id_old_facs():
    """"""

    return facs