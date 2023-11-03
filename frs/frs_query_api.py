
import requests
import json

import pandas as pd

un = 'colin.mcmillan@nrel.gov'
pw = 'CDXfrscm01!'
rid = "110037599042"

url = "https://frsqueryprd-api.epa.gov/facilityiptqueryprd/v1/FRS/QueryRegistry?"
params = {'UserID': un, "Password": pw, "registryId": rid}

r = requests.get(url, params=params)

    # "programSystemAcronym": "AIRS/AFS",
    # "programSystemId": "2210300069"
p2 = {'registry_id': rid}
r2 = requests.get('https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facility_wbd?', params=p2)