
import requests
import json

import pandas as pd

un = 'colin.mcmillan@nrel.gov'
pw = 'CDXfrscm01!'
rid = "110070620208"

url = "https://frsqueryprd-api.epa.gov/facilityiptqueryprd/v1/FRS/QueryRegistry?"
params = {'UserID': un, "Password": pw, "registryId": rid}

r = requests.get(url, params=params)