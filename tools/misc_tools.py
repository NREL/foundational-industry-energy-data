
import requests
import json
import logging
import concurrent.futures
import numpy as np

logging.basicConfig(level=logging.INFO)


class FRS_API:
    def __init__(self, huc_only=True) -> None:

        if huc_only:
            pass

        else:
            def load_query_credentials():
                """
                Loads user ID and password for
                accessing EPA FRS Query API. Not necessary to
                retrieve missing HUC codes.
                """

                with open('c:/users/cmcmilla/Documents/API_auth.json') as f:
                    credentials = json.load(f)["epa_frs_API"]

                return credentials

            self._cred = load_query_credentials()

        self._base_urls = {
            'frs_query_prog': 'https://frsqueryprd-api.epa.gov/facilityiptqueryprd/v1/FRS/QueryProgramFacility?',
            'wbd_api': 'https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facility_wbd?',
            'frs_api': 'https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities?',
            'frs_query_emm': 'https://frsqueryprd-api.epa.gov/facilityiptqueryprd/v1/FRS/QueryEmissionsUnit?'
            }

    def find_huc(self, registryID, huc='HUC_8'):
        """
        Calls EPA API for finding USGS Watershed Boundary Dataset information 
        for a passed FRS ID. See https://www.epa.gov/frs/frs-rest-services#get_facilities_wbd

        Parameters
        ----------
        registryID : int 
            Facility Registry Service ID

        Returns
        -------
        hucCode : str
            Hydrolic unit code. Can be [f'HUC_{n}' for n in range(2, 14, 2)]
        """

        huc_name = {
            "HUC_2": "Region",
            "HUC_4": "Subregion",
            "HUC_6": "Basin",
            "HUC_8":  "Subbasin",
            "HUC_10": "Watershed",
            "HUC_12": "Subwatershed"
            }

        params = {
            'registryID': registryID,
            'output': 'JSON'
            }

        url = self._base_urls['wbd_api']

        r = requests.get(url, params=params)

        try:
            hucCode = dict(
                registriyID=r.json()['Results'][huc_name[huc]][huc]
                )

        except json.JSONDecodeError:
            logging.error(f"ERROR: {r.content}")

            hucCode = None

        except KeyError:
            logging.error(f"No {huc_name[huc]} for {registryID}?")

            hucCode = None

        except (ConnectionError, ConnectionResetError):
            logging.error(f"{r.content}")

            hucCode = None

        return hucCode

    def parallelize_api(self, method, data):
        """
        Method for parallelizing API call methods

        Parameters
        ----------
        method :
            API call method

        data :
            Data to pass to mthod

        Returns
        -------
        results : list
            List of results from API calls
        """

        results = []

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=100)

        for result in executor.map(method, data):
            results.append(result)

        return results

    def find_huc_parallelized(self, final_data):
        """
        Parallelized API call to get HUC codes
        based on FRS Registry IDs

        Parameters
        ----------
        final_data : pandas.DataFrame

        Returns
        -------
        results : list
            List of dictionaries with registryID, HUC as
            key, value pairs
        """

        # Need to make sure registryIDs are int
        ids_missing_huc = final_data.query(
            "hucCode8.isnull()", engine="python"
            ).registryID.unique().astype(np.int64)

        results = self.parallelize_api(self.find_huc, ids_missing_huc)

        return results

    def find_facility_program_data(self, registryID):
        """"
        Get basic facility program data from EPA's 
        Facility Registry Service (FRS) API.

        Parameters
        ----------
        registryID : int
            Facility ID

        Returns
        -------
        data : json

        """

        params = {
            'registry_id': registryID,
            'output': 'JSON',
            'program_output': 'yes'
            }

        url = self._base_urls['frs_api']

        r = requests.get(url, params=params)

        try:
            data = r.json()

        except json.JSONDecodeError:
            data = None

        return data

    def query_program_facility(self, registryID):
        """
        Use EPA FRS Query API (requires registration 
        for user ID and password)

        Parameters
        -----------
        registryID : int
            FRS Registry ID

        Returns
        -------
        program_data : dict
        """

        url_base = self._base_urls['frs_query_prog']

        url_fac = f'{url_base}registryID={registryID}'

        headers = self._cred
        headers['accept'] = 'application/json'

        r = requests.get(url_fac, headers=headers)

        program_data = {}

        try:
            raw_data = r.json()

        except json.JSONDecodeError:
            program_data = None

        else:
            for p in raw_data:
                program_data[p['programSystemAcronym']] = p['programSystemId']

        return program_data

    def query_emissions_unit(self, acroynm, id):
        """

        Parameters
        ----------
        acronym : str

        id : str

        Returns
        -------
        emissions_data : dict
        """

        url_base = self._base_urls['frs_query_emm']

        url_unit = \
            f'{url_base}programSystemAcronym={acronym}&programSystemId{id}'

        # headers = self.load_query_credentials()
        headers = self._cred
        headers['accept'] = 'application/json'

        r = requests.get(url_unit, headers=headers)

        try:
            raw_data = r.json()[0]

        except json.JSONDecodeError:
            emissions_data = None

        else:
            emissions_data = dict(acronym=raw_data)

        return emissions_data

    def find_unit_data(self, registryID):
        """
        Calls methods for finding program data and then
        associated emissions unit data for a given Registry ID

        Parameters
        ----------
        registryID : int
            FRS Registry ID

        Returns
        -------
        unit_data : dict

        """
        program_data = self.query_program_facility(registryID)

        if program_data:
            data_list = [self.query_emissions_unit(k, v) for k, v in program_data.items()]

        else:
            return None

        try:
            unit_data = dict(registryID=data_list[0])

        except IndexError:
            logging.error(f"No unit data for {registryID}?")
            unit_data = None

        return unit_data

    def find_unit_data_parallelized(self, final_data):
        """
        Makes API calls to find program data and then
        emissions unit data for Registry IDs.

        Parameters
        ----------
        final_data : pandas.DataFrame

        Returns
        -------
        results : list
            list of dictionaries.
        """

        final_data_noid = final_data.query(
            "eisFacilityID.isnull() & ghgrpID.isnull()", engine='python'
            ).registryIDs.unique().astype(np.int64)

        results = self.parallelize_api(self.find_unit_data, final_data_noid)

        return results
