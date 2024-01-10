# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:14:53 2019

@author: cmcmilla
"""
import json
import requests
import numpy as np
import sys
import logging


def fipfind(data_directory, f, missingfips):
    """
    Match missing FIPS with facility coordinates using FCC API.

    Parameters
    ----------
    data_directory : str
        Directory for 

    f :

    missingfips :


    Returns
    -------
    fipfound : 
    """

    logging.basicConfig(level=logging.INFO)

    z2f = json.load(open(data_directory + '/zip2fips.json'))

    if np.isnan(missingfips.loc[f, 'LATITUDE']) == False:

        lat = missingfips.loc[f, 'LATITUDE']
        lon = missingfips.loc[f, 'LONGITUDE']

        payload = {
            'format': 'json', 'latitude': lat, 'longitude': lon,
            'showall': 'True', 'censusYear': 2010
            }

        try:
            r = requests.get(
                'https://geo.fcc.gov/api/census/block/find?',  # updated URL to https://
                params=payload
                )

        except requests.HTTPError as error:
            e = sys.exc_info()[0]

            logging.error(f'Error: {error}\n{e}\nLat, lon: {lat}, {lon}')

        if r.ok:

            if r.json()['County']['FIPS'] == None:

                fipfound = 0

            else:

                fipfound = r.json()['County']['FIPS']

        else:
            logging.error(f'Status code: {r.status_code}\nLatitude: {lat}\nLongitude: {lon}')

            fipfound = 0

        return fipfound

    if ((missingfips.loc[f, 'ZIP'] > 1000)

        & (np.isnan(missingfips.loc[ f, 'COUNTY_FIPS'])==True)

        & (str(missingfips.loc[f, 'ZIP']) in z2f)):

        fipfound = int(z2f[str(missingfips.loc[f,'ZIP'])])

        return fipfound

    else:

        fipfound = 0

    return fipfound
