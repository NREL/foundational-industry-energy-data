
import geopandas as gpd
import pandas as pd
import logging

from fied.datasets import (
    fetch_shapefile_census_block_groups,
    fetch_shapefile_congressional_district,
    fetch_shapefile_county,
    fetch_shapefile_NHDP,
    fetch_state_FIPS,
)

logging.basicConfig(level=logging.INFO)


class FiedGIS:
    
    def __init__(self):
        state_fips = pd.read_csv(fetch_state_FIPS(), sep="|", dtype={"STATE": str, "STUSAB": str})
        self._statefips = dict(state_fips[['STUSAB', 'STATE']].values)

    @staticmethod
    def get_shapefile(year=None, state_fips=None, ftype=None):
        """
        Get Census block group TIGER/Line shapefile for specified year 
        and state FIPS code, or get USGS HUC geodatabase. 

        Parameters
        ----------
        year : int
            Year of shapefile

        state_fips : str or None
            State FIPS of shapefile. Not necessary for congressional
            district.

        ftype : str, {'BG', 'CD', 'HUC'}
            Type of file to return. 'BG' == census block groups; 
            'CD' == congressional districts; 'HUC' == hydrolic unit code.

        Returns
        -------
        gf : geopandas.DataFrame
            gf

        """

        if ftype == "BG":
            gf = fetch_shapefile_census_block_groups(year, state_fips)
        elif ftype == "CD":
            gf = fetch_shapefile_congressional_district(year)

        elif ftype == "COUNTY":
            gf = fetch_shapefile_county(year)

        elif ftype == "HUC":
            gf = fetch_shapefile_NHDP()

        return gf

    @staticmethod
    def merge_coordinates_geom(fied_state, gf, ftype=None, data_source='fied'):
        """"
        First creates POINT geometry from facility coordinates. Then 
        locates the points within specific geographic identifier type. Finally,
        merges geographic identifier with facility DataFrame.

        Parameters
        ----------
        fied_state : pandas.DataFrame
            DataFrame of foundational data by state

        gf : geopandas
            Shapefile containing Census tracts

        ftype : str, {'BG', 'CD', 'COUNTY', 'HUC'}
            Type of file to return. 'BG' == census block groups; 
            'CD' == congressional districts; 'COUNTY' == county FIPS; 'HUC' == hydrolic unit code.

        data_source : str, {'fied', 'ghgrp'}
            Source of data with missing geographic identifiers.

        Returns
        -------
        matched_geo : pandas.DataFrame
            Geographic identifiers matched to facility coordinates.
        
        """

        crs = "EPSG:4269"

        col_fix = {
            'HUC': {
                'geocolumn': '',
                're_column': 'HUC'
                },
            'BG': {
                'geocolumn': 'GEOID',
                're_column': 'geoID'
                },
            'COUNTY': {
                'geocolumn': 'GEOID',
                're_column': 'COUNTY_FIPS'
                },
            'CD': {
                'geocolumn': 'GEOID',
                're_column': 'legislativeDistrictNumber'
                }
            }
        
        try:
            geometry = gpd.points_from_xy(
                fied_state.longitude, fied_state.latitude, 
                crs=crs
                )
            
        except AttributeError:
            geometry = gpd.points_from_xy(
                fied_state.LONGITUDE, fied_state.LATITUDE, 
                crs=crs
                )

        gdf = gpd.GeoDataFrame(fied_state, crs=crs, geometry=geometry)

        matched_geo= gpd.sjoin(
            gdf, gf[[col_fix[ftype]['geocolumn'], 'geometry']], how='left', 
            predicate='within'
            )
        
        matched_geo.rename(
            columns={col_fix[ftype]['geocolumn']: col_fix[ftype]['re_column']},
            inplace=True
            )
        
        if data_source == 'fied':
            drop_cols =  ['geometry', 'latitude', 'longitude', 'index_right']

        elif data_source == 'ghgrp':
            drop_cols =  ['geometry', 'LATITUDE', 'LONGITUDE', 'index_right']

        matched_geo.drop(drop_cols, axis=1, inplace=True)

        
        return matched_geo

    def merge_geom(self, df, year=None, ftypes=['BG', 'CD'], data_source='fied'):
        """
        Pulls together methods for creating Geopandas DataFrames from 
        geographic information files and merges geographic identifiers
        with the foundational data set.

        Parameters
        ----------
        df: pandas.DataFrame
            DataFrame with missing geographic data.

        year : int
            Year of foundational energy data.

        ftype : list; default=['BG', 'CD']
            Type of missing geo data to fill in.

        data_source : str, {'fied', 'ghgrp'}
            Source of missing geographic data. Used to specify
            columns in dataframe to use.

        Returns
        -------
        new_fied : pandas.DataFrame
            New foundational dataset with new columns 
            for geographic identifiers.

        """

        geo_data = pd.DataFrame()

        if data_source == 'fied':
            state_col = 'stateCode'
            data_cols = ['registryID', 'latitude', 'longitude']
            fac_id = 'registryID'

        elif data_source == 'ghgrp':
            state_col = 'STATE'
            data_cols = ['FACILITY_ID', 'LATITUDE', 'LONGITUDE']
            fac_id = 'FACILITY_ID'


        for state in df[state_col].unique():

            geo_data_state = pd.DataFrame()
    
            try:
                state_fips = self._statefips[state]

            except KeyError:
                continue

            df_state = pd.DataFrame(
                df.query(f"{state_col}==@state")[data_cols]
                )
            
            df_state.drop_duplicates(inplace=True)

            for t in ftypes:

                logging.info(f'Finding {t} for {state}')

                gf = FiedGIS.get_shapefile(
                    year=year, state_fips=state_fips,
                    ftype=t
                    )
                
                matched = FiedGIS.merge_coordinates_geom(
                    fied_state=df_state,
                    gf=gf,
                    ftype=t,
                    data_source=data_source
                    )

                geo_data_state = pd.concat(
                    [geo_data_state, matched.set_index(fac_id)], axis=1
                    )
            
            geo_data = geo_data.append(geo_data_state)

        if 'HUC' in ftypes:
            gf = FiedGIS.get_shapefile(year=year, ftype='HUC')
            hucs = FiedGIS.merge_coordinates_geom(df, gf, ftype='HUC')
            geo_data = pd.merge(
                geo_data, hucs,
                left_index=True, right_index=True, how='left'
                )

        else:
            pass

        if 'legislativeDistrictNumber' in df.columns:
            df.drop(['legislativeDistrictNumber'], axis=1, inplace=True)

        else:
            pass

        df = pd.merge(
            df, geo_data, left_on=fac_id, 
            right_index=True, how='left'
            )
        
        return df
