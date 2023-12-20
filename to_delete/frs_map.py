# -*- coding: utf-8 -*-
"""

Plot map of FRS IDs in manufacturing industries

"""
import pandas as pd
import plotly.express as px

epa_id = pd.read_csv('EPA_REGISTRY_ID_NAICS_MFG.csv')


fig = px.scatter_mapbox(epa_id, lat="LATITUDE83", lon="LONGITUDE83",
                        hover_name="NAICS_CODES", hover_data=["PRIMARY_NAME"],
                        color_discrete_sequence=["blue"], zoom=3, height=300)

token = "pk.eyJ1IjoiY2Fycmllc2NobyIsImEiOiJjbDl5cmIxcDUwN2l1M29wOGk0ODBnODdkIn0.viiPZOcMJMmT6TnJy1Gyfw"

fig.update_layout(mapbox_style="mapbox://styles/carriescho/cl9zy9j2t000014qmnne58bbl",
                  mapbox_accesstoken=token) #, mapbox_accesstoken=token
    
#token = "pk.eyJ1IjoiY2Fycmllc2NobyIsImEiOiJjbDl6eDkweGswMnh3M3ZtdmtodzJ2ZTZ3In0.ozOH4cnZ-NdWo8n-aZbuxQ"    
#fig.update_layout(mapbox_style="mapbox://styles/mapbox/light-v10",
#                  mapbox_accesstoken=token) #, mapbox_accesstoken=token

fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

#https://docs.mapbox.com/api/maps/styles/