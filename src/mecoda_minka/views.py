#!/usr/bin/env python3

"""
Examples of views made with folium using dataframe from get_dfs method.
"""

import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
import numpy as np

def create_heatmap(df):
    df.dropna(subset = ['latitude', 'longitude'], inplace = True)

    lats = df['latitude'].to_list()
    lons = df['longitude'].to_list()

    locations = list(zip(lats, lons))

    attr = (
        'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
    )
    tiles = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    
    #Define coordinates of where we want to center our map
    center = [np.mean(lats), np.mean(lons)]
    
    map = folium.Map(center, tiles=tiles, attr=attr, zoom_start=5)

    HeatMap(locations).add_to(map)

    return map


def create_markercluster(df):
    df.dropna(subset = ['latitude', 'longitude'], inplace = True)

    lats = df['latitude'].to_list()
    lons = df['longitude'].to_list()

    locations = list(zip(lats, lons))

    #Define coordinates of where we want to center our map
    center = [np.mean(lats), np.mean(lons)]
    
    m = folium.Map(location=center, tiles="cartodb positron", zoom_start=1)

    marker_cluster = MarkerCluster().add_to(m)

    for i in range(len(df)):
        folium.Marker(
            location=locations[i],
            popup=f"Id: {df['id'].values[i]}\n Especie:{df['taxon_name'].values[i]}",
            #icon=folium.Icon(color="green", icon="ok-sign"),
            icon=folium.Icon(color="green", icon='bug', prefix='fa'),
        ).add_to(marker_cluster)

    return m