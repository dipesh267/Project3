# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 18:14:22 2018

@author: bodej
"""


#Import folium for maps and pandas for data wrangling
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
plt.style.use('bmh')

#====================================================
# 1. Read data
#====================================================

path_geo = "../Data/Control_Areas/Control_Areas.shp"
shapes = gpd.read_file(path_geo)
areas = pd.read_csv("../Data/operating_system_names.csv")

# =============================================================================
#  2. Extract the lat and long
# =============================================================================

shapes['lat'] = shapes.centroid.x
shapes['lon'] = shapes.centroid.y

# =============================================================================
#  3. Remove extra columns and merge to IDS
# =============================================================================

#Drop extra columns
shapes.drop(columns = ['geometry', 'SOURCEDATE', 'VAL_METHOD', 'VAL_DATE',
                       'NAICS_CODE', 'PLAN_OUT', 'UNPLAN_OUT', 
                       'OTHER_OUT', 'YEAR', 'SHAPE__Len'], inplace = True)

varlist = ["PEAK_MONTH", "AVAIL_CAP", "TOTAL_CAP", "PEAK_LOAD", "MIN_LOAD"]

for var in varlist:
    shapes[var] = shapes[var].replace( {-999999: np.nan}) 

outpath =  ("../Data/operating_system_names_and_geoinfo.csv")
shapes.to_csv(outpath)

