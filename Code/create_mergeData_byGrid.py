import os
import pandas as pd
from pandas import DataFrame
from datetime import datetime as dt
import matplotlib.pyplot as plt

# Imports the method used for connecting to DBs
from sqlalchemy import create_engine, inspect

# Allow us to declare column types
from sqlalchemy import Column, Integer, String, Float, Boolean 

from sqlalchemy.orm import Session

def createMergeFile(startDate, EndDate, weatherData, gridname):
    engine = create_engine("sqlite:///../Data/megawatt.sqlite")

    newdf = DataFrame(engine.execute("SELECT * FROM demand where date >'"+startDate+"'and date < '"+EndDate+"' and name='"+gridname+"'").fetchall())
    newdf = newdf.rename(columns={
        newdf.columns[0]: 'datetime',
        newdf.columns[1]: 'demand',
        newdf.columns[2]: 'abbr',
        newdf.columns[3] : 'datetime_utc',
        newdf.columns[4] : 'date',
        newdf.columns[5] : 'hour'
        })

    newdf['datetime'] = pd.to_datetime(newdf['datetime'])
    newdf['datetime_utc'] = pd.to_datetime(newdf['datetime_utc'])

    # weather_file = os.path.join('../Data','2016-2018_MA_weather_data.csv')
    weather_file = os.path.join('../Data',weatherData)
    raw_listings_df = pd.read_csv(weather_file, encoding="ISO-8859-1")

    raw_listings_df['datetime'] = pd.to_datetime(raw_listings_df['datetime'])
    raw_listings_df = raw_listings_df.rename(columns={
        'datetime':'datetime_utc'
    })

    del raw_listings_df['Unnamed: 0']

    whatdf = pd.merge(newdf, raw_listings_df, on='datetime_utc',how='inner')

    whatdf.to_csv('mergedSample.csv')

createMergeFile('2017-01-01','2018-12-01','2016-2018_MA_weather_data.csv','AZPS')
