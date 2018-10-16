# Dependencies
import requests
import gridnames
import time
from datetime import datetime
import pandas as pd
from pprint import pprint

# Imports the method used for connecting to DBs
from sqlalchemy import create_engine, inspect

# Allow us to declare column types
from sqlalchemy import Column, Integer, String, Float, Boolean 

from sqlalchemy.orm import Session


station_names = gridnames.stations
#station_names = ['AVA']
engine = create_engine("sqlite:///../Data/megawatt.sqlite")

def pullAllDemand():
    grid_df = pd.DataFrame()

    for station in station_names:
        url = "http://api.eia.gov/series/?api_key=b2416f9351f1bc487d7de96e1a731aab&series_id=EBA."+station+"-ALL.D.H"
        api_call = requests.get(url)
        response = api_call.json()

        name_list = []
        date_list = []
        mg_list = []
                
        # for response in responses:
        name = response['series'][0]['series_id'] #name is format blah.name-blah.blah.blah
        names = name.split('-')  #names is ['blah.name','blah.blah.blah']
        name = names[0].split('.')[1] 
        demand_list = response['series'][0]['data']
        for demand in demand_list:
            if demand[0] != '' and demand[1] != '':
                name_list.append(name)
                date_list.append(demand[0])
                mg_list.append(demand[1])

        grid_df = pd.DataFrame({
            'name': name_list,
            'date': date_list,
            'demand': mg_list
        })
        grid_df['date'] = pd.to_datetime(grid_df['date'])
        #grid_df.to_csv('demand.csv')
        grid_df.to_sql('demand', con = engine, if_exists='append', index=False)

    return("all demand pull done!!")

def pullDayAheadDemand():
    dayahead_df = pd.DataFrame()

    for station in station_names:
        url = "http://api.eia.gov/series/?api_key=b2416f9351f1bc487d7de96e1a731aab&series_id=EBA."+station+"-ALL.DF.H"
        api_call = requests.get(url)
        response = api_call.json()

        name_list = []
        date_list = []
        mg_list = []
                
        # for response in responses:
        name = response['series'][0]['series_id'] #name is format blah.name-blah.blah.blah
        names = name.split('-')  #names is ['blah.name','blah.blah.blah']
        name = names[0].split('.')[1] 
        demand_list = response['series'][0]['data']
        for demand in demand_list:
            if demand[0] != '' and demand[1] != '':
                name_list.append(name)
                date_list.append(demand[0])
                mg_list.append(demand[1])

        dayahead_df = pd.DataFrame({
            'name': name_list,
            'date': date_list,
            'demand': mg_list
        })
        dayahead_df['date'] = pd.to_datetime(dayahead_df['date'])
        #grid_df.to_csv('demand.csv')
        dayahead_df.to_sql('day_ahead_demand', con = engine, if_exists='append', index=False)

    return("day ahead deman pull done!!")

print(pullAllDemand())
print(pullDayAheadDemand())