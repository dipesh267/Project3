# Dependencies
import os
import stations
import pandas as pd
from math import cos, asin, sqrt

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295     #Pi/180
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a)) 

def findBottomN(list1, N): 
    # https://www.geeksforgeeks.org/python-program-to-find-n-largest-elements-from-a-list/
    #list value example [723740-23194_110] 723740-23194 = id for api, 110 is distance
    final_list = [] 
  
    for i in range(0, N):  
        min1 = int(list1[0].split('_')[1])
        minName = list1[0].split('_')[0]
        for j in range(len(list1)):      
            if int(list1[j].split('_')[1])< min1: 
                min1 = int(list1[j].split('_')[1]) 
                minName = list1[j].split('_')[0]
        # print(minName+'_'+str(min1))
        list1.remove(minName+'_'+str(min1))
        # final_list.append(minName+'_'+str(min1)) 
        final_list.append(minName)
    return final_list

weather_file = os.path.join('../Data','Weather_station_list.csv')
weather_df = pd.read_csv(weather_file, encoding="ISO-8859-1")
weather_list = []

for index, row in weather_df.iterrows():
    temp_dict = {
        'id': str(row['USAF'])+'-'+str(row['WBAN']),
        'lat': row['LAT'],
        'lon': row['LON'],
        'state': row['STATE'],
        'icao': row['ICAO']
    }
    weather_list.append(temp_dict)

# print(weather_list[0]['id'])
# print(len(weather_list))

station_list = stations.stations
grid_weather = []
    
for station in station_list:  
    # longitute goes from -180 t0 180. Latitute from -90 to 90 
    grid_abbr = station[3]
    grid_lon = station[6]
    grid_lat = station[7]
    distance_list = []
    for weather_st in weather_list:

        w_lat = weather_st['lat']
        w_lon = weather_st['lon']

        distance_list.append(weather_st['id']+'_'+str(int(distance(grid_lat,grid_lon,w_lat,w_lon))))
        #distance_list.append(int(distance(grid_lat,grid_lon,w_lat,w_lon)))
    temp_dict = {
        grid_abbr: findBottomN(distance_list,5)
    }
    grid_weather.append(temp_dict)
# print(grid_weather)
# for grid in grid_weather:

f = open('closestStations.csv','w')
for grid in grid_weather:
    for key, value in grid.items():
        f.write(key+","+value[0]+","+value[1]+","+value[2]+","+value[3]+","+value[4])
        f.write('\n')    





