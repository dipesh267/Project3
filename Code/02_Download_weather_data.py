# -*- coding: utf-8 -*-
"""
Created on Sun Oct 14 13:00:12 2018
@author: Josh B. and Shandiz M. 

Notes:
    
The code below reads in 3 years of hourly weather data from multiple 
weather stations in the U.S. 

We found a NOAA repository with weather data for all weather stations going back
decades. The data, however, is a specific folder structure, zipped (.gz files), 
and when unzipped is in fixed width format. Because the data is to voluminous, 
the code below:
    0.  Sets up directories to keep the files organized
    
    1.  Narrows to the candidate weather stations to:
        a. Stations in continental US
        b. Stations at international airports
        c. Stations that have data over analysis period
    
    2. Read in the files - loop over stations and years
        a. Send request for file
        b. Unzip
        c. Write to drive as txt file
    
    3. Read in and combine files - loop over stations and years
        a. Read in fixed width data
        b. Basic clean up
        c. Append to main dataset
        
    4. Fix date and time issues  
        
"""
import numpy as np
import pandas as pd
import urllib.request  
import io
import gzip
import datetime
import os
import matplotlib.pyplot as plt
plt.style.use('bmh')

# =============================================================================
# 00 Make sub directories (only run to set up initial directories)
# =============================================================================
# os.mkdir("Resources")
# os.mkdir("Resources/01-Historical_Demand")
# os.mkdir("Resources/02-Historical_Forecast_Demand")
# os.mkdir("Resources/03-Historical_Weather")
# os.mkdir("Resources/04-Forecast_Weather")

# =============================================================================
# 01 Narrow down to relevant weather stations
# =============================================================================

# A. Read in data
url = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"

stations = pd.read_csv(url)
stations.head()

# B.Narrow it down
stations = stations.loc[stations["CTRY"] == "US", : ]  # US only
stations = stations.loc[((stations["STATE"] != "HI") &
                        (stations["STATE"] != "AK")), : ]  # Continental US only
stations['airport'] = ((stations["STATION NAME"].str.contains('INTERNATIONAL AIRPORT')) |
        (stations["STATION NAME"].str.contains('REGIONAL AIRPORT')))

stations = stations.loc[stations.airport == True, :] # Narrow down to major airports
stations = stations.loc[stations['BEGIN'] <= 20150101,  : ]  # Narrow to sites with a good history
stations = stations.loc[stations['END'] > 20180930,  : ]  # Narrow to sites with current data

# C. Export CSV files with final list of weather stations
outfile = "../Data/Weather_station_list.csv"
stations.to_csv(outfile)

# =============================================================================
# 02 Download, UnZip data, and save fixed-with files
# =============================================================================

# A. Set up lists
stationlist = stations["USAF"].astype(str) + "-" + stations["WBAN"].astype(str) 
years = [2016, 2017, 2018]


# B. Unzip and save file 
for station in stationlist:
    for year in years:
        
        #station = "911820-22521"
        #year = 2018
        
        try: 
            
            # Update paths
            url = f'https://www1.ncdc.noaa.gov/pub/data/noaa/{year}/{station}-{year}.gz'
            outfilepath = f"../Data/{station}-{year}.txt"
    
            # Request File
            response = urllib.request.urlopen(url)
            compressed_file = io.BytesIO(response.read())
            
            #Unzip
            decompressed_file = gzip.GzipFile(fileobj=compressed_file)
            
            # Write to drive
            with open(outfilepath, 'wb') as outfile:
                outfile.write(decompressed_file.read())
                
            print(f'File {station}-{year} downloaded ' + str(datetime.datetime.now().time()))     
        
        except: 
            print(f'File {station}-{year} did NOT download' + str(datetime.datetime.now().time()))   
                
    
# =============================================================================
# 03 Read in fixed width data, parse, clean up, and append
# =============================================================================

# A. Empty dataframe to hold final weather data
weatherdata = pd.DataFrame([])            

# B. Columns specs and names for fixed with file
#    A fixed width file requires inputing the cut points
#    and, in this case, the column names.
#    Documentation: https://www1.ncdc.noaa.gov/pub/data/noaa/ish-format-document.pdf
      

colspecs = [(4,10), (10,15), (15, 23), (23,27), (60,63), (65,69), 
            (70,75), (87,92), (93,98)]
names = ["usaf", "wban", "date", "time", "windir", "windspeed",
         "skycond", "tempc", "dewpoint"]

# C. Read in fixed width files and add to dataset
for station in stationlist:
    for year in years:
        
        filepath = f"../Data/{station}-{year}.txt"

        try: 
            
           newdata = pd.read_fwf(filepath, colspecs= colspecs, names = names)
           weatherdata = weatherdata.append(newdata)    
           print(f'File {station}-{year} added ' + str(datetime.datetime.now().time()))     
           os.remove(filepath) # Only run once done to get rid of unneeded files
           
        except: 
            print(f'File {station}-{year} was NOT added ' + str(datetime.datetime.now().time()))   

# D. Clean up empty values (marked as 9999)
for var in ["windir", "windspeed", "skycond", "tempc", "dewpoint"]:
    weatherdata[var] = weatherdata[var].replace( {9999: np.nan}) 

# E. Convert temperature from Celcius to Fahrenheit 
weatherdata.tempc = weatherdata.tempc/10
weatherdata['tempf'] = 32 + (weatherdata.tempc * 9/5)                
weatherdata['dewpoint'] = 32 + (weatherdata.dewpoint/10 * 9/5)   

# =============================================================================
# 04 Fix Date/Time Issues and save 
# =============================================================================

#file = "../Data/2016-2018_Weather_all_stations.csv"
#weatherdata = pd.read_csv(file)
#weatherdata = weatherdata.loc[(weatherdata.wban%20 == 5), :] # sample for analysis


#A. Create a datetime variable

# Extract time components
weatherdata['time'] = weatherdata['time'].astype(str)
weatherdata['time'] = weatherdata['time'].str.pad(4, side = 'left', fillchar = "0" )
weatherdata['hh'] = weatherdata['time'].str.slice(start = 0, stop = 2)
weatherdata['mm'] = weatherdata['time'].str.slice(start = 2, stop = 4)

# Extract date components
weatherdata['date'] = weatherdata['date'].astype(str)
weatherdata['year'] = weatherdata['date'].str.slice(start = 0, stop = 4)
weatherdata['month'] = weatherdata['date'].str.slice(start = 4, stop = 6)
weatherdata['day'] = weatherdata['date'].str.slice(start = 6, stop = 8)

# Create date time variable and round to the nearest hour
weatherdata['datetime'] = (weatherdata.month + "-" +
           weatherdata.day + "-" + weatherdata.year + " " +
           weatherdata.hh + ":" + weatherdata.mm ) 

weatherdata.drop( columns = ['date', 'time', 'year', 'month', 'day', 
                             'hh', 'mm'], inplace = True) # Drop unneeded columns

weatherdata['datetime'] = pd.to_datetime(weatherdata.datetime)  # Convert to datetime object

#B.  Round to the nearest hour
weatherdata['datetime'] = weatherdata['datetime'].dt.round("H")

#C.  Remove duplicates
weatherdata.drop_duplicates(subset=['datetime', 'usaf', 'wban'], inplace= True )

#D. Check if we need to adjust weather for time zones and, if so, do so

oneday  = weatherdata.loc[((weatherdata['datetime'] > '2018-7-18 01:00:00') & 
                         (weatherdata['datetime'] < '2018-7-19 01:00:00') &
                         (weatherdata.usaf == 724880))] 
oneday.plot("datetime", "tempf")    
# Need to adjust for time zone
#weatherdata['datetime'] = weatherdata.datetime + pd.Timedelta(-7, unit='h')


##F. Extract additional time variables 
#weatherdata['year'] = weatherdata['datetime'].dt.year
#weatherdata['month'] = weatherdata['datetime'].dt.month
#weatherdata['dayofweek'] = weatherdata['datetime'].dt.dayofweek
#weatherdata['dayofyear'] = weatherdata['datetime'].dt.dayofyear
#weatherdata['date'] = weatherdata['datetime'].dt.date
#weatherdata['hour'] = weatherdata['datetime'].dt.hour   

#G. Check individual stations for missing data
#   If a station is missing a substantial amount of data, drop it. 

#G. Save - Need to decide is this is SQL file or csv
outfile = "../Data/2016-2018_Weather_all_stations_fixed.csv"
weatherdata.to_csv(outfile)


#H. Export Boston weather for testing
ma_weather = weatherdata.loc[weatherdata.usaf == 725090]
outfile = "../Data/2016-2018_MA_weather_data.csv"
ma_weather.to_csv(outfile)

