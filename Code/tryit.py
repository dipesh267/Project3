import numpy as np
import pandas as pd
import urllib.request  
import gzip
import io
import datetime
import os

weather_file = os.path.join('../Data','closestStations.csv')
weather_df = pd.read_csv(weather_file, encoding="ISO-8859-1")

# A. Set up lists
st_data_fetch = []
for index, row in weather_df.iterrows():
    if not(row['ID1'] in st_data_fetch):
        st_data_fetch.append(row['ID1'])
    if not(row['ID2'] in st_data_fetch):
        st_data_fetch.append(row['ID2'])
    if not(row['ID3'] in st_data_fetch):
        st_data_fetch.append(row['ID3'])
    if not(row['ID1'] in st_data_fetch):
        st_data_fetch.append(row['ID4'])
    if not(row['ID1'] in st_data_fetch):
        st_data_fetch.append(row['ID5'])
    
years = [2016]


# B. Unzip and save file 
for station in st_data_fetch:
    for year in years:
        
        #station = "911820-22521"
        #year = 2018
        
        try: 
            
            # Update paths
            url = f'https://www1.ncdc.noaa.gov/pub/data/noaa/{year}/{station}-{year}.gz'
            # outfilepath = f"../Data/{station}-{year}.txt"
            outfilepath = f"{station}-{year}.txt"
            # Request File
            response = urllib.request.urlopen(url)
            compressed_file = io.BytesIO(response.read())
            #Unzip
            decompressed_file = gzip.GzipFile(fileobj=compressed_file)
            # Write to drive
            with open(outfilepath, 'wb') as outfile:
                outfile.write(decompressed_file.read())
                
            # print(f'File {station}-{year} downloaded ' + str(datetime.datetime.now().time()))     
        
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
for station in st_data_fetch:
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

#G. Save - Need to decide is this is SQL file or csv
outfile = "../Data/2016-2018_Weather_all_stations_fixed.csv"
weatherdata.to_csv(outfile)


#H. Export Boston weather for testing
ma_weather = weatherdata.loc[weatherdata.usaf == 725090]
outfile = "../Data/2016-2018_MA_weather_data.csv"
ma_weather.to_csv(outfile)



