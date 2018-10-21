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
        
        filepath = f"{station}-{year}.txt"

        try: 
            
           newdata = pd.read_fwf(filepath, colspecs= colspecs, names = names)
           weatherdata = weatherdata.append(newdata)    
           print(f'File {station}-{year} added ' + str(datetime.datetime.now().time()))     
           os.remove(filepath) # Only run once done to get rid of unneeded files
           
        except: 
            print(f'File {station}-{year} was NOT added ' + str(datetime.datetime.now().time()))   
print(weatherdata.head())
