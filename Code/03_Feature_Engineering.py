# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 10:44:26 2018

@author: bodej
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
plt.style.use('bmh')

# =============================================================================
# 01 Read in Sample data and Adjust for Time Zone
# =============================================================================

#A. Read in file
filepath = "../Data/mergedSample.csv"
data = pd.read_csv(filepath)
data = data.rename(columns = {"value": "mw"})


#B. Adjust for time zone 
data['datetime'] = pd.to_datetime(data.datetime)  # Convert to datetime object
data.datetime.asfreq("H")  #  informs data is hourly
data['datetime'] = data.datetime.dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

#C. Summarize dataset info
data.dtypes
data.head()
data.describe()


# =============================================================================
# 02 Define features
# =============================================================================

#A. Extract additional time variables 

data['year'] = data['datetime'].dt.year
data['month'] = data['datetime'].dt.month
data['dayofweek'] = data['datetime'].dt.dayofweek
data['dayofyear'] = data['datetime'].dt.dayofyear
data['date'] = data['datetime'].dt.date
data['hour'] = data['datetime'].dt.hour   

#B. Extract peak and average info
dategroup = data.groupby(by = 'date')
data_ = dategroup.mw.mean()
data = data.join(data.groupby('date')['mw'].max(), on = 'date', rsuffix='_peak')
data = data.join(data.groupby('date')['tempf'].max(), on = 'date', rsuffix='_peak')
data = data.join(data.groupby('date')['tempf'].mean(), on = 'date', rsuffix='_avg')

#C. Extract lags for weather and MW
data['mw_3hrlag'] = data.mw.shift(3)
data['mw_24hrlag'] = data.mw.shift(24)
data['mw_weeklag'] = data.mw.shift(168)
data['tempf_3hrlag'] = data.tempf.shift(3)
data['tempf_24hrlag'] = data.tempf.shift(24)
data['tempf_weeklag'] = data.tempf.shift(168)

#D. Rolling averages of temperature
data['tempf_3hrma'] = data.tempf.rolling(3).mean()
data['tempf_24hrma'] = data.tempf.rolling(24).mean()
data['tempf_weeklag'] = data.tempf.rolling(168).mean()

# =============================================================================
# 03 Exploratory Plots
# =============================================================================

# A. Daily Peak versus Avg. Daily Temp
peak = data.groupby('date')['mw'].max()
avgtemp = data.groupby('date')['tempf'].mean()

sns.regplot(avgtemp, peak, lowess = True, color = (0,0.6,0.8, 0.6))
plt.ylabel('Daily Peak Demand')
plt.xlabel('Daily Avg. Temp')
plt.figure(figsize =[800, 200])
plt.show()

#B. Hourly load by temperature bins
#Crate max temp bins
bins = list(range(5,105,5))
names = []
for bin in bins:
    lb = bin-5
    name = f'{lb} to {bin}F' 
    print(name)
    names.append(name)
del names[0]   
   
bindata = data.loc[data.tempf_peak>=60].pivot_table(
        index = 'hour', columns = 'bins_maxtemp', values = 'mw', aggfunc = 'mean')


bindata.plot()
plt.xlabel("Hour of day")
plt.ylabel("Demand (MW)") 
plt.xticks(np.arange(0,25,3))
plt.show()

#C. Load duration curve

data['rank'] = data.mw.rank(method= 'first', ascending=False)
data['pctrank'] = data.mw.rank(method= 'first', ascending=False, pct = True)

data.loc[data.pctrank < 0.05].plot.line(x = 'pctrank', y = 'mw')
plt.xlabel("% of hours ranked from highest to lowest")
plt.ylabel("Demand (MW)") 
plt.show()
