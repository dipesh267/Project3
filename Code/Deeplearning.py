
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('matplotlib', 'inline')
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sklearn
import sklearn.datasets
from numpy.random import seed
from keras.models import Sequential
from keras.layers import Dense, Dropout
from datetime import datetime
seed(1)


# In[2]:


df=pd.read_csv("mergedSample.csv")
df.dropna(inplace=True)
df.head()


# In[3]:


# df['datetime_int']=
# datetime.strptime('2016-01-02 00:00:00')

timedelta = [i.total_seconds() for i in pd.to_datetime(df.datetime)-min(pd.to_datetime(df.datetime))]
df['datetime_delta'] = timedelta


# In[4]:


# Create Data
# X = df.tempf 
X = df[['tempf', 'datetime_delta']]
y = df.value


# In[5]:


model = Sequential()
model.add(Dense(20, input_dim=2))
model.add(Dense(1,activation='relu'))

model.compile(optimizer='adam',
              loss='mean_squared_error',
              metrics=['accuracy'])


# In[6]:


model.fit(X,y)


# In[7]:


model.summary()

