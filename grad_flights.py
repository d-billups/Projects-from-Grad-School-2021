#!/usr/bin/env python
# coding: utf-8

# For this problem, you will be working with flight data from the Bureau of Transportation Statistics. For development, these data are a little largeish (~700Mb/7 million flights) for prototyping, so two truncated files are provided under 'data/2007-005.csv' and 'data/2008-005.csv'. Important: it's strongly recommended to prototype and develop code using the truncated data.
# 
# Design note: The code you will develop as part of this problem's solution should be generalized so that it works when there are more than just two years' worth of data, i.e. when there are more files than just for years 2007 and 2008. Additionally, all refernce outputs are presented for the half-percent annual flight sample (small files).

# A1. (4 points) To start, complete the function to take a year as an input argument and load its data into a pandas dataframe. This load should drop the rows in the dataframe that have a null in any of these columns: "Year", "Month", "DayofMonth", "DepTime", "Origin", and "Dest", and then return the result

# In[1]:


# A1:Function(4/4)

import pandas as pd

def read_data(year):
    
    filename = "data/" + str(year) + "-005.csv" if True else ".csv"
    
    flight_yr = pd.read_csv(filename)
    
    flight_yr.dropna(subset=['Year', 'Month', 'DayofMonth', 'DepTime', 'Origin', 'Dest'],inplace=True)
  
    return flight_yr


# In[2]:


flight_07 = read_data(2007)
flight_08 = read_data(2008)


# In[4]:


flight_07.head()


# In[5]:


flight_08.head()


# Check to make sure that the specific columns listed do not contain nulls

# In[4]:


erase = ['Year', 'Month', 'DayofMonth', 'DepTime', 'Origin', 'Dest']


# In[42]:


for i in erase:
    print(i,":", flight_07[i].isnull().sum())


# In[43]:


for i in erase:
    print(i,":", flight_08[i].isnull().sum())


# A2. (7 points) Next, complete the new data-loading functions to create a new column in the dataframe that contains datetime objects holding the departure dates of the flights under a new column keyed as "DepartureDate".

# In[6]:


from datetime import date, datetime, timedelta


# In[38]:


def create_dep_datetime(row):
    
    #---your code starts here---
    
    year = row['Year']
    month = row['Month']
    day = row['DayofMonth']

    #---your code stops here---
    
    return datetime(year, month, day)


# Use this function to create 'DepartureDate'

# In[68]:


# A2:Function(4/7)

def read_data_parsetimes(year):
    depart = []
    
    filename = "data/" + str(year) + "-005.csv" if True else ".csv"
    
    flight_info = pd.read_csv(filename)
    flight_info.dropna(subset=['Year', 'Month', 'DayofMonth', 'DepTime', 'Origin', 'Dest'],inplace=True)
  
    
    for i in range(len(flight_info)):
        depart.append(create_dep_datetime(flight_info.iloc[i]))
        
    flight_info['DepartureDate'] = depart
    
    return flight_info


# In[72]:


read_data_parsetimes(2008).head()


# In[73]:


read_data_parsetimes(2007).head()


# A3. (5 points) Now complete the updated function that must also takes an airport code as an input argument. This should then return a dataframe of flights originating from that airport that occurred in the specified year.

# In[74]:


def read_data_parsetimes_byorigin(airport, year):
    
    depart = []
    
    filename = "data/" + str(year) + "-005.csv" if True else ".csv"
    
    flight_info = pd.read_csv(filename)
    flight_info.dropna(subset=['Year', 'Month', 'DayofMonth', 'DepTime', 'Origin', 'Dest'],inplace=True)
  
    
    for i in range(len(flight_info)):
        depart.append(create_dep_datetime(flight_info.iloc[i]))
        
    flight_info['DepartureDate'] = depart
    
    flight_info = flight_info.loc[(flight_info['Origin']==airport) & (flight_info['Year']==year)]
    
    return flight_info


# In[229]:


read_data_parsetimes_byorigin("PHL", 2008).head()


# A4. (4 points) Using this function, create dataframes holding the flight data for Philadelphia International Airport (PHL) for 2007 and 2008. Then use the .groupby() method to obtain the busiest month of the year for both years. Did this change from 2007 to 2008?

# In[230]:


month_07, count_07 = int(), int()

# try using .groupby() and .size() to create a count for months
# and then finding the month with highest number of flights.
#---your code starts here---

phl_2007 = read_data_parsetimes_byorigin("PHL", 2007)

month_count = phl_2007.groupby('Month').size()
#print(month_count) give the list of months with their counts

month_07, count_07 = month_count.idxmax(),  max(month_count)


month_07, count_07


# In[231]:


phl_2008 = read_data_parsetimes_byorigin("PHL", 2008)

month_count = phl_2008.groupby('Month').size()
#print(month_count)

month_08, count_08 = month_count.idxmax(),  max(month_count)


month_08, count_08


# In[41]:


# A4:Inline(1/4)

# Was the busiest month the same month for both years, 2007 and 2008?
# Answer one of: "Yes" or "No"
print("No")


# Will use this function in the final step of the larger fn

# In[175]:


def get_dates(data):

        month = data[0]
        day = data[1]
        year = data[2]

        return datetime(year, month, day)


# A5. (8 points) Finally, complete the updated function that takes two integer tripes of the form (month, day, year), each representing a date as either the start or end of a range (potentially more than two years long). The function must now return all flights originating from the specified airport within this range of time. If the range spans more than two years, it should load data from all necessary files (i.e., assume more than two year-files exist) and return a single dataframe containing all the data within the specified range of time.

# FINAL FUNCTION

# In[182]:


def read_data_parsetimes_byorigin_daterange(airport, start, end):
    depart = []
    years = []

    for i in range(start[-1], end[-1]+1): #grabs the years only from the start and end date
        years.append(i)

    flight_data = pd.DataFrame() #initialize dataframe to concatenate later

    for year in years: #loop through the files that contain the year(s) provided in start/end dates
        
        filename = "data/" + str(year) + ("-005.csv" if True else ".csv")
        
        flight_data_for_year = pd.read_csv(filename)
                
        flight_data = pd.concat([flight_data, flight_data_for_year])
        
        flight_data.dropna(subset=['Year', 'Month', 'DayofMonth', 'DepTime', 'Origin', 'Dest'],inplace=True)
        
        
    for i in range(len(flight_data)):
        depart.append(create_dep_datetime(flight_data.iloc[i]))
        
    flight_data['DepartureDate'] = depart
    
    #use get_date fn to change date tuples to datetime objects
    
    start = get_dates(start)
    end = get_dates(end)
    
    #last, filter out the flights between the start and end date that left from the specified airport
    
    flight_range = flight_data.loc[(flight_data['DepartureDate'] >= start) & (flight_data['DepartureDate'] <= end)\
                                  & (flight_data['Origin']==airport)]
    
    
    return flight_range


# In[233]:


read_data_parsetimes_byorigin_daterange("LAX",(1,17,2007),(2,3,2008))

#Can see how the interval is from the dates provided


# Using this function, get all the flight data for flights from PHL for 2007–2008. Then, create a daily count of flights over all of the days in the two years and report the busiest day over the two years.

# In[234]:


# A5:Inline(2/8)

busy_day, busy_count = int(), int() #initialize values

all_phl_07_08 = read_data_parsetimes_byorigin_daterange("PHL",(1,1,2007),(12,31,2008))

#group flights by their departure date, grab the counts, and sort from highest to lowest
sorted_flight_dts = all_phl_07_08.groupby('DepartureDate').size().sort_values(ascending=False)

#grab the element at the top of the list
busy_day, busy_count = sorted_flight_dts.index[0], sorted_flight_dts.values[0] 

busy_day, busy_count


# In[ ]:




