import pandas as pd
from datetime import datetime
def listOfTuples(l1, l2):
    return list(map(lambda x, y:(x,y), l1, l2))
dataframe = pd.read_csv("Jan_2016_flights.csv",usecols=[8,1,4,6,7])
dataframe["DEP_TIME"]=dataframe["DEP_TIME"].fillna(dataframe["CRS_DEP_TIME"])
dataframe["DEP_TIME"]=dataframe["DEP_TIME"].apply(lambda x: int((x/100) +1) if(x%100 >30) else int(x/100))
dataframe['period'] = dataframe["FL_DATE"].astype(str) + " "+ (dataframe["DEP_TIME"]%24).astype(str).str.zfill(2)+":00:00"
lat=pd.read_csv("lat_long.csv")
lat["Lat"]=lat["Lat"].apply(lambda x : round(x,6))
lat['location']=lat["Lat"].astype(str)+ ","+lat["Lon"].astype(str)
location_data=dict(zip(lat["Airport"].tolist(),lat["location"].tolist()))  #pass in the airport code to get the location
all_flight=listOfTuples(dataframe['period'],dataframe['ORIGIN'])
weather = pd.read_csv("weather_exported.csv",usecols=[0,1,5,9,10,12,15,17])
weather['DateHrGmt']=weather['DateHrGmt'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'))
weather["inputLatitude"]=weather["inputLatitude"].apply(lambda x : round(x,6))
weather['location']=weather["inputLatitude"].astype(str)+ ","+weather["inputLongitude"].astype(str)
weather["all"]=weather["DateHrGmt"].astype(str)+ " "+weather["location"].astype(str)
weather_data=dict(zip(weather["all"].astype(str).tolist(),zip(weather["SurfaceWetBulbTemperatureFahrenheit"].tolist(),weather["RelativeHumidityPercent"].tolist(),weather["CloudCoveragePercent"].tolist(),weather["WindSpeedMph"].tolist(),weather["PrecipitationPreviousHourInches"].tolist())))
dataframe["location"]='0'
for i in range(len(dataframe)):
    if dataframe["ORIGIN"][i] in location_data:
        dataframe.at[i,"location"]=location_data[dataframe["ORIGIN"][i]]
dataframe["temp"]='0'
dataframe["Humidity"]='0'
dataframe["Cloud"]='0'
dataframe["Wind"]='0'
dataframe["Percipitation"]='0'
for i in range(len(dataframe)):
    if dataframe["ORIGIN"][i] in location_data:
        dataframe.at[i,"temp"]=weather_data[dataframe["period"][i]+" " +dataframe["location"][i]][0]
        dataframe.at[i, "Humidity"] = weather_data[dataframe["period"][i] + " " + dataframe["location"][i]][1]
        dataframe.at[i, "Cloud"] = weather_data[dataframe["period"][i] + " " + dataframe["location"][i]][2]
        dataframe.at[i, "Wind"] = weather_data[dataframe["period"][i] + " " + dataframe["location"][i]][3]
        dataframe.at[i, "Percipitation"] = weather_data[dataframe["period"][i] + " " + dataframe["location"][i]][4]

df = pd.DataFrame(columns = ['Day', 'Month','Year','Time','Departure','Temperature','Humidity','Cloud','Wind','Percipitation','Delay'])
for i in range(len(dataframe)):
    df.at[i,'Year']=dataframe['FL_DATE'][i][:4]
print(df)

dataframe["day_week"]='0'
airport = {'BOS': 1,'DEN': 2, 'DFW':3,'EWR':4,'IAH':5,'JFK':6,'LGA':7,'ORD':8,'PHL':9,'SFO':10} 
  
# traversing through dataframe 
# Gender column and writing 
# values where key matches 
dataframe.ORIGIN = [airport[item] for item in dataframe.ORIGIN]
for i in range(len(dataframe)):
    dataframe.at[i,"day_week"]=dataframe["Day"][i]%7

"""for i in range(len(dataframe)):
    if pd.isnull(dataframe["Dep_delay"][i]):
        dataframe=dataframe.drop([i],axis=0)"""
#dataframe.to_csv("required_data.csv")
