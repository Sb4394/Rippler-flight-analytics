import os
from datetime import timedelta, date
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import (StructType,
                               StructField,
                               DoubleType,
                               IntegerType,
                               StringType,
                               BooleanType)
import psycopg2
import io

def load_data(query):
    """
    Load data into database postgres
    """
    connection = psycopg2.connect(host= os.environ["host"], user= os.environ["Username"], password=os.environ["Password"])
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
 


# Selected holidays-- to be used for Nearest Holiday
holidays_2010 = [
    date(2010, 1, 1), date(2010, 1, 18), date(2010, 4, 4), date(2010, 5, 31),
    date(2010, 7, 4),  date(2010, 9, 6), date(2010, 10, 11), date(2010, 11, 11),
    date(2010, 11, 25), date(2010, 12, 25)
]
holidays_2011 = [
    date(2011, 1, 1), date(2011, 1, 17), date(2011, 4, 24), date(2011, 5, 30),
    date(2011, 7, 4),  date(2011, 9, 5), date(2011, 10, 10), date(2011, 11, 11),
    date(2011, 11, 24), date(2011, 12, 25)
]
holidays_2012 = [
    date(2012, 1, 1), date(2012, 1, 16), date(2012, 4, 8), date(2012, 5, 28),
    date(2012, 7, 4),  date(2012, 9, 3), date(2012, 10, 8), date(2012, 11, 11),
    date(2012, 11, 22), date(2012, 12, 25)
]
holidays_2013 = [
    date(2013, 1, 1), date(2013, 1, 21), date(2013, 3, 31), date(2013, 5, 27),
    date(2013, 7, 4),  date(2013, 9, 2), date(2013, 10, 14), date(2013, 11, 11),
    date(2013, 11, 28), date(2013, 12, 25)
]
holidays_2014 = [
    date(2014, 1, 1), date(2014, 1, 20), date(2014, 4, 20), date(2014, 5, 26),
    date(2014, 7, 4),  date(2014, 9, 1), date(2014, 10, 13), date(2014, 11, 11),
    date(2014, 11, 27), date(2014, 12, 25)
]
holidays_2015 = [
    date(2015, 1, 1), date(2015, 1, 19), date(2015, 4, 5), date(2015, 5, 25),
    date(2015, 7, 4),  date(2015, 9, 7), date(2015, 10, 12), date(2015, 11, 11),
    date(2015, 11, 26), date(2015, 12, 25)
]
holidays_2016 = [
    date(2016, 1, 1), date(2016, 1, 18), date(2016, 3, 27), date(2016, 5, 30),
    date(2016, 7, 4),  date(2016, 9, 5), date(2016, 10, 10), date(2016, 11, 11),
    date(2016, 11, 24), date(2016, 12, 25)
]
holidays_2017 = [
    date(2017, 1, 1), date(2017, 1, 16), date(2017, 4, 16), date(2017, 5, 29),
    date(2017, 7, 4),  date(2017, 9, 4), date(2017, 10, 9), date(2017, 11, 11),
    date(2017, 11, 23), date(2017, 12, 25)
]
holidays_2018 = [
    date(2018, 1, 1), date(2018, 1, 15), date(2018, 4, 1), date(2018, 5, 28),
    date(2018, 7, 4),  date(2018, 9, 3), date(2018, 10, 8), date(2018, 11, 11),
    date(2018, 11, 22), date(2018, 12, 25)
]
holidays_2019 = [
    date(2019, 1, 1), date(2019, 1, 21), date(2019, 4, 21), date(2019, 5, 27),
    date(2019, 7, 4),  date(2019, 9, 2), date(2019, 10, 14), date(2019, 11, 11),
    date(2019, 11, 28), date(2019, 12, 25)
] 
# utility function to loop thru each day in year
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
# lookup table that matches each date, for the years speicfied above, to the
# the number of days until the nearest holiday

hday_lookup = {}
for d in daterange( date(2010, 1, 1), date(2020, 1, 1) ):
    if d.year == 2010: holidays=holidays_2010
    if d.year == 2011: holidays=holidays_2011
    if d.year == 2012: holidays=holidays_2012
    if d.year == 2013: holidays=holidays_2013
    if d.year == 2014: holidays=holidays_2014
    if d.year == 2015: holidays=holidays_2015
    if d.year == 2016: holidays=holidays_2016
    if d.year == 2017: holidays=holidays_2017
    if d.year == 2018: holidays=holidays_2018
    if d.year == 2019: holidays=holidays_2019
    hday_lookup[d] = min( (float(abs(d- holiday).days) for holiday in holidays) )

def nearest_holiday(year, month, day):
    d = date(int(year), int(month), int(day))
    return hday_lookup[d]

# add a boolean column that indicates whether flight delayed or not (threshold 15 mins)
was_delayed_udf = udf(lambda x: float(x >= 15), DoubleType())

# convert hours, e.g. 1430 --> 14
get_hour_udf = udf(lambda x: float(x // 100), DoubleType())

# add column that indicates how close a flight is to a holiday
nearest_holiday_udf = udf(nearest_holiday, DoubleType())
if __name__ == "__main__":

    spark = SparkSession.builder \
        .master('local') \
        .appName('Flight Delay') \
        .getOrCreate()

    sc = spark.sparkContext
    # Pass AWS keys
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    # Use S3 streaming
    addr = "s3a://airlineddata/all_year/"


    flight_data = spark.read \
        .format('com.databricks.spark.csv') \
        .csv(addr,inferSchema='true', nanValue="", header='true', mode='PERMISSIVE')
    # flight_data = flight_data.na.drop()
    # there is a PR to accept multiple `nanValue`s, until then, however, the schema
    # must be manually cast (due to the way the DOT stores the data)
    flight_data = flight_data \
        .withColumn('Year', flight_data['Year'].cast('int')) \
        .withColumn('Month', flight_data['Month'].cast('Double')) \
        .withColumn('DayofMonth', flight_data['DayofMonth'].cast('Double')) \
        .withColumn('CRSDepTime', flight_data['CRSDepTime'].cast('Double')) \
        .withColumn('DayOfWeek', flight_data['DayOfWeek'].cast('Double')) \
        .withColumn('DepTime', flight_data['DepTime'].cast('Double')) \
        .withColumn('DepDelay', flight_data['DepDelay'].cast('Double')) \
        .withColumn('TaxiOut', flight_data['TaxiOut'].cast('int')) \
       .withColumn('TaxiIn', flight_data['TaxiIn'].cast('int')) \
        .withColumn('CRSArrTime', flight_data['CRSArrTime'].cast('int')) \
        .withColumn('ArrTime', flight_data['ArrTime'].cast('int')) \
        .withColumn('ArrDelay', flight_data['ArrDelay'].cast('int')) \
        .withColumn('Cancelled', flight_data['Cancelled'].cast('int')) \
        .withColumn('Diverted', flight_data['Diverted'].cast('int')) \
        .withColumn('CRSElapsedTime', flight_data['CRSElapsedTime'].cast('int')) \
        .withColumn('ActualElapsedTime', flight_data['ActualElapsedTime'].cast('int')) \
        .withColumn('AirTime', flight_data['AirTime'].cast('int')) \
        .withColumn('Distance', flight_data['Distance'].cast('Double')) \
        .withColumn('CarrierDelay', flight_data['CarrierDelay'].cast('int')) \
        .withColumn('WeatherDelay', flight_data['WeatherDelay'].cast('int')) \
        .withColumn('NASDelay', flight_data['NASDelay'].cast('int')) \
        .withColumn('SecurityDelay', flight_data['SecurityDelay'].cast('int')) \
        .withColumn('LateAircraftDelay', flight_data['LateAircraftDelay'].cast('int'))

    flight_data = flight_data \
        .dropna(subset=['DepDelay']) \
        .dropna(subset=['CRSDepTime']) \
        .filter(flight_data['Cancelled'] == 0)

    # add new udf computed columns
    flight_data = flight_data \
        .withColumn('Delayed', was_delayed_udf(flight_data['DepDelay'])) \
        .withColumn('CRSDepTime', get_hour_udf(flight_data['CRSDepTime'])) \
        .withColumn('HDays', nearest_holiday_udf(flight_data['Year'],
                                                 flight_data['Month'],
                                                 flight_data['DayofMonth']))

    # columns used in the predictive models
    cols = ['DepDelay', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'Distance', 'Reporting_Airline',
            'Origin', 'Dest', 'HDays', 'Delayed']

    # rename columns
    flights = flight_data \
        .select(*cols) 
      #  .withColumnRenamed('DepDelay', 'Delay') \
      #  .withColumnRenamed('CRSDepTime', 'Hour')

    print("Table before storing")
    base_data_path = '/home/ubuntu/Projects/flight-delay/data'
    if not os.path.exists(os.path.join(base_data_path, 'parquet')):
        os.makedirs(os.path.join(base_data_path, 'parquet'))
    flights.write.parquet(os.path.join(base_data_path, 'parquet', 'all_year.parquet'),
                                       mode='overwrite')

    url ='jdbc:postgresql://10.0.0.9:5432/'
    properties = {'user':os.environ["Username"],'password':os.environ["Password"],'driver':'org.postgresql.Driver'}
    flights.write.jdbc(url=url, table='flight_data',mode='overwrite',  properties=properties)
