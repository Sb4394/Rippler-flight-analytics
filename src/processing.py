from __future__ import print_function
import sys, os

from pyspark import SparkConf, SparkContext, SparkFiles,HiveContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_timestamp

from pyspark.sql.functions import *
from pyspark.sql.types import *
#import pyspark.sql.functions as f
#from pyspark.sql import functions as F
import re,html
from pyspark.sql.functions import broadcast
import six
import psycopg2
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as W
#from pyspark.sql.functions import datediff


def main(sc):
    """
    Grabbing files from s3
    """
    # Pass AWS keys
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    # Use S3 streaming
    addr =  "s3a://airlineddata/2018.csv"
    flight_data = spark_session.read \
        .format('com.databricks.spark.csv') \
        .csv(addr, inferSchema='true', nanValue="", header='true', mode='PERMISSIVE')
    flight_data.cache()
    # cast the columns
    flight_data = flight_data \
        .withColumn('Year', flight_data['Year'].cast('int')) \
        .withColumn('Month', flight_data['Month'].cast('int')) \
        .withColumn('DayofMonth', flight_data['DayofMonth'].cast('int')) \
        .withColumn('DepTime', flight_data['DepTime'].cast('int')) \
        .withColumn('Origin', flight_data['Origin'].cast("string"))
    # pad 0 in front of the hhmm. ex: 834 as 0834 implies 08:34
    cols = ["Year","Month","DayOfWeek","DayofMonth","DepTime","Origin","Dest","CRSElapsedTime","CRSDepTime","Distance","DepDelay","Reporting_Airline","WheelsOff","WheelsOn"]  
    flight_data=flight_data.select(*cols)
    flight_data=flight_data.na.drop(subset=["DepTime"])

    flight_data=flight_data.withColumn('DepTime',lpad(flight_data['DepTime'],4,'0').alias('Deptime'))
    # merge date and time
    flight_data = flight_data.withColumn("merge", concat_ws(" ", col("Year"), col("Month"), col("DayofMonth"), col("DepTime"))).withColumn("period", to_timestamp("merge", "yyyy MM dd HHmm")).drop("merge")
    #read weather files
    add =  "s3a://2018weather/"
    weather = spark_session.read \
        .format('com.databricks.spark.csv') \
        .csv(add, inferSchema='true', nanValue="", header='true', mode='PERMISSIVE')
    weather.cache()
    #drop null rows
    weather=weather.na.drop()
    weather=weather.filter(weather.tmpf != 'M')
    weather=weather.withColumn("periodw", to_timestamp("valid", "yyyy-MM-dd HH:mm"))
    #join weather and flight data
    joinedDF = flight_data.join(weather,col("Origin") == col("station"),"inner")
    additional_cols = joinedDF.withColumn("time_diff",  abs(unix_timestamp(col("period")) - unix_timestamp(col("periodw"))))
    #rank based on the time_diff and take the first row only
    partDf = additional_cols.select("Year","Month","DayOfWeek","DayofMonth","DepTime","Origin","Dest","period","periodw","CRSElapsedTime","station","CRSDepTime","Distance","DepDelay","Reporting_Airline","WheelsOff","WheelsOn","tmpf","dwpf","relh","sknt", W.row_number().over(Window.partitionBy("Origin","DepTime").orderBy("time_diff") ).alias("rank") ).filter(col("rank") == 1)
    flight_data.createOrRepalceTempView("flight")
    weather.createOrReplaceTempView("weather")
    joined_df=spark_session.sql("""SELECT 
                    flight.*, 
                    wth.tmpf, 
                    wth.dwpf, 
                    wth.relh, 
                    wth.sknt 
                    FROM flight
                    LEFT JOIN weather as wth on
                    flight.Origin = wth.station AND 
                    wth.period =(  
                                   select top 1 period
                                    from weather as wth2
                                    where wth2.station = wth.station
                                    order by abs(datediff(mi, flight.period,wth2.period))
                                        )
                       """)
    joined_df.write.jdbc(url = "jdbc:postgresql://"+os.environ["POSTGRES_URL"]+":5432/flight-data", 
                          table = "marcos",
                          mode = "append",
                          properties={"driver": 'org.postgresql.Driver',
                                      "user": os.environ["POSTGRES_USER"],
                                      "password": os.environ["POSTGRES_PASSWORD"]}) 


if __name__ == '__main__':
    """
    Setup Spark session
    """
    #sc = SparkContext(conf=SparkConf().setAppName("fl"))
    # Create spark session
    spark_session =SparkSession.builder.appName("fl").getOrCreate()
    sc = spark_session.sparkContext
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    # Run the main function
    main(sc)
  



