from __future__ import print_function
import sys, os

from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re,html
from pyspark.sql.functions import broadcast
import six
import psycopg2
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

#from config.config import *


def load_data(query):
    """
    Load data into database postgres
    """
    connection = psycopg2.connect(host= "ec2-3-230-34-56.compute-1.amazonaws.com", user= "postgres", password=os.environ["Password"])
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
 
def main(sc):
    """
    Grabbing files from s3
    """
    # Pass AWS keys
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    # Use S3 streaming
    addr = "s3a://airlineddata/sample.csv"
    df=spark_session.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(addr)
    df.take(1)
    df.show(5)
    df.cache()
    df.printSchema()
    df.describe().toPandas().transpose()
    
    correlation=[]
    for i in df.columns:
       if not( isinstance(df.select(i).take(1)[0][0], six.string_types)):
           print( "Correlation to Delay for ", i, df.stat.corr('Dep_delay',i))
           correlation.append(df.stat.corr('Dep_delay',i))
    print(correlation)

    query='''INSERT INTO flight_ml (Day,Day_week,Dep-time,Origin,Temp,Humidity,Cloud,Wind,Precipitation) VALUES (lr_model.coefficients[0]),lr_model.coefficients[1]),lr_model.coefficients[2]),lr_model.coefficients[3]),lr_model.coefficients[4]),lr_model.coefficients[5]),lr_model.coefficients[6]),lr_model.coefficients[7]),lr_model.coefficients[8]))'''
    load_data(query)

    vectorAssembler = VectorAssembler(inputCols = ['Day','Day_week','Dep_time','Origin','Temp','Humidity','Cloud','Wind','Precipitation'], outputCol = 'features')
    v_df = vectorAssembler.transform(df)
    v_df = v_df.select(['features', 'Dep_delay'])
    splits = v_df.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]
    lr = LinearRegression(featuresCol = 'features', labelCol='Dep_delay', maxIter=50, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(train_df)
    print("Coefficients: " + str(lr_model.coefficients))
    print("Intercept: " + str(lr_model.intercept))
    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)
    #v_df.show(3) 
    #query='''INSERT INTO flight_ml (Origin,Day,Month,Year,Temp,Humidity,Cloud,Wind,Precipitation) VALUES ( 'ORD', 1, 1,2016,76,25,80,45,0)'''
    #load_data(query)

if __name__ == '__main__':
    """
    Setup Spark session"""
    sc = SparkContext(conf=SparkConf().setAppName("fl"))  
    # Create spark session
    spark_session =SparkSession.builder.master("local").appName("fl").getOrCreate()
    sc = spark_session.sparkContext
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    # Run the main function
    main(sc)

