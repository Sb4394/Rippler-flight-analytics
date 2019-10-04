import os
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local[3]') \
        .appName('Flight Delay') \
        .getOrCreate()

    # read in the pre-processed DataFrame from the parquet file
    base_dir = '/home/ubuntu/Projects/flight-delay/data/parquet'
    flights_df = spark.read.parquet(os.path.join(base_dir, '2015.parquet'))

    print('Table before Encoding')
    flights_df.show(5)
    print("Total number of rows: %d" % flights_df.count())
    #DepDelay|Month|DayofMonth|DayOfWeek|CRSDepTime|Distance|Reporting_Airline|Origin|Dest|HDays|Delayed|
    # categorical columns that will be OneHotEncoded
    cat_cols = ['Month','DayOfWeek', 'DayofMonth','Reporting_Airline','Origin','Dest']

    # numeric columns that will be a part of features used for prediction
    non_cat_cols = [ 'Distance', 'HDays','Delayed','CRSDepTime']

    # Create StringIndexer for each categorical feature
    cat_indexers = [ StringIndexer(inputCol=col, outputCol=col+'_Index')
                     for col in cat_cols ]

    # OneHotEncode each categorical feature after being StringIndexed
    encoders = [ OneHotEncoder(dropLast=False, inputCol=indexer.getOutputCol(),
                               outputCol=indexer.getOutputCol()+'_Encoded')
                 for indexer in cat_indexers ]

    # Assemble all feature columns (numeric + categorical) into `features` col
    assembler = VectorAssembler(inputCols=[encoder.getOutputCol()
                                           for encoder in encoders] + non_cat_cols,
                                outputCol='Features')

    # Train a random forest model
    rf = RandomForestRegressor(labelCol='DepDelay',featuresCol='Features',numTrees=250, maxDepth=30)
    #rf = RandomForestClassifier(labelCol='Delayed',featuresCol='Features', numTrees=8)

   # Chain indexers, encoders, and forest into one pipeline
    pipeline = Pipeline(stages=[ *cat_indexers, *encoders, assembler, rf ] )

    # split the data into training and testing splits (70/30 rn)
    (trainingData, testData) = flights_df.randomSplit([0.7, 0.3])

    # Train the model -- which also runs indexers and coders
    model = pipeline.fit(trainingData)
    # use model to make predictions
    predictions = model.transform(testData)

    predictions.select('DepDelay', 'prediction', 'Features' ).show(100)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol='DepDelay', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    print('Test Error = %g' % (1.0 - accuracy))

    rf_model = model.stages[-1]
    print(rf_model) # summary only
    model.save('new_model')

