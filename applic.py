from __future__ import division
from flask import Flask, jsonify, render_template, redirect, url_for, g, Response, request
from flask_cors import CORS
from flask_wtf import FlaskForm
from wtforms import StringField
from pymongo import MongoClient
from bson import json_util
from bson.json_util import dumps
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
# import pymysql
import os

app = Flask(__name__)
app.secret_key = 'todo'
CORS(app)

DB_NAME = 'flight'
# docker run -e URI=test <image-name>
connection = MongoClient("mongodb://account-db:password@ip-address:27017")


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/reasons')
def reasons():
    return render_template('reasons.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/FindDelay')
def FindDelay():
    spark = SparkSession.builder \
        .master('local[3]') \
        .appName('Flight Delay') \
        .getOrCreate()
    flnum= request.args.get('flnum');
    fldate = request.args.get('fldate');
    flmonth = request.args.get('flmonth');
    fldept = request.args.get('fldept');
    flac = request.args.get('flac');
    flor = request.args.get('flor');
    ffdest = request.args.get('ffdest');
    model.load('rf_model')

    predictions = model.transform(userData)
    prediction=predictions.select('prediction')
    
    return jsonify(result = 'The Flight Number you entered was ' + dd + num , prediction = "We predict %.2f minutes delay for this flight" %prediction);    
    json_docs = []
    for one in data:

        json_doc = json.dumps(one, default=json_util.default)
        json_doc = eval(json_doc)
        json_docs.append(json_doc)
    return jsonify(json_docs)

# Add linechart here
@app.route('/fakeline', methods=['POST'])
def fakeline():
    year = request.json["year"]
    carrier = request.json["carrier"]
    json_docs = [{"year":year}, {"carrier": carrier}]

    return jsonify(json_docs)
# Send json file for dynamic dropdown
@app.route('/send')
def send():
    return "<a href=%s>file</a>" % url_for('static', filename='StateCityCode.json')

@app.route('/sendline')
def sendline():
    return "<a href=%s>file</a>" % url_for('static', filename='LineChart.json')


if __name__ == '__main__':
    app.run(host='0.0.0.0')



