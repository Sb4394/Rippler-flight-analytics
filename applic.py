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
    num= request.args.get('flnum');
    aa = request.args.get('fldate');
    bb = request.args.get('flmonth');
    cc = request.args.get('fldept');
    dd = request.args.get('flac');
    ee = request.args.get('flor');
    ff = request.args.get('ffdest');
    fldate=int(aa)
    flmonth=int(bb)
    fldept=int(cc)
    flac=str(dd)
    flor=str(ee)
    ffdest=str(ff)
    airport = {
    "ATL" : 1,
    "LAX" : 2,
    "ORD" : 3,
    "DFW" : 4,
    "DEN" : 5,
    "JFK" : 6,
    "SFO" : 7,
    "SEA" : 8,
    "LAS" : 9,
    "MCO" : 10,
    "EWR" : 11,
    "CLT" : 12,
    "PHX" : 13,
    "IAH" : 14,
    "MIA" : 15,
    "BOS" : 16,
    "MSP" : 17,
    "FLL" : 18,
    "DTW" : 19,
    "PHL" : 20,
    "LGA" : 21,
    "BWI" : 22,
    "SLC" : 23,
    "SAN" : 24,
    "IAD" : 25,
    "DCA" : 26,
    "MDW" : 27,
    "TPA" : 28,
    "PDX" : 29,
    "HNL" : 30
        }
    airlines={
    "AS":1,
    "DL":2,
    "SX":3,
    "NK":4,
    "EV":5,
    "HA":6,
    "UA":7,
    "B6":8,
    "SW":9,
    "AA":10
    }

    if fldate==15:
        a=1
    else:
        a=abs(fldate-15)

    if flmonth==1 or flmonth==12 or flmonth==8 or flmonth==7 or flmonth==5:
        b=4
    elif flmonth==2 or flmonth==11 or flmonth==6:
        b=3
    elif flmonth==3 or flmonth==10:
        b=2
    else:
        b=1
    c=200

    if fldept>=0 and fldept<=5:
        c=1
    elif fldept==6:
        c=2
    elif fldept==7:
        c=2
    elif fldept==8:
        c=4
    elif fldept==9:
        c=5
    elif fldept==10:
        c=3
    elif fldept==11:
        c=3
    elif fldept==12:
        c=4
    elif fldept>=13 and fldept<=16:
        c=3
    elif fldept==17:
        c=4
    elif fldept==18:
        c=5
    elif fldept==19:
        c=6
    elif fldept==20:
        c=6
    elif fldept==21:
        c=5
    elif fldept==22:
        c=3
    elif fldept==23:
        c=2
    else:
        c=1
    score=a+b+c + airlines[flac] + airport[flor] + airport[ffdest]
    prediction=35*((score-6)/89) + 2*np.random.rand()
    
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



