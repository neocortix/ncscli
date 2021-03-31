#import os
import random
#import sys
import time


import flask
from flask import Flask
app = Flask(__name__)
jsonify = flask.json.jsonify  # handy alias

@app.route('/')
@app.route('/baroque')  # new line for https
def root():
    #return 'Hello, World!'
    # get the incoming query params, using defaults for any that are not provided

    # POLICIES:
    # numIterations should be an int, >0, < 1000000 (one million)
    # numSubloops should be an int, >0, < 50
    # totalSleepDurationsSeconds should be a float, >=0, < 30
    # if any of those conditions are not met, the function should return error 400
    # if any of those variables is not specified, it should use the defaults

    args = flask.request.args
    numIterations = args.get( 'numIterations', 39000 )  # controls CPU utilization
    numSubloops = args.get( 'numSubloops', 5 )
    totalSleepDurationSeconds = args.get( 'totalSleepDurationSeconds', 1.0 )

    try:
        # convert query params to appropriate numeric types
        numIterations = int( numIterations )
        numSubloops = int( numSubloops )
        totalSleepDurationSeconds = float( totalSleepDurationSeconds )
    except Exception as _exc:
        return jsonify("parameter type not accepted"), 400

    if numIterations != int(numIterations) or numIterations <= 0 or numIterations > 1000000:
        return jsonify("numIterations parameter value not accepted"), 400
    if numSubloops != int(numSubloops) or numSubloops <= 0 or numSubloops > 50:
        return jsonify("numSubloops parameter value not accepted"), 400
    if totalSleepDurationSeconds < 0 or totalSleepDurationSeconds >= 30:
        return jsonify("totalSleepDurationsSeconds parameter value not accepted"), 400

    if False:
        # old way, not reading params
        numIterations = 39000  # controls CPU utilization
        # numIterations = 25000  # controls CPU utilization
        numSubloops = 5
        totalSleepDurationSeconds = 1.0 # controls duration of task, concurrency
        # totalSleepDurationSeconds = 0.3 # controls duration of task, concurrency

    startTime = time.time()

    numIterationsPerSubloop = int(numIterations/numSubloops)
    sleepDurationPerSubloop = totalSleepDurationSeconds/numSubloops
    for z in range(0,numSubloops):
        for x in range( 0,numIterationsPerSubloop):
            y = random.random()
        time.sleep(sleepDurationPerSubloop)

    elapsed = time.time() - startTime
    return 'Elapsed Time:  '+str( '%.3f' % elapsed )+' seconds\n'
