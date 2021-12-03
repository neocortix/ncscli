import logging
import os
import random
#import sys
import time
# third-party imports
import flask
from flask import Flask
import werkzeug.utils  # comes with flask

app = Flask(__name__)
logger = app.logger
logger.setLevel(logging.INFO)
jsonify = flask.json.jsonify  # handy alias

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 10 * 1000 * 1000

@app.route('/')
@app.route('/baroque')  # new line for https
def root():
    logger.debug( 'hello from baroque' )
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

ALLOWED_EXTENSIONS = {'ico', 'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'csv', 'jmx'}
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/baroque/uploader', methods=['GET', 'POST'])
def upload_file():
    # aliases for flask things
    request = flask.request
    redirect = flask.redirect
    secure_filename = werkzeug.utils.secure_filename
    url_for = flask.url_for
    # actual example code
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            return jsonify( 'no file was passed in the request' ), 400
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if not file.filename:
            return jsonify( 'no file name was passed in the request' ), 400
        if not allowed_file(file.filename):
            msg = 'That file type is not supported. Supported extensions are %s' % sorted(ALLOWED_EXTENSIONS)
            return jsonify( msg ), 415
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            fakeIt = True
            if fakeIt:
                fileLength = len( file.read() )
                msg = 'would upload file %s, length: %d bytes' % (filename, fileLength )
                logger.info( msg )
                return jsonify( msg ), 200
            else:
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                return redirect(url_for('download_file', name=filename))
        logger.warning( 'unhandled situation' )
        return jsonify( 'could not handle that request'), 400
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''

@app.route('/baroque/uploads/<name>')
def download_file(name):
    return flask.send_from_directory(app.config["UPLOAD_FOLDER"], name)

app.add_url_rule(
    "/baroque/uploads/<name>", endpoint="download_file", build_only=True
    )

@app.route('/baroque/location')
def locationHandler():
    logger.debug( 'hello from baroque/location' )
    args = flask.request.args
    logger.info( 'args: %s', args )
    return jsonify( args ), 200
