import base64
import datetime
import enum
import glob
import json
import logging
import os
import re
import signal
import sys
import time
import subprocess
import uuid
import zipfile

# third-party imports
import dateutil.parser
import flask
import psutil
import requests

# neocortix imports
try:
    import ncs
except ImportError:
    # set python path for default place, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    import ncs

app = flask.Flask(__name__)
logger = app.logger
logger.setLevel(logging.INFO)
jsonify = flask.json.jsonify  # handy alias

g_workingDirPath = os.getcwd() + '/bfrData'
#g_dataDirPath = os.getcwd() + '/data'
g_minDpr = 37
g_minRamMB = 4000
g_engineScriptName = 'animateWholeFrames.py'

@app.route('/')
@app.route('/api/')
def hello_world():
    '''the root URI does nothing useful'''
    return jsonify( 'Please refer to the documentation' ), 200

@app.route('/api/jobs/', methods=['GET', 'POST'])
def jobsHandler():
    logger.info( 'handling a request %s ', flask.request )
    if flask.request.method == 'POST':
        #logger.debug( 'postedData: %s',flask.request.get_data() )
        args = flask.request.get_json(force=True)
        #logger.debug( 'args %s', args )
        #existing = getJobs()
        #if existing:
        #    return jsonify( {} ), 503
        returns = launchJob( args )
        return returns
    elif flask.request.method in ['GET', 'HEAD']:
        args = flask.request.args
        # could also do get_json, for full Dmitry emulation
        #logger.debug( 'args %s', args )
        return jsonify( getJobs() )

@app.route('/api/jobs/<jobId>', methods=['GET', 'PUT'])
def jobHandler( jobId ):
    logger.info( 'handling a request %s ', flask.request )
    if flask.request.method == 'GET':
        args = flask.request.args
        # could also do get_json, for full Dmitry emulation
        #logger.debug( 'args %s', args )
        returns = getJobInfo( jobId )
        return returns
    elif flask.request.method == 'PUT':
        args = flask.request.get_json()
        returns = stopJob( jobId )
        return returns

def zipThese( fileList, archivePath ):
    '''zips listed files into an archive (with 'w' mode)'''
    # files in the list should all come from the same directory, or otherwise avid basename conflicts
    with zipfile.ZipFile( archivePath, 'w', \
        compression=zipfile.ZIP_DEFLATED ) as zipper:
        for inFilePath in fileList:
            arcName = os.path.basename( inFilePath )
            zipper.write( inFilePath, arcname=arcName )

@app.route('/api/jobs/<jobId>/<fileName>' )
def jobFileHandler( jobId, fileName ):
    logger.info( 'would retrieve file "%s"', fileName )
    if fileName == 'rendered_frames.zip':
        try:
            pngFiles = glob.glob( os.path.join( dataDirPath( jobId ), 'rendered_frame_*.png') )
            archivePath = os.path.join( dataDirPath( jobId ), fileName )
            zipThese( pngFiles, archivePath )
        except Exception as exc:
            logger.warning( 'exception creating zip (%s) %s ', type(exc), exc )
            # presuming the problem is temporary, return a 503 Service Unavailable
            return ( jsonify('temporarily busy'), 503, {'Retry-After': 1} )

    return flask.send_from_directory( dataDirPath( jobId ), fileName )


@app.route('/api/instances/available' )
def instancesAvailableHandler():
    #logger.info( 'handling a request %s ', flask.request )
    args = flask.request.args
    headers = flask.request.headers
    authToken = headers.get( 'X-Neocortix-Cloud-API-AuthToken' )
    if not authToken:
        authToken = args.get( 'authToken' )
    #logger.info( 'args %s', args )
    returns = getInstancesAvailable( authToken, args )
    return returns

def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def applyDprIfNone( filtersJson, minDpr ):
    '''add a dpr specification to the filtersJson, if it doesn't have one already)'''
    filters = json.loads( filtersJson )
    if 'dpr' not in filters:
        filters['dpr'] = '>=%d' % minDpr
    return json.dumps( filters )

def applyMinRamIfNone( filtersJson, minRamMB ):
    '''add a minimum ram specification to the filtersJson, if it doesn't have one already)'''
    filters = json.loads( filtersJson )
    if 'ram' not in filters:
        filters['ram'] = '>=%d' % (minRamMB * 1000000)
    return json.dumps( filters )

def findRunningScript( targets ):
    #logger.info( 'looking for script %s', targets )
    myPid = os.getpid()
    otherProc = None
    for proc in psutil.process_iter():
        try:
            procInfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            continue
        if procInfo['pid'] == myPid:
            continue  # never find the calling process!
        if 'python' in procInfo['name']:
            scriptName = procInfo['cmdline'][1] if len(procInfo['cmdline']) >1 else '<none>'
            if scriptName.startswith('-') and len(procInfo['cmdline']) >2:
                scriptName = procInfo['cmdline'][2]
            cmdLine = procInfo['cmdline']
            #logger.info( 'seeing: %s %s', procInfo['pid'], scriptName )
            if anyFound( targets, scriptName ):
                otherProc = procInfo['pid']
                #logger.debug( 'found: %s %s', procInfo['pid'], procInfo['cmdline'] )
                break
    if otherProc:
        return cmdLine
    else:
        return None

def findRunningJob( jobId ):
    '''find a process that is running the job with the given id'''
    logger.info( 'looking for job %s', jobId)
    targets = [g_engineScriptName]
    myPid = os.getpid()
    otherProcId = None
    foundProc = None
    for proc in psutil.process_iter():
        try:
            procInfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            continue
        if procInfo['pid'] == myPid:
            continue  # never find the calling process!
        if 'python' in procInfo['name']:
            scriptName = procInfo['cmdline'][1] if len(procInfo['cmdline']) >1 else '<none>'
            if scriptName.startswith('-') and len(procInfo['cmdline']) >2:
                scriptName = procInfo['cmdline'][2]
            cmdLine = procInfo['cmdline']
            #logger.info( 'seeing: %s %s', procInfo['pid'], scriptName )
            if anyFound( targets, scriptName ):
                if ('--jobId' in cmdLine) and (jobId in cmdLine):
                    foundProc = proc
                    otherProcId = procInfo['pid']
                    #logger.debug( 'found: %s %s', procInfo['pid'], procInfo['cmdline'] )
                    break
    #return otherProcId
    return foundProc

def workingDirPath( jobId ):
    return os.path.join( g_workingDirPath, str(jobId) )

def dataDirPath( jobId ):
    return workingDirPath( jobId )

def stdFilePath( baseName, jobId ):
    return '%s/%s.txt' % (dataDirPath( jobId ), baseName)

def anyJobsRunning():
    targetScriptNames = [g_engineScriptName]
    found = findRunningScript( targetScriptNames )
    return found

def getInstancesAvailable( authToken, args ):
    '''gets the number of available instances'''
    #authToken = args.get('authToken')
    if not authToken:
        return jsonify('no authToken provided'), 401
    filtersJson = args.get('filter', None)
    filtersJson = applyDprIfNone( filtersJson, g_minDpr )
    filtersJson = applyMinRamIfNone( filtersJson, g_minRamMB )
    #if not filtersJson:
    #    return jsonify('missing filter arg'), 422
    callTime = time.time()
    nAvail = ncs.getAvailableDeviceCount( authToken, filtersJson )
    logger.info( 'ncs.getAvailableDeviceCount took %.1f seconds', time.time()-callTime )
    #logger.info( '%d devices available to launch', nAvail )
    return jsonify(nAvail), 200

def getJobs():
    #return ["1"]  # pretend busy
    found = anyJobsRunning()
    if found:
        logger.info( 'script running "%s"', found )
        return ["1"]
    else:
        return []

def getJobInfo( jobId ):
    '''returns (json, rc) tuple for the specified job (404 if not found)'''
    info = {'id': jobId }
    stdOutFilePath = stdFilePath('stdout', jobId)
    stdErrFilePath = stdFilePath('stderr', jobId)
    logger.info( 'checking %s', stdErrFilePath )
    if not os.path.isfile( stdErrFilePath ):
        return jsonify('job %s not found' % jobId), 404

    found = findRunningJob( jobId )
    #logger.debug( 'find returned %s', found )
    if found:  # and jobId in found:
        info['state'] = 'running'
    else:
        info['state'] = 'stopped'

    with open( stdErrFilePath, encoding='utf8' ) as inFile:
        stdErrText = inFile.read()
    info['stderr'] = stdErrText
    with open( stdOutFilePath, encoding='utf8' ) as inFile:
        stdOutText = inFile.read()
    info['stdout'] = stdOutText

    settingsFilePath = dataDirPath( jobId ) + '/settings.json'
    if os.path.isfile( settingsFilePath ):
        with open( settingsFilePath, encoding='utf8' ) as settingsFile:
            settings = json.load( settingsFile )
            outFileName = settings.get( 'outVideoFileName' )
            #logger.info( 'outFileName %s', outFileName )
            if outFileName:
                outFilePath = dataDirPath( jobId ) + '/' + outFileName
                found = os.path.isfile( outFilePath )
                if found:
                    url = flask.url_for( 'jobFileHandler', jobId=jobId, fileName=outFileName )
                    logger.info( 'outFileName url: %s', url )
                    if url:
                        info['outputVidUrl'] = url
    progressFilePath = dataDirPath( jobId ) + '/progress.json'
    if os.path.isfile( progressFilePath ):
        with open( progressFilePath, encoding='utf8' ) as progressFile:
            try:
                progress = json.load( progressFile )
            except Exception as exc:
                logger.warning( 'exception parsing progress (%s) %s ', type(exc), exc )
            else:
                info['progress'] = progress


    return jsonify(info), 200

def launchJob( args ):
    '''attempts to launch a job; returns (info, responseCode) tuple'''
    jobId = str( uuid.uuid4() )
    info = {'id': jobId }
    
    pyLibPath = '~/ncscli/examples/blender'
    wdPath = workingDirPath( jobId )
    os.makedirs( dataDirPath( jobId ), exist_ok=True )

    stdOutFilePath = stdFilePath('stdout', jobId)
    stdErrFilePath = stdFilePath('stderr', jobId)
    blenderFilePath = dataDirPath( jobId ) + '/render.blend'

    if True:
        # newer
        if 'dataUri' not in args:
            logger.warning( 'no blender file given')
            return jsonify('no blender file given'), 400
        # quick and dirty data-uri splitter; change this if needing other types of URLs
        dataUri = args['dataUri']
        logger.info( 'dataUri: %s...', dataUri[0:100] )
        if ';base64,' not in dataUri:
            return jsonify('given data is not in data-uri (base64) format'), 400
        encoded = dataUri.split(',')[1]
        decoded = base64.b64decode( encoded )
        #logger.info ('decoded: %s', decoded )
        
        with open( blenderFilePath, 'wb' ) as outFile:
            outFile.write( decoded )
        
        argsStr = blenderFilePath
        for key,val in args.items():
            if key != 'dataUri':
                if key == 'filter':
                    val = applyDprIfNone( val, g_minDpr )
                    val = applyMinRamIfNone( val, g_minRamMB )
                argsStr += ' --' + key + " '" + str(val) + "'"
        logger.info( 'argsStr: %s', argsStr )
    else:
        # older
        # enquote each arg
        args = ["'" + arg + "'" for arg in args ]

        argsStr = ' '.join(args)
    argsStr += ' --instTimeLimit 1800'
    if 'frameTimeLimit' not in argsStr:
        argsStr += ' --frameTimeLimit 2700'
    argsStr += ' --jobId ' + jobId
    argsStr += ' --dataDir ' + dataDirPath( jobId )
    cmd = 'cd  %s && LANG=en_US.UTF-8 PYTHONPATH=%s %s/animateWholeFrames.py %s' \
        % (wdPath, pyLibPath, pyLibPath, argsStr)
    #cmd += ' --filter \'{"regions":"north-america","dpr": ">=37"}\''

    cmd += ' > %s 2> %s' % (stdOutFilePath, stdErrFilePath)

    logger.info( 'starting cmd %s', cmd )
    proc = subprocess.Popen( cmd, shell=True )
    return jsonify(info), 200

def stopJob( jobId ):
    '''stops the given job if it is running; returns (json, rc) tuple for the specified job (404 if not found)'''
    info = {'id': jobId }
    logger.info( 'would stop job %s', jobId )

    foundProc = findRunningJob( jobId )
    if not foundProc:
        # COULD refine this test, as an already-test should not be "not found"
        return jsonify(info), 404
    foundProc.send_signal( signal.SIGTERM )
    return jsonify(info), 200
