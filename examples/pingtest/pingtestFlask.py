#import datetime
#import enum
import json
import logging
import psutil
import os
#import re
import signal
import sys
import time
import subprocess
import uuid

# third-party imports
import flask

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

g_workingDirPath = os.getcwd() + '/pingtestData'

@app.route('/')
@app.route('/api/')
def hello_world():
    '''the root URI does nothing useful'''
    return jsonify( 'Please refer to the documentation' ), 200

@app.route('/api/tests/', methods=['GET', 'POST'])
def testsHandler():
    logger.info( 'handling a request %s ', flask.request )
    if flask.request.method == 'POST':
        #logger.debug( 'postedData: %s',flask.request.get_data() )
        args = flask.request.get_json(force=True)
        #logger.debug( 'args %s', args )
        returns = launchTest( args )
        return returns
    elif flask.request.method in ['GET', 'HEAD']:
        args = flask.request.args
        # could also do get_json, for full Dmitry emulation
        #logger.debug( 'args %s', args )
        return jsonify( getTests() )

@app.route('/api/tests/<testId>', methods=['GET', 'PUT'])
def testHandler( testId ):
    logger.info( 'handling a request %s ', flask.request )
    if flask.request.method == 'GET':
        args = flask.request.args
        # could also do get_json, for full Dmitry emulation
        #logger.debug( 'args %s', args )
        returns = getTestInfo( testId )
        return returns
    elif flask.request.method == 'PUT':
        args = flask.request.get_json()
        returns = stopTest( testId )
        return returns

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

def findRunningTest( testId ):
    '''find a process that is running the test with the given id'''
    logger.info( 'looking for test %s', testId)
    targets = ['runDistributedPingtest.py']
    myPid = os.getpid()
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
                if ('--testId' in cmdLine) and (testId in cmdLine):
                    foundProc = proc
                    #logger.debug( 'found: %s %s', procInfo['pid'], procInfo['cmdline'] )
                    break
    return foundProc

def workingDirPath( testId ):
    return os.path.join( g_workingDirPath, str(testId) )

def dataDirPath( testId ):
    return os.path.join( workingDirPath( testId ), 'data' )

def stdFilePath( baseName, testId ):
    return '%s/%s.txt' % (dataDirPath( testId ), baseName)

def anyTestsRunning():
    targetScriptNames = ['runDistributedPingtest' ]
    found = findRunningScript( targetScriptNames )
    return found

def getInstancesAvailable( authToken, args ):
    '''gets the number of available instances'''
    #authToken = args.get('authToken')
    if not authToken:
        return jsonify('no authToken provided'), 401
    filtersJson = args.get('filter', None)

    callTime = time.time()
    nAvail = ncs.getAvailableDeviceCount( authToken, filtersJson )
    logger.info( 'ncs.getAvailableDeviceCount took %.1f seconds', time.time()-callTime )
    #logger.info( '%d devices available to launch', nAvail )
    return jsonify(nAvail), 200

def getTests():
    #return ["1"]  # pretend busy
    found = anyTestsRunning()
    if found:
        logger.info( 'script running "%s"', found )
        return ["1"]
    else:
        return []

def getTestInfo( testId ):
    '''returns (json, rc) tuple for the specified test (404 if not found)'''
    info = {'id': testId }
    stdOutFilePath = stdFilePath('stdout', testId)
    stdErrFilePath = stdFilePath('stderr', testId)
    logger.info( 'checking %s', stdErrFilePath )
    if not os.path.isfile( stdErrFilePath ):
        return jsonify('test %s not found' % testId), 404

    found = findRunningTest( testId )
    #logger.debug( 'find returned %s', found )
    if found:  # and testId in found:
        info['state'] = 'running'
    else:
        info['state'] = 'stopped'

    with open( stdErrFilePath, encoding='utf8' ) as inFile:
        stdErrText = inFile.read()
    info['stderr'] = stdErrText
    with open( stdOutFilePath, encoding='utf8' ) as inFile:
        stdOutText = inFile.read()
    info['stdout'] = stdOutText

    wwwDirPath = os.path.join( workingDirPath( testId ), 'www' )
    #statsFilePath = wwwDirPath + '/stats.html'
    statsFilePath = wwwDirPath + '/areaTable.htm'
    if os.path.isfile( statsFilePath ):
        with open( statsFilePath, encoding='utf8' ) as inFile:
            statsText = inFile.read()
        info['stats'] = statsText

    locInfoFilePath = wwwDirPath + '/locInfo.json'
    if os.path.isfile( locInfoFilePath ):
        with open( locInfoFilePath, encoding='utf8' ) as inFile:
            locInfo = inFile.read()
        info['locInfo'] = locInfo

    return jsonify(info), 200

def launchTest( args ):
    '''attempts to launch a test; returns (info, responseCode) tuple'''
    testId = str( uuid.uuid4() )
    info = {'id': testId }
    
    #with open( 'pingtestFlask_config.json', 'r' ) as inFile:
    #    config = json.load( inFile )
    #masterHost = config.get('masterHost')
    #if not masterHost:
    #    return jsonify('masterHost config setting not found'), 500

    pyLibPath = '~/ncscli/examples/pingtest'
    wdPath = workingDirPath( testId )
    os.makedirs( dataDirPath( testId ), exist_ok=True )

    stdOutFilePath = stdFilePath('stdout', testId)
    stdErrFilePath = stdFilePath('stderr', testId)

    # enquote each arg
    args = ["'" + str(arg) + "'" for arg in args ]

    argsStr = ' '.join(args)
    argsStr += ' --testId ' + testId
    cmd = 'cd  %s && PYTHONPATH=%s %s/runDistributedPingtest.py %s' \
        % (wdPath, pyLibPath, pyLibPath, argsStr)

    cmd += ' > %s 2> %s' % (stdOutFilePath, stdErrFilePath)

    #logger.debug( 'starting cmd %s', cmd )
    proc = subprocess.Popen( cmd, shell=True )
    return jsonify(info), 200

def stopTest( testId ):
    '''stops the given test if it is running; returns (json, rc) tuple for the specified test (404 if not found)'''
    info = {'id': testId }
    logger.info( 'would stop test %s', testId )

    foundProc = findRunningTest( testId )
    if not foundProc:
        # COULD refine this test, as an already-test should not be "not found"
        return jsonify(info), 404
    foundProc.send_signal( signal.SIGTERM )
    return jsonify(info), 200
