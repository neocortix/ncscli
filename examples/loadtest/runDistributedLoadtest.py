#!/usr/bin/env python3
"""
does distributed load testing using Locust on NCS instances
"""

# standard library modules
import argparse
import contextlib
import getpass
import json
import logging
import os
import socket
import signal
import subprocess
import sys
import threading
import time
import uuid

# third-party module(s)
import requests

# neocortix modules
import analyzeLtStats
import extractAnsibleRecap
import ncscli.ncs as ncs

# try:
#     import ncs
# except ImportError:
#     # set system and python paths for default places, since path seems to be not set properly
#     ncscliPath = os.path.expanduser('~/ncscli/ncscli')
#     sys.path.append( ncscliPath )
#     os.environ["PATH"] += os.pathsep + ncscliPath
#     import ncs


logger = logging.getLogger(__name__)


# possible place for globals is this class's attributes
class g_:
    signaled = False

class SigTerm(BaseException):
    #logger.warning( 'unsupported SigTerm exception created')
    pass

def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; will try to shut down gracefully' )
    #raise SigTerm()

def sigtermSignaled():
    return g_.signaled


def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def loadSshPubKey():
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

# some port-reservation code adapted from https://github.com/Yelp/ephemeral-port-reserve

def preopen(ip, port):
    ''' open socket with SO_REUSEADDR and listen on it'''
    port = int(port)
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #logger.info( 'binding ip %s port %d', ip, port )
    s.bind((ip, port))

    # the connect below deadlocks on kernel >= 4.4.0 unless this arg is greater than zero
    s.listen(1)
    return s

def preclose(s):
    sockname = s.getsockname()
    # get the port into a TIME_WAIT state
    with contextlib.closing(socket.socket()) as s2:
        s2.connect(sockname)
        s.accept()
    s.close()
    # return sockname[1]

def preopenPorts( startPort, maxPort, nPorts ):
    sockets = []
    gotPorts = False
    while not gotPorts:
        try:
            for port in range( startPort, startPort+nPorts ):
                logger.info( 'preopening port %d', port )
                sock = preopen( '127.0.0.1', port )
                sockets.append( sock )
            gotPorts = True
        except OSError as exc:
            logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
            startPort += nPorts
            sockets = []
            if startPort >= maxPort:
                break
    results = {}
    if not gotPorts:
        logger.error( 'search for available ports exceeded maxPort (%d)', maxPort )
        return results
    results['ports'] = list( range( startPort, startPort+nPorts ) )
    results['sockets'] = sockets
    return results

def launchInstances_old( authToken, nInstances, sshClientKeyName, filtersJson=None ):
    results = {}
    # call ncs launch via command-line
    filtersArg = "--filter '" + filtersJson + "'" if filtersJson else " "
    cmd = 'ncs.py sc --authToken %s launch --count %d %s --sshClientKeyName %s --json > launched.json' % \
        (authToken, nInstances, filtersArg, sshClientKeyName )
    try:
        subprocess.check_call( cmd, shell=True, stdout=sys.stderr )
    except subprocess.CalledProcessError as exc: 
        logger.error( 'CalledProcessError %s', exc.output )
        #raise  # TODO raise a more helpful specific type of error
        results['cloudServerErrorCode'] = exc.returncode
        results['instancesAllocated'] = []
    return results

def launchInstances( authToken, nInstances, sshClientKeyName,
    filtersJson=None, encryptFiles=True ):
    returnCode = 13
    # call ncs launch via command-line
    #filtersArg = "--filter '" + filtersJson + "'" if filtersJson else " "
    #cmd = 'ncs.py sc --authToken %s launch --count %d %s --sshClientKeyName %s --json > launched.json' % \
    #    (authToken, nInstances, filtersArg, sshClientKeyName )

    cmd = [
        'ncs.py', 'sc', '--authToken', authToken, 'launch',
        '--encryptFiles', str(encryptFiles),
        '--count', str(nInstances), # filtersArg,
        '--sshClientKeyName', sshClientKeyName, '--json'
    ]
    if filtersJson:
        cmd.extend( ['--filter',  filtersJson] )
    #logger.debug( 'cmd: %s', cmd )
    try:
        outFile = open('launched.json','w' )
        #proc = subprocess.Popen( cmd, shell=True )
        proc = subprocess.Popen( cmd, stdout=outFile )
        while True:
            #logger.debug( 'polling ncs')
            proc.poll() # sets proc.returncode
            if proc.returncode != None:
                break
            if sigtermSignaled():
                logger.info( 'signaling ncs')
                proc.send_signal( signal.SIGTERM )
                try:
                    logger.info( 'waiting ncs')
                    proc.wait(timeout=60)
                    if proc.returncode:
                        logger.warning( 'ncs return code %d', proc.returncode )
                except subprocess.TimeoutExpired:
                    logger.warning( 'ncs launch did not terminate in time' )
            time.sleep( 1 )
        returnCode = proc.returncode
        if outFile:
            outFile.close()
    except Exception as exc: 
        logger.error( 'exception while launching instances (%s) %s', type(exc), exc, exc_info=True )
        returnCode = 99
    return returnCode

def terminateThese( authToken, inRecs ):
    logger.info( 'to terminate %d instances', len(inRecs) )
    iids = [inRec['instanceId'] for inRec in inRecs]
    ncs.terminateInstances( authToken, iids )

def jsonToInv():
    cmd = 'cat launched.json | jsonToInv.py > launched.inv'
    try:
        subprocess.check_call( cmd, shell=True, stdout=sys.stderr )
    except subprocess.CalledProcessError as exc: 
        logger.error( '%s', exc.output )
        raise  # TODO raise a more helpful specific type of error

def installPrereqs():
    invFilePath = 'launched.inv'
    tempFilePath = 'data/installPrereqsDeb.temp'
    scriptDirPath = os.path.dirname(os.path.realpath(__file__))
    jsonToInv()
    logger.info( 'calling installPrereqsQuicker.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ANSIBLE_DISPLAY_FAILED_STDERR=yes ansible-playbook %s/installPrereqsQuicker.yml -i %s | tee data/installPrereqsDeb.temp; wc installed.inv' \
        % (scriptDirPath, invFilePath)
    try:
        exitCode = subprocess.call( cmd, shell=True, stdout=subprocess.DEVNULL )
        if exitCode:
            logger.warning( 'ansible-playbook installPrereqs returned exit code %d', exitCode )
    except subprocess.CalledProcessError as exc: 
        logger.error( '%s', exc.output )
        raise  # TODO raise a more helpful specific type of error
    installerRecap = extractAnsibleRecap.extractRecap( tempFilePath )
    wellInstalled = extractAnsibleRecap.getGoodInstances( installerRecap )
    sys.stderr.flush()
    return wellInstalled

def startWorkers( victimUrl, masterHost, dataPorts ):
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook %s/startWorkers.yml -e "victimUrl=%s masterHost=%s masterPort=%s" -i installed.inv |tee data/startWorkers.out' \
        % (scriptDirPath(), victimUrl, masterHost, dataPorts[0])
    try:
        subprocess.check_call( cmd, shell=True, stdout=subprocess.DEVNULL )
    except subprocess.CalledProcessError as exc: 
        logger.warning( 'startWorkers returnCode %d (%s)', exc.returncode, exc.output )

def killWorkerProcs():
    logger.info( 'calling killWorkerProcs.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook %s/killWorkerProcs.yml -i installed.inv' \
        % (scriptDirPath())
    try:
        subprocess.check_call( cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL )
    except subprocess.CalledProcessError as exc: 
        logger.info( 'exception from killWorkerProcs (return code %d)', exc.returncode )


def output_reader(proc):
    for line in iter(proc.stdout.readline, b''):
        print('Locust: {0}'.format(line.decode('utf-8')), end='', file=sys.stderr)

def startMaster( victimHostUrl, dataPorts, webPort ):
    logger.info( 'calling runLocust.py' )
    result = {}
    cmd = [
        'python3', '-u', scriptDirPath()+'/runLocust.py', '--host='+victimHostUrl, 
        '--heartbeat-liveness=30',
        '--master-bind-port', str(dataPorts[0]), '--web-port', str(webPort),
        '--master', '--loglevel', 'INFO', '-f', scriptDirPath()+'/master_locust.py'
    ]
    try:
        proc = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        result['proc'] = proc
        t = threading.Thread(target=output_reader, args=(proc,))
        result['thread'] = t
        t.start()
    except subprocess.CalledProcessError as exc:
        # this section never runs because Popen does not raise this exception
        logger.error( 'return code: %d %s', exc.returncode,  exc.output )
        raise  # TODO raise a more helpful specific type of error
    finally:
        return result

def stopMaster( specs ):
    logger.info( 'specs %s', specs )
    proc = specs.get('proc')
    if proc:
        proc.terminate()
        try:
            proc.wait(timeout=5)
            if proc.returncode:
                logger.warning( 'runLocust return code %d', proc.returncode )
        except subprocess.TimeoutExpired:
            logger.warning( 'runLocust did not terminate in time' )
    thread = specs.get('thread')
    if thread:
        thread.join()

def genXmlReport( wasGood ):
    '''preliminary version generates "fake" junit-style xml'''
    templateProlog = '''<?xml version="1.0" ?>
<testsuites>
    <testsuite tests="1" errors="0" failures="%d" name="loadtests" >
        <testcase classname="com.neocortix.loadtest" name="loadtest" time="1.0">
    '''
    templateFail = '''
        <failure message="response time too high">Assertion failed</failure>
    '''
    templateEpilog = '''
        </testcase>
    </testsuite>
</testsuites>
    '''
    if wasGood:
        return (templateProlog % 0) + templateEpilog
    else:
        return (templateProlog % 1) + templateFail + templateEpilog

def testsPass( args, loadTestStats ):
    if loadTestStats.get('nReqsSatisfied', 0) <= 0:
        return False
    return loadTestStats.get('meanResponseTimeMs30', 99999) <= args.reqMsprMean

def conductLoadtest( masterUrl, nWorkersWanted, usersPerWorker,
    startTimeLimit, susTime, stopWanted, nReqInstances, rampUpRate
    ):
    logger.info( 'locals %s', locals() )
    hatch_rate = rampUpRate if rampUpRate else nWorkersWanted  # force it non-zero
    if not masterUrl.endswith( '/' ):
        masterUrl = masterUrl + '/'

    if stopWanted:
        logger.info( 'requesting stop via %s', masterUrl+'stop' )
        resp = requests.get( masterUrl+'stop' )
        logger.info( '%s', resp.json() )

    startTime = time.time()
    deadline = startTime + startTimeLimit
    workersFound = False
    while True:
        if g_.signaled:
            break
        try:
            reqUrl = masterUrl+'stats/requests'
            resp = requests.get( reqUrl )
            respJson = resp.json()
            if 'slaves' in respJson:
                workerData = respJson['slaves']
                workersFound = len(workerData)
                logger.info( '%d workers found', workersFound )
                if workersFound >= nWorkersWanted:
                    break
                if time.time() > deadline:
                     break                   
        except Exception as exc:
            logger.warning( 'exception (%s) %s', type(exc), exc )
        time.sleep(1)

    nGoodWorkers = 0
    maxRps = 0
    if workersFound:
        url = masterUrl+'swarm'
        nUsersWanted = nWorkersWanted * usersPerWorker
        reqParams = {'locust_count': nUsersWanted,'hatch_rate': hatch_rate }
        logger.info( 'swarming, count: %d, rate %.1f', nUsersWanted, hatch_rate )
        resp = requests.post( url, data=reqParams )
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
            logger.info( 'error url "%s"', url )
        logger.info( 'monitoring for %d seconds', susTime )
        deadline = time.time() + susTime
        while time.time() <= deadline:
            if g_.signaled:
                break
            try:
                resp = requests.get( masterUrl+'stats/requests' )
                respJson = resp.json()
                rps = respJson['total_rps']
                maxRps = max( maxRps, rps )
                if 'slaves' in respJson:
                    workerData = respJson['slaves']
                    workersFound = len(workerData)
                    #logger.info( '%d workers found', workersFound )
                    nGoodWorkers = 0
                    nUsers = 0
                    # loop for each worker, getting actual number of users
                    for worker in workerData:
                        if worker['user_count'] > 0: # could check for >= usersPerWorker
                            nGoodWorkers += 1
                            nUsers += worker['user_count']
                        else:
                            logger.info( '%s %d %s', 
                                worker['state'], worker['user_count'], worker['id'] )
                    logger.info( '%d workers found, %d working; %d simulated users',
                        workersFound, nGoodWorkers, nUsers )
            except Exception as exc:
                logger.warning( 'exception (%s) %s', type(exc), exc )
            time.sleep(5)
    # print summary
    print( '%d of %d workers showed up, %d workers working at the end'
        % (workersFound, nWorkersWanted, nGoodWorkers) )

    # get final status of workers
    resp = requests.get( masterUrl+'stats/requests' )
    respJson = resp.json()
    if stopWanted:
        resp = requests.get( masterUrl+'stop' )

    # save final status of workers as json
    with open( 'data/locustWorkers.json', 'w' ) as jsonOutFile:
        if 'slaves' in respJson:
            workerData = respJson['slaves']
        else:
            workerData = []
        json.dump( workerData, jsonOutFile, sort_keys=True, indent=2 )

    print( '%d simulated users' % (respJson['user_count']) )
    '''
    if nReqInstances:
        pctGood = 100 * nGoodWorkers / nReqInstances
        print( '\n%d out of %d = %.0f%% success rate' % (nGoodWorkers, nReqInstances, pctGood ) )
    '''

def executeLoadtest( targetHostUrl, htmlOutFileName='ltStats.html' ):
    masterStarted = False
    masterFailed = False
    masterSpecs = None
    loadTestStats = None
    workersStarted = False
    try:
        if not sigtermSignaled():
            startPort = args.startPort
            maxPort=args.startPort+300
            while (not masterStarted) and (not masterFailed):
                if startPort >= maxPort:
                    logger.warning( 'startPort (%d) exceeding maxPort (%d)',
                        startPort, maxPort )
                    break
                preopened = preopenPorts( startPort, maxPort, nPorts=3 )
                reservedPorts = preopened['ports']
                sockets = preopened['sockets']
                dataPorts = reservedPorts[0:2]
                webPort = reservedPorts[2]

                for sock in sockets:
                    preclose( sock )
                sockets = []

                masterSpecs = startMaster( targetHostUrl, dataPorts, webPort )
                if masterSpecs:
                    proc = masterSpecs['proc']
                    deadline = time.time() + 30
                    while time.time() < deadline:
                        proc.poll() # sets proc.returncode
                        if proc.returncode != None:
                            logger.warning( 'master gave returnCode %d', proc.returncode )
                            if proc.returncode == 98:
                                logger.info( 'locust tried to bind to a busy port')
                                # continue outer loop with higher port numbers
                                startPort += 3
                            else:
                                logger.error( 'locust gave an unexpected returnCode %d', proc.returncode )
                                # will break out of the outer loop
                                masterFailed = True
                            break
                        time.sleep(.5)
                    if proc.returncode == None:
                        masterStarted = True
        if masterStarted and not sigtermSignaled():
            logger.info( 'calling startWorkers' )
            startWorkers( targetHostUrl, args.masterHost, dataPorts )
            workersStarted = True
        if masterStarted and not sigtermSignaled():
            #time.sleep(5)
            masterUrl = 'http://127.0.0.1:%d' % webPort
            conductLoadtest( masterUrl, nWorkersWanted, args.usersPerWorker,
                args.startTimeLimit, args.susTime,
                stopWanted=True, nReqInstances=nWorkersWanted, rampUpRate=rampUpRate )
        
        if masterStarted and masterSpecs:
            time.sleep(5)
            stopMaster( masterSpecs )
        if workersStarted:
            killWorkerProcs()
        if masterStarted:
            try:
                time.sleep( 5 )
                loadTestStats = analyzeLtStats.reportStats(dataDirPath, htmlOutFileName)
            except Exception as exc:
                logger.warning( 'got exception from analyzeLtStats (%s) %s',
                    type(exc), exc, exc_info=False )
            plottingWanted = True
            if plottingWanted:
                try:
                    temp = analyzeLtStats.temporallyIntegrateLocustStats(
                        dataDirPath+'/locustStats.csv' )
                    analyzeLtStats.plotIntegratedStats( temp,
                        dataDirPath+'/integratedPerf.png' )
                except Exception as exc:
                    logger.warning( 'got exception from integrating plotting stats (%s) %s',
                        type(exc), exc, exc_info=False )
                # extended plotting using the boss's code
                try:
                    cmd = [
                        scriptDirPath()+'/plotLocustAnalysis.py',
                        '--launchedFilePath', launchedJsonFilePath,
                        '--mapFilePath', scriptDirPath()+'/WorldCountryBoundaries.csv',
                        '--outDirPath', dataDirPath,
                        '--statsFilePath', dataDirPath+'/locustStats.csv'
                    ]
                    plottingRc = subprocess.call( cmd, stdout=sys.stderr, stderr=subprocess.STDOUT )
                    if plottingRc:
                        logger.warning( 'plotLocustAnalysis returned RC %d', plottingRc )
                except Exception as exc:
                    logger.warning( 'got exception from extended plotting (%s) %s',
                        type(exc), exc, exc_info=False )
                

    except KeyboardInterrupt:
        logger.warning( '(ctrl-c) received, will shutdown gracefully' )
        if workersStarted:
            killWorkerProcs()
        if masterStarted and masterSpecs:
            stopMaster( masterSpecs )
        raise
    except Exception as exc:
        logger.warning( 'an exception occurred (%s); will try to shutdown gracefully', type(exc) )
        if workersStarted:
            killWorkerProcs()
        if masterStarted and masterSpecs:
            stopMaster( masterSpecs )
        raise
    return loadTestStats


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'victimHostUrl', help='url of the host to target as victim' )
    ap.add_argument( 'masterHost', help='hostname or ip addr of the Locust master' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use' )
    ap.add_argument( '--altTargetHostUrl', help='an alternative target host URL for comparison' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--rampUpRate', type=float, default=0, help='# of simulated users to start per second (overall)' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--startPort', type=int, default=30000, help='a starting port number to listen on' )
    ap.add_argument( '--targetUris', nargs='*', help='list of URIs to target' )
    ap.add_argument( '--usersPerWorker', type=int, default=35, help='# of simulated users per worker' )
    ap.add_argument( '--startTimeLimit', type=int, default=30, help='time to wait for startup of workers (in seconds)' )
    ap.add_argument( '--susTime', type=int, default=10, help='time to sustain the test after startup (in seconds)' )
    ap.add_argument( '--reqMsprMean', type=float, default=1000, help='required ms per response' )
    ap.add_argument( '--testId', help='to identify this test' )
    args = ap.parse_args()
    argsToSave = vars(args).copy()
    del argsToSave['authToken']


    signal.signal( signal.SIGTERM, sigtermHandler )

    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = 'data'
    os.makedirs( dataDirPath, exist_ok=True )

    launchedJsonFilePath = 'launched.json'

    argsFilePath = os.path.join( dataDirPath, 'runDistributedLoadtest_args.json' )
    with open( argsFilePath, 'w' ) as argsFile:
        json.dump( argsToSave, argsFile, indent=2 )

    xmlReportFilePath = dataDirPath + '/testResults.xml'
    if os.path.isfile( xmlReportFilePath ):
        try:
            # delete old file
            os.remove( xmlReportFilePath )
        except Exception as exc:
            logger.warning( 'exception while deleting old xml report file (%s) %s', type(exc), exc, exc_info=False )

    launchWanted = args.launch

    rampUpRate = args.rampUpRate
    if not rampUpRate:
        rampUpRate = args.nWorkers

    os.environ['ANSIBLE_CONFIG'] = os.path.join( scriptDirPath(), 'ansible.cfg' )
    #logger.info( 'ANSIBLE_CONFIG: %s', os.getenv('ANSIBLE_CONFIG') )

    # check whether the victimHost is available
    try:
        resp = requests.head( args.victimHostUrl )
        if (resp.status_code < 200) or (resp.status_code >= 400):
            logger.error( 'got response %d from target host %s',
                resp.status_code, args.victimHostUrl )
            sys.exit(1)
    except Exception as exc:
        logger.warning( 'could not access target host %s',args.victimHostUrl )
        logger.error( 'got exception %s', exc )
        sys.exit(1)

    # check whether the altTargetHostUrl, if any, is available
    if args.altTargetHostUrl:
        try:
            resp = requests.head( args.altTargetHostUrl )
            if (resp.status_code < 200) or (resp.status_code >= 400):
                logger.error( 'got response %d from alt target host %s',
                    resp.status_code, args.altTargetHostUrl )
                sys.exit(1)
        except Exception as exc:
            logger.warning( 'could not access alt target host %s',args.altTargetHostUrl )
            logger.error( 'got exception %s', exc )
            sys.exit(1)

    nWorkersWanted = args.nWorkers
    if launchWanted:
        # overwrite the launchedJson file as empty list, so we won't have problems with stale contents
        with open( launchedJsonFilePath, 'w' ) as outFile:
            json.dump( [], outFile )
    loadTestStats = None
    try:
        masterSpecs = None
        if launchWanted:
            nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
            if nWorkersWanted > (nAvail + 5):
                logger.error( 'not enough devices available (%d requested)', nWorkersWanted )
                sys.exit(1)
            if nWorkersWanted == 0:
                logger.info( '%d devices available to launch', nAvail )
                nWorkersWanted = nAvail
            if args.sshClientKeyName:
                sshClientKeyName = args.sshClientKeyName
            else:
                keyContents = loadSshPubKey().strip()
                #sshClientKeyName = 'loadtest_%s@%s' % (getpass.getuser(), socket.gethostname())
                randomPart = str( uuid.uuid4() )[0:13]
                #keyContents += ' #' + randomPart
                sshClientKeyName = 'loadtest_%s' % (randomPart)
                respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
                if respCode < 200 or respCode >= 300:
                    logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                    sys.exit( 'could not upload SSH client key')

            #TODO handle error from launchInstances
            rc = launchInstances( args.authToken, nWorkersWanted, sshClientKeyName, filtersJson=args.filter )
            if rc:
                logger.debug( 'launchInstances returned %d', rc )
            # delete sshClientKey only if we just uploaded it
            if sshClientKeyName != args.sshClientKeyName:
                logger.info( 'deleting sshClientKey %s', sshClientKeyName)
                ncs.deleteSshClientKey( args.authToken, sshClientKeyName )
            if rc:
                logger.warning( 'launchInstances returned %d', rc )
        wellInstalled = []
        if rc == 0 and not sigtermSignaled():
            wellInstalled = installPrereqs()
            logger.info( 'installPrereqs succeeded on %d instances', len( wellInstalled ))

        if len( wellInstalled ):
            if args.targetUris:
                targetUriFilePath = dataDirPath + '/targetUris.json'
                with open( targetUriFilePath, 'w' ) as outFile:
                    json.dump( args.targetUris, outFile, indent=1 )
                #uploadTargetUris( targetUriFilePath )
            # do all the steps of the actual loadtest (the first of 2 if doing a comparison)
            loadTestStats = executeLoadtest( args.victimHostUrl )
            logger.info ( 'loadTestStatsA: %s', loadTestStats )
            xml = genXmlReport( testsPass( args, loadTestStats ) )
            with open( xmlReportFilePath, 'w' ) as outFile:
                outFile.write( xml )
            if args.altTargetHostUrl:
                # rename output files for the primary target
                srcFilePath = os.path.join( dataDirPath, 'ltStats.html' )
                if os.path.isfile( srcFilePath ):
                    os.rename( srcFilePath, os.path.join( dataDirPath, 'ltStats_a.html' ) )
                srcFilePath = os.path.join( dataDirPath, 'locustStats.csv' )
                if os.path.isfile( srcFilePath ):
                    os.rename( srcFilePath, os.path.join( dataDirPath, 'locustStats_a.csv' ) )
                print() # a blank lne to separate the outputs from the 2 subtests
                sys.stdout.flush()

                # do all the steps of the second loadtest
                loadTestStatsB = executeLoadtest( args.altTargetHostUrl, htmlOutFileName='ltStats_b.html' )
                logger.info ( 'loadTestStatsB: %s', loadTestStatsB )
                # rename an output file from the second subtest
                srcFilePath = os.path.join( dataDirPath, 'locustStats.csv' )
                if os.path.isfile( srcFilePath ):
                    os.rename( srcFilePath, os.path.join( dataDirPath, 'locustStats_b.csv' ) )

                # optional code to compare stats

                comparison = {}
                comparison[ args.victimHostUrl ] = loadTestStats
                comparison[ args.altTargetHostUrl ] = loadTestStatsB
                comparisonFilePath = os.path.join( dataDirPath, 'comparison.json' )
                with open( comparisonFilePath, 'w' ) as comparisonOutFile:
                    json.dump( comparison, comparisonOutFile, indent=2 )
                # compose per-worker comparison table and save it
                compDf = analyzeLtStats.compareLocustStatsByWorker( launchedJsonFilePath,
                    os.path.join( dataDirPath, 'locustStats_a.csv' ),
                    os.path.join( dataDirPath, 'locustStats_b.csv' )
                    )
                compDf.to_json( dataDirPath+'/compWorkerTable.json', 'table', index=True )
                # compose per-area comparison table and save it
                compDf = analyzeLtStats.compareLocustStats( launchedJsonFilePath,
                    os.path.join( dataDirPath, 'locustStats_a.csv' ),
                    os.path.join( dataDirPath, 'locustStats_b.csv' )
                    )
                compDf.to_json( dataDirPath+'/compAreaTable.json', 'table', index=True )
                html = compDf.to_html( 
                    classes=['sortable'], justify='left', float_format=lambda x: '%.1f' % x
                    )
                with open( dataDirPath+'/compAreaTable.htm', 'w', encoding='utf8') as htmlOutFile:
                    htmlOutFile.write( html )


    except KeyboardInterrupt:
        logger.warning( '(ctrl-c) received, will shutdown gracefully' )
    except SigTerm:
        logger.warning( 'unsupported SIGTERM exception raised, may shutdown gracefully' )
        if masterSpecs:
            logger.info( 'shutting down locust master')
            stopMaster( masterSpecs )
    except Exception as exc:
        logger.error( 'an exception occurred; will try to shutdown gracefully', exc_info=True )
    if launchWanted:
        # get instances from json file, to see which ones to terminate
        launchedInstances = []
        with open( launchedJsonFilePath, 'r') as jsonInFile:
            try:
                launchedInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        if len( launchedInstances ):
            jobId = launchedInstances[0].get('job')
            if jobId:
                logger.info( 'calling terminateJobInstances for job "%s"', jobId )
                ncs.terminateJobInstances( args.authToken, jobId )
            else:
                terminateThese( args.authToken, launchedInstances )
            # purgeKnownHosts works well only when known_hosts is not hashed
            cmd='purgeKnownHosts.py launched.json > /dev/null'
            try:
                subprocess.check_call( cmd, shell=True )
            except Exception as exc:
                logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )
    if loadTestStats and loadTestStats.get('nReqsSatisfied', 0) > 0 and testsPass( args, loadTestStats ):
        rc = 0
    else:
        rc=1
    logger.info( 'finished with rc %d', rc)
    sys.exit(rc)

