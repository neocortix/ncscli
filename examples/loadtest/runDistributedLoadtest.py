#!/usr/bin/env python3
"""
does distributed load testing using Locust on NCS instances
"""

# standard library modules
import argparse
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

# third-party module(s)
import requests

# neocortix modules
import analyzeLtStats
import extractAnsibleRecap
try:
    import ncs
except ImportError:
    # set system and python paths for default places, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    os.environ["PATH"] += os.pathsep + ncscliPath
    import ncs


logger = logging.getLogger(__name__)


# possible place for globals is this class's attributes
class g_:
    signaled = False

class SigTerm(BaseException):
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

def loadSshPubKey():
    pubKeyFilePath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    with open( pubKeyFilePath ) as inFile:
        contents = inFile.read()
    return contents

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

def launchInstances( authToken, nInstances, sshClientKeyName, filtersJson=None ):
    returnCode = 13
    # call ncs launch via command-line
    #filtersArg = "--filter '" + filtersJson + "'" if filtersJson else " "
    #cmd = 'ncs.py sc --authToken %s launch --count %d %s --sshClientKeyName %s --json > launched.json' % \
    #    (authToken, nInstances, filtersArg, sshClientKeyName )

    cmd = [
        'ncs.py', 'sc', '--authToken', authToken, 'launch',
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
    jsonToInv()
    logger.info( 'calling installPrereqsQuicker.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ANSIBLE_DISPLAY_FAILED_STDERR=yes ansible-playbook installPrereqsQuicker.yml -i %s | tee data/installPrereqsDeb.temp; wc installed.inv' \
        % invFilePath
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

def startWorkers( victimUrl, masterHost ):
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook startWorkers.yml -e "victimUrl=%s masterHost=%s" -i installed.inv |tee data/startWorkers.out' \
        % (victimUrl, masterHost)
    try:
        subprocess.check_call( cmd, shell=True, stdout=subprocess.DEVNULL )
    except subprocess.CalledProcessError as exc: 
        logger.warning( 'startWorkers returnCode %d (%s)', exc.returncode, exc.output )

def killWorkerProcs():
    logger.info( 'calling killWorkerProcs.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook killWorkerProcs.yml -i installed.inv'
    try:
        subprocess.check_call( cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL )
    except subprocess.CalledProcessError as exc: 
        logger.info( 'exception from killWorkerProcs (return code %d)', exc.returncode )


def output_reader(proc):
    for line in iter(proc.stdout.readline, b''):
        print('subprocess: {0}'.format(line.decode('utf-8')), end='', file=sys.stderr)

def startMaster( victimHostUrl ):
    logger.info( 'calling runLocust.py' )
    result = {}
    cmd = [
        'python3', '-u', 'runLocust.py', '--host='+victimHostUrl, 
        '--heartbeat-liveness=30',
        '--master', '--loglevel', 'INFO', '-f', 'master_locust.py'
    ]
    try:
        proc = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        result['proc'] = proc
        t = threading.Thread(target=output_reader, args=(proc,))
        result['thread'] = t
        t.start()
    except subprocess.CalledProcessError as exc: 
        logger.error( '%s', exc.output )
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
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--rampUpRate', type=float, default=0, help='# of simulated users to start per second (overall)' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use' )
    ap.add_argument( '--targetUris', nargs='*', help='list of URIs to target' )
    ap.add_argument( '--usersPerWorker', type=int, default=35, help='# of simulated users per worker' )
    ap.add_argument( '--startTimeLimit', type=int, default=10, help='time to wait for startup of workers (in seconds)' )
    ap.add_argument( '--susTime', type=int, default=10, help='time to sustain the test after startup (in seconds)' )
    ap.add_argument( '--testId', help='to identify this test' )
    args = ap.parse_args()

    signal.signal( signal.SIGTERM, sigtermHandler )

    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = 'data'
    launchedJsonFilePath = 'launched.json'
    launchWanted = args.launch

    rampUpRate = args.rampUpRate
    if not rampUpRate:
        rampUpRate = args.nWorkers

    os.makedirs( dataDirPath, exist_ok=True )

    nWorkersWanted = args.nWorkers
    if launchWanted:
        # overwrite the launchedJson file as empty list, so we won't have problems with stale contents
        with open( launchedJsonFilePath, 'w' ) as outFile:
            json.dump( [], outFile )
    try:
        masterSpecs = None
        if launchWanted:
            if nWorkersWanted == 0:
                nAvail = ncs.getAvailableDeviceCount( args.authToken, filtersJson=args.filter )
                logger.info( '%d devices available to launch', nAvail )
                nWorkersWanted = nAvail
            if args.sshClientKeyName:
                sshClientKeyName = args.sshClientKeyName
            else:
                keyContents = loadSshPubKey()
                sshClientKeyName = 'loadtest_%s@%s' % (getpass.getuser(), socket.gethostname())
                respCode = ncs.uploadSshClientKey( args.authToken, sshClientKeyName, keyContents )
                if respCode < 200 or respCode >= 300:
                    logger.warning( 'ncs.uploadSshClientKey returned %s', respCode )
                    sys.exit( 'could not upload SSH client key')

            #TODO handle error from launchInstances
            launchInstances( args.authToken, nWorkersWanted, sshClientKeyName, filtersJson=args.filter )
        wellInstalled = []
        if not sigtermSignaled():
            wellInstalled = installPrereqs()
            logger.info( 'installPrereqs succeeded on %d instances', len( wellInstalled ))

        if len( wellInstalled ):
            if args.targetUris:
                targetUriFilePath = dataDirPath + '/targetUris.json'
                with open( targetUriFilePath, 'w' ) as outFile:
                    json.dump( args.targetUris, outFile, indent=1 )
                #uploadTargetUris( targetUriFilePath )
            if not sigtermSignaled():
                startWorkers( args.victimHostUrl, args.masterHost )
                time.sleep(5)

                masterSpecs = startMaster( args.victimHostUrl )
                time.sleep(5)
            if not sigtermSignaled():
                conductLoadtest( 'http://127.0.0.1:8089', nWorkersWanted, args.usersPerWorker,
                    args.startTimeLimit, args.susTime,
                    stopWanted=True, nReqInstances=nWorkersWanted, rampUpRate=rampUpRate )
            
            if masterSpecs:
                time.sleep(5)
                stopMaster( masterSpecs )

            killWorkerProcs()
            try:
                time.sleep( 5 )
                loadTestStats = analyzeLtStats.reportStats(dataDirPath)
            except Exception as exc:
                logger.warning( 'got exception from analyzeLtStats (%s) %s',
                    type(exc), exc, exc_info=False )

    except KeyboardInterrupt:
        logger.warning( '(ctrl-c) received, will shutdown gracefully' )
    except SigTerm:
        logger.warning( 'SIGTERM received, will shutdown gracefully' )
        if masterSpecs:
            logger.info( 'shutting down locust master')
            stopMaster( masterSpecs )
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

    logger.info( 'finished')
    sys.exit(0)

