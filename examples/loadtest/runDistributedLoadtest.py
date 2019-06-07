#!/usr/bin/env python3
"""
does distributed load testing using Locust on NCS instances
"""

# standard library modules
import argparse
import json
import logging
import subprocess
import sys
import threading
import time

# third-party module(s)
import requests

# neocortix modules
import analyzeLtStats
import ncs


logger = logging.getLogger(__name__)

def launchInstances( authToken, nInstances, sshClientKeyName, filtersJson=None ):
    results = {}
    # call ncs launch via command-line
    filtersArg = "--filter '" + filtersJson + "'" if filtersJson else " "
    cmd = 'ncs.py sc --authToken %s launch --count %d %s --sshClientKeyName %s --json > launched.json' % \
        (authToken, nInstances, filtersArg, sshClientKeyName )
    try:
        subprocess.check_call( cmd, shell=True, stdout=sys.stderr )
    except subprocess.CalledProcessError as exc: 
        logger.error( '%s', exc.output )
        #raise  # TODO raise a more helpful specific type of error
        results['cloudServerErrorCode'] = exc.returncode
        results['instancesAllocated'] = []
    return results

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
    jsonToInv()
    logger.info( 'calling installPrereqsQuicker.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook installPrereqsQuicker.yml -i %s | tee data/installPrereqsDeb.temp; wc installed.inv' \
        % invFilePath
    try:
        exitCode = subprocess.call( cmd, shell=True, stdout=sys.stderr )
        if exitCode:
            logger.warning( 'ansible-playbook installPrereqs returned exit code %d', exitCode )
    except subprocess.CalledProcessError as exc: 
        logger.error( '%s', exc.output )
        raise  # TODO raise a more helpful specific type of error

def startWorkers( victimUrl, masterHost ):
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook startWorkers.yml -e "victimUrl=%s masterHost=%s" -i installed.inv |tee data/startWorkers.out' \
        % (victimUrl, masterHost)
    try:
        subprocess.check_call( cmd, shell=True, stdout=sys.stderr )
    except subprocess.CalledProcessError as exc: 
        logger.warning( 'startWorkers returnCode %d (%s)', exc.returncode, exc.output )

def killWorkerProcs():
    logger.info( 'calling killWorkerProcs.yml' )
    cmd = 'ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook killWorkerProcs.yml -i installed.inv'
    try:
        subprocess.check_call( cmd, shell=True, stdout=sys.stderr )
    except subprocess.CalledProcessError as exc: 
        logger.info( 'exception from killWorkerProcs %s', exc.output )


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
            #print(outs.decode('utf-8'))
        except subprocess.TimeoutExpired:
            logger.warning( 'runLocust did not terminate in time' )
    thread = specs.get('thread')
    if thread:
        thread.join()

def conductLoadtest( masterUrl, nWorkersWanted, usersPerWorker,
    startTimeLimit, susTime, stopWanted, nReqInstances
    ):
    logger.info( 'locals %s', locals() )
    if not masterUrl.endswith( '/' ):
        masterUrl = masterUrl + '/'

    if stopWanted:
        logger.info( 'requesting stop via %s', masterUrl+'stop' )
        resp = requests.get( masterUrl+'stop' )
        logger.info( '%s', resp.json() )
        # might be good to do a "reset" here 
        #resp = requests.get( masterUrl+'stats/reset' )
        #logger.info( '%s', resp.text )
        #time.sleep(1)

    startTime = time.time()
    deadline = startTime + startTimeLimit
    while True:
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
        reqParams = {'locust_count': nUsersWanted,'hatch_rate': nWorkersWanted/1 }
        resp = requests.post( url, data=reqParams )
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
            logger.info( 'error url "%s"', url )
        logger.info( 'monitoring for %d seconds', susTime )
        deadline = time.time() + susTime
        while time.time() <= deadline:
            try:
                resp = requests.get( masterUrl+'stats/requests' )
                respJson = resp.json()
                rps = respJson['total_rps']
                maxRps = max( maxRps, rps )
                #logger.info( 'total_rps %.1f', rps )
                if 'slaves' in respJson:
                    workerData = respJson['slaves']
                    workersFound = len(workerData)
                    logger.info( '%d workers found', workersFound )
                    nGoodWorkers = 0
                    nUsers = 0
                    # loop for each worker, getting actual number of users
                    for worker in workerData:
                        #logger.debug( 'worker %s user_count %d', worker['id'][0:16], worker['user_count'] )
                        if worker['user_count'] > 0: # could check for >= usersPerWorker
                            nGoodWorkers += 1
                            nUsers += worker['user_count']
                        else:
                            logger.info( '%s %d %s', 
                                worker['state'], worker['user_count'], worker['id'] )
                    logger.info( '%d workers working; %d simulated users', nGoodWorkers, nUsers )
            except Exception as exc:
                logger.warning( 'exception (%s) %s', type(exc), exc )
            time.sleep(5)
    # print summary
    print( '%d of %d workers showed up, %d workers working'
        % (workersFound, nWorkersWanted, nGoodWorkers) )

    # get final status of workers
    resp = requests.get( masterUrl+'stats/requests' )
    respJson = resp.json()
    #logger.info( 'resp keys %s', respJson.keys() )
    #logger.info( '%s', respJson )
    if stopWanted:
        resp = requests.get( masterUrl+'stop' )
        #logger.info( '%s', resp.json() )

    # save final status of workers as json
    with open( 'data/locustWorkers.json', 'w' ) as jsonOutFile:
        if 'slaves' in respJson:
            workerData = respJson['slaves']
        else:
            workerData = []
        json.dump( workerData, jsonOutFile, sort_keys=True, indent=2 )

    # print lists of working and not-fully-working workers
    with open( 'data/workingWorkers.txt', 'w' ) as wwOutFile:
        if 'slaves' in respJson:
            workerData = respJson['slaves']
            for worker in workerData:
                if worker['user_count'] >= usersPerWorker:
                    print( worker['id'], file=wwOutFile )
                #else:
                #    print( '$$$ %s %d %s' % 
                #        (worker['state'], worker['user_count'], worker['id']), file=sys.stderr )
    #print( 'peak RPS (nominal)', maxRps )
    print( '%d simulated users' % (respJson['user_count']) )
    if nReqInstances:
        pctGood = 100 * nGoodWorkers / nReqInstances
        print( '\n%d out of %d = %.0f%% success rate' % (nGoodWorkers, nReqInstances, pctGood ) )



if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'victimHostUrl', help='url of the host to target as victim' )
    ap.add_argument( 'masterHost', help='hostname or ip addr of the Locust master' )
    ap.add_argument( '--authToken', help='the NCS authorization token to use' )
    ap.add_argument( '--masterUrl', default='http://127.0.0.1:8089', help='url of the Locust master to control' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='# of worker devices' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use' )
    ap.add_argument( '--usersPerWorker', type=int, default=35, help='# of simulated users per worker' )
    ap.add_argument( '--startTimeLimit', type=int, default=10, help='time to wait for startup of workers (in seconds)' )
    ap.add_argument( '--susTime', type=int, default=10, help='time to sustain the test after startup (in seconds)' )
    ap.add_argument( '--nReqInstances', type=int, help='the # of instances to launch (or zero for all available)' )
    ap.add_argument( '--stop', action='store_true', help='to stop load before and after test' )
    args = ap.parse_args()
    #logger.info( 'args: %s', str(args) )

    dataDirPath = 'data'
    launchedJsonFilePath = 'launched.json'
    launchWanted = True

    nWorkersWanted = args.nWorkers
    if launchWanted:
        nAvail = ncs.getAvailableDeviceCount( args.authToken ) # could pass filtersJson
        logger.info( '%d devices available to launch', nAvail )

        if nWorkersWanted == 0:
            nWorkersWanted = nAvail
        launchInstances( args.authToken, nWorkersWanted, args.sshClientKeyName ) # could pass filtersJson

        installPrereqs()

    masterSpecs = None
    if args.stop:
        startWorkers( args.victimHostUrl, args.masterHost )
        time.sleep(5)

        masterSpecs = startMaster( args.victimHostUrl )
        time.sleep(5)
    
    conductLoadtest( args.masterUrl, nWorkersWanted, args.usersPerWorker,
        args.startTimeLimit, args.susTime,
        stopWanted=args.stop, nReqInstances=args.nReqInstances )
    
    if masterSpecs:
        time.sleep(5)
        stopMaster( masterSpecs )

    killWorkerProcs()

    if launchWanted:

        with open( launchedJsonFilePath, 'r') as jsonInFile:
            launchedInstances = json.load(jsonInFile)  # an array
        terminateThese( args.authToken, launchedInstances )
        # purgeKnownHosts works well only when known_hosts is not hashed
        cmd='purgeKnownHosts.py launched.json > /dev/null'
        try:
            subprocess.check_call( cmd, shell=True )
        except Exception as exc:
            logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )

    try:
        time.sleep( 5 )
        loadTestStats = analyzeLtStats.reportStats(dataDirPath)
    except Exception as exc:
        logger.warning( 'got exception from analyzeLtStats (%s) %s',
            type(exc), exc, exc_info=True )
    else:
        #json.dump( loadTestStats, sys.stdout, indent=2, sort_keys=True, default=str )
        pass
