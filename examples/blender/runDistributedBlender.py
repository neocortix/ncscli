#!/usr/bin/env python3
"""
does distributed blender rendering using NCS instances
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
try:
    import ncs
except ImportError:
    # set system and python paths for default places, since path seems to be not set properly
    ncscliPath = os.path.expanduser('~/ncscli/ncscli')
    sys.path.append( ncscliPath )
    os.environ["PATH"] += os.pathsep + ncscliPath
    import ncs
import eventTiming
import tellInstances


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

def launchInstances( authToken, nInstances, sshClientKeyName, filtersJson=None ):
    returnCode = 13
    # call ncs launch via command-line
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


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    ncs.logger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    tellInstances.logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')
    logger.info('__file__ %s', __file__)

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'blendFilePath', help='the .blend file to render' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--image_x', type=int, help='the width (in pixels) of the output',
        default=960 )
    ap.add_argument( '--image_y', type=int, help='the height (in pixels) of the output',
        default=540 )
    ap.add_argument( '--blocks_user', type=int, help='the number of blocks to partiotion the image into',
        default=60 )
    ap.add_argument( '--filetype', choices=['PNG', 'OPEN_EXR'], help='the type of output file',
        default='OPEN_EXR' )
    args = ap.parse_args()

    signal.signal( signal.SIGTERM, sigtermHandler )

    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = '.'
    launchedJsonFilePath = 'launched.json'
    launchWanted = args.launch
    timeLimit = 1200

    eventTimings = []
    #starterTiming = eventTiming.eventTiming('startup')
    #starterTiming.finish()
    #eventTimings.append(starterTiming)


    os.makedirs( dataDirPath, exist_ok=True )

    #os.environ['ANSIBLE_CONFIG'] = os.path.join( scriptDirPath(), 'ansible.cfg' )
    #logger.info( 'ANSIBLE_CONFIG: %s', os.getenv('ANSIBLE_CONFIG') )



    nWorkersWanted = args.nWorkers
    if launchWanted:
        # overwrite the launchedJson file as empty list, so we won't have problems with stale contents
        with open( launchedJsonFilePath, 'w' ) as outFile:
            json.dump( [], outFile )
    resultsLogFilePath = dataDirPath + '/runDistributedBlender.jlog'
    # truncate the resultsLogFile
    with open( resultsLogFilePath, 'wb' ) as xFile:
        pass # xFile.truncate()
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
                keyContents = loadSshPubKey()
                #sshClientKeyName = 'loadtest_%s@%s' % (getpass.getuser(), socket.gethostname())
                randomPart = str( uuid.uuid4() )[0:13]
                keyContents += ' #' + randomPart
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
        
        loadedInstances = None
        with open( launchedJsonFilePath, 'r' ) as jsonFile:
            loadedInstances = json.load(jsonFile)  # a list of dicts
        startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]

        wellInstalled = []
        if not sigtermSignaled():
            installerCmd = 'sudo apt-get update && sudo apt-get install -q -y blender'
            # tell them to ping
            stepTiming = eventTiming.eventTiming('tellInstances install')
            logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
            stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
                resultsLogFilePath=resultsLogFilePath,
                download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
                timeLimit=timeLimit, upload=None
                )
            stepTiming.finish()
            eventTimings.append(stepTiming)
            logger.info( 'stepStatuses %s', stepStatuses )




    except KeyboardInterrupt:
        logger.warning( '(ctrl-c) received, will shutdown gracefully' )
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
            '''
            cmd='purgeKnownHosts.py launched.json > /dev/null'
            try:
                subprocess.check_call( cmd, shell=True )
            except Exception as exc:
                logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )
            '''
    if loadTestStats and loadTestStats.get('nReqsSatisfied', 0) > 0:
        rc = 0
    else:
        rc=1
    logger.info( 'finished with rc %d', rc)
    sys.exit(rc)

