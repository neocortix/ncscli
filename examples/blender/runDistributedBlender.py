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
import shutil
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
import purgeKnownHosts
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

def launchInstances( authToken, nInstances, sshClientKeyName, launchedJsonFilepath, filtersJson=None ):
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
        outFile = open( launchedJsonFilepath,'w' )
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

def generateDtrConf( dtrParams, inRecs, settingsFile ):
    outLines = []
    for key, value in dtrParams.items():
         print( key, '=', value, file=settingsFile )
       
    for inRec in inRecs:
        details = inRec
        #iid = details['instanceId']
        #logger.info( 'NCSC Inst details %s', details )
        if 'commandState' in details and details['commandState'] != 'good':
            continue
        if details['state'] == 'started':
            if 'ssh' in details:
                host = details['ssh']['host']
                port = details['ssh']['port']
                user = details['ssh']['user']
                outLine = "node = %s@%s:%s" % (
                        user, host, port
                )
                #print( outLine)
                outLines.append( outLine )
                #print( "node = root@%s:%s" % (
                #        host, port
                #    ))
    for outLine in sorted( outLines):
        print( outLine, file=settingsFile )

def jsonToKnownHosts( instances, outFile ):
    outLines = []
    for inRec in instances:
        details = inRec
        if 'commandState' in details and details['commandState'] != 'good':
            continue
        if details['state'] == 'started':
            if 'ssh' in details:
                host = details['ssh']['host']
                port = details['ssh']['port']
                ecdsaKey = details['ssh']['host-keys']['ecdsa']
                ipAddr = socket.gethostbyname( host )
                outLine = "[%s]:%s,[%s]:%s %s" % (
                        host, port, ipAddr, port, ecdsaKey
                )
                outLines.append( outLine )
    for outLine in sorted( outLines):
        print( outLine, file=outFile )

def output_reader(proc):
    for line in iter(proc.stdout.readline, b''):
        print('<dtr>: {0}'.format(line.decode('utf-8')), end='', file=sys.stderr)

def startDtr( dtrDirPath, workingDir, flush=True ):
    '''starts dtr; returns a dict with "proc" and "thread" elements '''
    dtrArg = '--flush' if flush else '--clean'
    logger.info( 'calling dtr.py' )
    result = {}
    cmd = [
        'python3', '-u', dtrDirPath+'/dtr.py', dtrArg 
    ]
    try:
        logger.info( 'cmd %s', cmd )
        proc = subprocess.Popen( cmd, cwd=workingDir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        #proc = subprocess.Popen( cmd, stderr=subprocess.PIPE, stdout=subprocess.STDOUT )
        logger.info( 'started dtr.py' )
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

def triage( statuses ):
    ''' separates good tellInstances statuses from bad ones'''
    goodOnes = []
    badOnes = []

    for status in statuses:
        if isinstance( status['status'], int) and status['status'] == 0:
            goodOnes.append( status['instanceId'])
        else:
            badOnes.append( status )
    return (goodOnes, badOnes)


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
        default=480 )
    ap.add_argument( '--image_y', type=int, help='the height (in pixels) of the output',
        default=270 )
    ap.add_argument( '--blocks_user', type=int, help='the number of blocks to partiotion the image into',
        default=30 )
    ap.add_argument( '--filetype', choices=['PNG', 'OPEN_EXR'], help='the type of output file',
        default='PNG' )
    args = ap.parse_args()

    signal.signal( signal.SIGTERM, sigtermHandler )

    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = './data'
    launchedJsonFilePath = dataDirPath+'/launched.json'
    dtrSettingsFilePath = dataDirPath + '/user_settings.conf'
    dtrDirPath = os.path.expanduser('~/dtr')

    launchWanted = args.launch
    timeLimit = 1200  # was 900 1200

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
    resultsLogFilePath = os.path.join(dataDirPath, os.path.basename( __file__ ) + '.jlog' )
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
            rc = launchInstances( args.authToken, nWorkersWanted,
                sshClientKeyName, launchedJsonFilePath, filtersJson=args.filter )
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
        goodInstances = startedInstances

        if launchWanted:  #launchWanted:
            with open( os.path.expanduser('~/.ssh/known_hosts'), 'a' ) as khFile:
                jsonToKnownHosts( goodInstances, khFile )
        

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
        if not sigtermSignaled():
            (goodOnes, badOnes) = triage( stepStatuses )
            stepTiming.finish()
            eventTimings.append(stepTiming)
            logger.info( 'stepStatuses %s', stepStatuses )
            goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodOnes ]

            dtrParams = {
                'image_x': args.image_x,
                'image_y': args.image_y,
                'blocks_user': args.blocks_user,
                'filetype': args.filetype,                
            }
            with open( dtrSettingsFilePath, 'w' ) as settingsFile:
                generateDtrConf( dtrParams, goodInstances, settingsFile )

            # copy some files into a working dir, because dtr has no args for them
            shutil.copyfile( dtrDirPath+'/bench.blend', dataDirPath+'/bench.blend' )
            shutil.copyfile( args.blendFilePath, dataDirPath+'/render.blend' )

            masterSpecs = startDtr( dtrDirPath, workingDir=dataDirPath, flush=True )
            if masterSpecs:
                proc = masterSpecs['proc']
                deadline = time.time() + 60 * 60
                while time.time() < deadline:
                    proc.poll() # sets proc.returncode
                    if proc.returncode != None:
                        logger.warning( 'master gave returnCode %d', proc.returncode )
                        if proc.returncode:
                            logger.error( 'dtr gave an unexpected returnCode %d', proc.returncode )
                            # will break out of the outer loop
                            masterFailed = True
                        break
                    time.sleep(.5)
                logger.info('at end of polling loop, return code: %s', proc.returncode )



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
            cmd='purgeKnownHosts.py %s > /dev/null' % (launchedJsonFilePath)
            try:
                subprocess.check_call( cmd, shell=True )
            except Exception as exc:
                logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )
            
    if loadTestStats and loadTestStats.get('nReqsSatisfied', 0) > 0:
        rc = 0
    else:
        rc=1
    logger.info( 'finished with rc %d', rc)
    sys.exit(rc)

