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
import math
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
import jsonToKnownHosts
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
    tellInstances.terminate()
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

def launchInstances( authToken, nInstances, sshClientKeyName, launchedJsonFilepath,
        filtersJson=None, encryptFiles=True ):
    returnCode = 13
    # call ncs launch via command-line
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

def magickConvert( srcFilePath, destFilePath ):
    colorSpace = 'sRGB'  # fancier version could maybe override this
    # using magick convert (rather than just 'convert') means we are expecting image v 7.x
    # could use -depth 8 to produce smaller files (default is 16)
    # would use 'magick convert' instead of just 'convert' to force use of version >= 7.0
    cmd = [
        'convert', srcFilePath,
        '-colorspace', colorSpace,
        destFilePath
    ]
    #logger.debug( 'conversion cmd %s', ' '.join(cmd)  )
    try:
        subprocess.check_call( cmd,
            stdout=sys.stderr, stderr=subprocess.STDOUT
            )
    except Exception as exc:
        logger.warning( 'magick convert call threw exception (%s) %s',type(exc), exc )

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

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'blendFilePath', help='the .blend file to render' )
    ap.add_argument( '--authToken', required=True, help='the NCS authorization token to use' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--instTimeLimit', type=int, default=900, help='amount of time (in seconds) installer is allowed to take on instances' )
    ap.add_argument( '--jobId', help='to identify this job' )
    ap.add_argument( '--launch', type=boolArg, default=True, help='to launch and terminate instances' )
    ap.add_argument( '--nWorkers', type=int, default=1, help='the # of worker instances to launch (or zero for all available)' )
    ap.add_argument( '--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--timeLimit', type=int, help='time limit (in seconds) for the whole job',
        default=24*60*60 )
    ap.add_argument( '--useCompositor', type=boolArg, default=True, help='whether or not to use blender compositor' )
    # dtr-specific args
    ap.add_argument( '--width', type=int, help='the width (in pixels) of the output',
        default=960 )
    ap.add_argument( '--height', type=int, help='the height (in pixels) of the output',
        default=540 )
    ap.add_argument( '--blocks_user', type=int, help='the number of blocks to partition the image (or zero for "auto"',
        default=0 )
    ap.add_argument( '--fileType', choices=['PNG', 'OPEN_EXR'], help='the type of output file',
        default='PNG' )
    ap.add_argument( '--frame', type=int, help='the frame number to render',
        default=1 )
    ap.add_argument( '--seed', type=int, help='the blender cycles noise seed',
        default=0 )
    args = ap.parse_args()
    #logger.debug('args: %s', args)

    signal.signal( signal.SIGTERM, sigtermHandler )
    myPid = os.getpid()
    logger.info('procID: %s', myPid)


    #logger.info( '--filter arg <%s>', args.filter )

    dataDirPath = os.path.abspath('./data' )
    launchedJsonFilePath = dataDirPath+'/launched.json'
    dtrSettingsFilePath = dataDirPath + '/user_settings.conf'
    settingsJsonFilePath = dataDirPath + '/settings.json'
    dtrDirPath = os.path.expanduser('~/dtr')

    launchWanted = args.launch

    startTime = time.time()
    eventTimings = []
    #starterTiming = eventTiming.eventTiming('startup')
    #starterTiming.finish()
    #eventTimings.append(starterTiming)

    os.makedirs( dataDirPath, exist_ok=True )

    nWorkersWanted = args.nWorkers
    if launchWanted:
        # overwrite the launchedJson file as empty list, so we won't have problems with stale contents
        with open( launchedJsonFilePath, 'w' ) as outFile:
            json.dump( [], outFile )
    resultsLogFilePath = os.path.join(dataDirPath, os.path.basename( __file__ ) + '.jlog' )
    # truncate the resultsLogFile
    with open( resultsLogFilePath, 'wb' ) as xFile:
        pass # xFile.truncate()

    dtrStatus = None
    try:
        masterSpecs = None
        if launchWanted:
            logger.info( 'launching using filters: %s', args.filter )
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
                randomPart = str( uuid.uuid4() )[0:13]
                keyContents += ' #' + randomPart
                sshClientKeyName = 'bfr_%s' % (randomPart)
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
                jsonToKnownHosts.jsonToKnownHosts( goodInstances, khFile )
        

        wellInstalled = []
        if not sigtermSignaled():
            installerCmd = 'sudo apt-get -qq update && sudo apt-get -qq -y install blender > /dev/null'
            # tell them to ping
            stepTiming = eventTiming.eventTiming('tellInstances install')
            logger.info( 'calling tellInstances to install on %d instances', len(startedInstances))
            stepStatuses = tellInstances.tellInstances( startedInstances, installerCmd,
                resultsLogFilePath=resultsLogFilePath,
                download=None, downloadDestDir=None, jsonOut=None, sshAgent=args.sshAgent,
                timeLimit=min(args.instTimeLimit, args.timeLimit), upload=None,
                knownHostsOnly=True
                )
            # restore our handler because tellInstances may have overridden it
            signal.signal( signal.SIGTERM, sigtermHandler )
            if not stepStatuses:
                logger.warning( 'no statuses returned from installer')
            (goodOnes, badOnes) = triage( stepStatuses )
            stepTiming.finish()
            eventTimings.append(stepTiming)
            logger.info( 'stepStatuses %s', stepStatuses )
        if goodOnes and not sigtermSignaled():
            goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodOnes ]

            blocks_user = args.blocks_user
            if not blocks_user:
                if nWorkersWanted == 1:
                    blocks_user = 1
                else:
                    # nBlocks computation is arbitrary, based on limited experience
                    diag = math.sqrt( args.width * args.height )
                    blocks_user = diag / 12
                    #blocks_user = args.height / 9  # arbitrary, based on limited experience
                    blocks_user = int( max( blocks_user, len(goodInstances)*1 ) )

            dtrParams = {
                'image_x': args.width,
                'image_y': args.height,
                'blocks_user': blocks_user,
                'filetype': args.fileType,
                'frame': args.frame,
                'seed': args.seed,
            }
            if args.useCompositor:
                dtrParams['filetype'] = 'OPEN_EXR'
            extensions = {'PNG': 'png', 'OPEN_EXR': 'exr'}
            prerenderedFileName = 'composite_seed_%d.%s' % \
                (args.seed, extensions[dtrParams['filetype']])
            outFilePattern = 'rendered_frame_######_seed_%d.%s'%(args.seed,extensions[args.fileType])
            outFileName = outFilePattern.replace( '######', '%06d' % args.frame )

            with open( dtrSettingsFilePath, 'w' ) as settingsFile:
                generateDtrConf( dtrParams, goodInstances, settingsFile )

            settingsToSave = dtrParams.copy()
            settingsToSave['outFileName'] = outFileName
            with open( settingsJsonFilePath, 'w' ) as settingsFile:
                json.dump( settingsToSave, settingsFile )

            # copy some files into a working dir, because dtr has no args for them
            shutil.copyfile( dtrDirPath+'/bench.blend', dataDirPath+'/bench.blend' )
            if os.path.abspath( args.blendFilePath ) \
                != os.path.abspath( dataDirPath+'/render.blend' ):
                shutil.copyfile( args.blendFilePath, dataDirPath+'/render.blend' )

            masterSpecs = startDtr( dtrDirPath, workingDir=dataDirPath, flush=True )
            if masterSpecs:
                proc = masterSpecs['proc']
                deadline = startTime + args.timeLimit
                while time.time() < deadline:
                    proc.poll() # sets proc.returncode
                    if proc.returncode != None:
                        dtrStatus = proc.returncode
                        if proc.returncode != 0:
                            logger.error( 'dtr gave an unexpected returnCode %d', proc.returncode )
                        break
                    if sigtermSignaled():
                        logger.info( 'signaling dtr')
                        proc.send_signal( signal.SIGTERM )
                        try:
                            logger.info( 'waiting dtr')
                            proc.wait(timeout=300)
                            if proc.returncode:
                                logger.warning( 'dtr return code %d', proc.returncode )
                        except subprocess.TimeoutExpired:
                            logger.warning( 'dtr did not terminate in time' )
                        break
                    time.sleep(.5)
                if proc.returncode == None:
                    logger.info('at end of polling loop, no return code' )
                    dtrStatus = 124  # linux convention for timeout
                    proc.send_signal( signal.SIGTERM )
                    try:
                        logger.info( 'waiting dtr')
                        proc.wait(timeout=300)
                        if proc.returncode:
                            logger.warning( 'dtr return code %d', proc.returncode )
                    except subprocess.TimeoutExpired:
                        logger.warning( 'dtr did not terminate in time' )


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
            launcherJob = launchedInstances[0].get('job')
            if launcherJob:
                logger.info( 'calling terminateJobInstances for job "%s"', launcherJob )
                ncs.terminateJobInstances( args.authToken, launcherJob )
            else:
                terminateThese( args.authToken, launchedInstances )
            # purgeKnownHosts works well only when known_hosts is not hashed
            cmd='purgeKnownHosts.py %s > /dev/null' % (launchedJsonFilePath)
            try:
                subprocess.check_call( cmd, shell=True )
            except Exception as exc:
                logger.error( 'purgeKnownHosts threw exception (%s) %s',type(exc), exc )

    # run blender compositor if dtr succeeded
    if dtrStatus == 0:
        retCode = None
        if args.useCompositor:
            cmd = [
                'blender', '-b', '-noaudio', dataDirPath+'/render.blend',
                '-P', scriptDirPath()+'/composite_bpy.py',
                '-o',  dataDirPath+'/'+outFilePattern,
                '-f', str(args.frame), '--', '--prerendered', prerenderedFileName
            ]
            logger.info( 'compositing cmd %s', cmd )
            try:
                retCode = subprocess.call( cmd,
                    stdout=sys.stderr, stderr=subprocess.STDOUT
                    )
            except Exception as exc:
                logger.warning( 'blender composite_bpy call threw exception (%s) %s',type(exc), exc )
            # retCode 90 indicates that there was no compositor graph
            if retCode == 90:
                magickConvert( dataDirPath+'/'+prerenderedFileName, dataDirPath+'/'+outFileName )
            #TODO: do something about other non-zero return codes
        if (args.useCompositor==False):
            # rename dtr/imagemagick output to the more desirable fileName
            os.rename( dataDirPath+'/'+prerenderedFileName, dataDirPath+'/'+outFileName )
    # clean up .blend files that were copied
    if os.path.isfile( dataDirPath+'/render.blend' ):
        try:
            # delete render.blend, except in certain conditions; this may be seen as risky
            if dataDirPath != dtrDirPath:
                os.remove( dataDirPath+'/render.blend' )
        except Exception as exc:
            logger.warning( 'exception while deleting render.blend (%s) %s', type(exc), exc, exc_info=False )
    if os.path.isfile( dataDirPath+'/bench.blend' ):
        try:
            if dataDirPath != dtrDirPath:
                os.remove( dataDirPath+'/bench.blend' )
        except Exception as exc:
            logger.warning( 'exception while deleting bench.blend (%s) %s', type(exc), exc, exc_info=False )

    if dtrStatus != None:
        rc = dtrStatus
    elif sigtermSignaled():
        rc = 128 + 15  # linux convention when exiting due to signal 15 (sigterm)
    else:
        rc=1
    logger.info( 'finished with rc %d', rc)
    sys.exit(rc)

