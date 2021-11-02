#!/usr/bin/env python3
'''launches new NCS instances and starts the squid proxy on them'''
import argparse
from concurrent import futures
import datetime
import json
import logging
import os
import subprocess
import sys
import time
# third-party module(s)
import requests
# neocortix modules
import ncscli.ncs as ncs
import ncscli.batchRunner as batchRunner
import ncscli.plotInstanceMap as plotInstanceMap
import ncscli.tellInstances as tellInstances
import startForwarders  # expected to be in the same directory


# squid 4.6 has been installed by apt; 4.13 is newer
squidVersion = '4.6'  # will be overridden by cmd-line arg

class g_:
    signaled = False
    interrupted = False


def readJLog( inFilePath ):
    '''read JLog file, return list of decoded objects'''
    recs = []
    # read and decode each line as json
    try:
        with open( inFilePath, 'rb' ) as inFile:
            for line in inFile:
                try:
                    decoded = json.loads( line )
                except Exception as exc:
                    logger.warning( 'exception decoding json (%s) %s', type(exc), exc )
                recs.append( decoded )
    except Exception as exc:
        logger.warning( 'exception reading file (%s) %s', type(exc), exc )
    return recs

def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

class FrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing a squid proxy on a worker'''

    def installerCmd( self ):
        return '%s/installCustomSquid.sh %s' % (workerDirName, squidVersion )
        return '%s/installSquid.sh %s' % (workerDirName, squidVersion )


def sigtermSignaled():
    return g_.signaled

def commandInstance( inst, cmd, timeLimit ):
    deadline = time.time() + timeLimit
    sshSpecs = inst['ssh']
    #logInstallerOperation( iid, ['connect', sshSpecs['host'], sshSpecs['port']] )
    with subprocess.Popen(['ssh',
                    '-p', str(sshSpecs['port']),
                    '-o', 'ServerAliveInterval=30',
                    '-o', 'ServerAliveCountMax=12',
                    sshSpecs['user'] + '@' + sshSpecs['host'], cmd],
                    encoding='utf8',
                    #stdout=subprocess.PIPE,  # subprocess.PIPE subprocess.DEVNULL
                    ) as proc:  # stderr=subprocess.PIPE
        #logInstallerOperation( iid, ['command', cmd] )
        #stderrThr = threading.Thread(target=trackStderr, args=(proc,))
        #stderrThr.start()
        abbrevIid = inst['instanceId'][0:16]
        while time.time() < deadline:
            proc.poll() # sets proc.returncode
            if proc.returncode == None:
                logger.debug( 'waiting for command on instance %s', abbrevIid)
            else:
                if proc.returncode == 0:
                    logger.debug( 'command succeeded on instance %s', abbrevIid )
                else:
                    logger.warning( 'instance %s gave returnCode %d', abbrevIid, proc.returncode )
                break
            if sigtermSignaled():
                break
            if g_.interrupted:
                break
            time.sleep(5)
        proc.poll()
        returnCode = proc.returncode if proc.returncode != None else 124 # declare timeout if no rc
        #if returnCode:
        #    logger.warning( 'command returnCode %s', returnCode )
        #if returnCode == 124:
        #    logInstallerEvent( 'timeout', args.instTimeLimit, iid )
        #else:
        #    logInstallerEvent('returncode', returnCode, iid )
        proc.terminate()
        try:
            proc.wait(timeout=5)
            if proc.returncode:
                logger.debug( 'ssh return code %d', proc.returncode )
        except subprocess.TimeoutExpired:
            logger.warning( 'ssh did not terminate in time' )
        #stderrThr.join()
        if returnCode:
            #logger.warning( 'terminating instance because installerFailed %s', iid )
            #terminateInstances( args.authToken, [iid] )
            #logOperation( 'terminateBad', [iid], '<recruitInstances>' )
            #purgeHostKeys( [inst] )
            return returnCode
        else:
            return 0
    return 1

def configureProxy( inst, port, timeLimit=500 ):
    iid = inst['instanceId']
    logger.debug( 'would configure proxy on instance %s for port %d', iid[0:16], port )
    rc = 1
    # generate a command to copy config files to the right place on the instance
    configDirPath = '/etc/squid'
    cmd = "cp -p -r %s/conf/* %s" % (workerDirName, configDirPath )
    logger.debug( 'cmd: %s', cmd )
    rc = commandInstance( inst, cmd, timeLimit=timeLimit )
    return rc

def configureProxies( instances, ports, timeLimit=600 ):
    '''configure proxies, in parallel'''
    returnCodes = []
    with futures.ThreadPoolExecutor( max_workers=len(instances) ) as executor:
        parIter = executor.map( configureProxy, instances, ports, timeout=timeLimit )
        returnCodes = [None] * len(instances)
        try:
            index = 0
            for returnCode in parIter:
                returnCodes[index] = returnCode
                index += 1
                time.sleep( .1 )
        except KeyboardInterrupt:
            logger.warning( 'interrupted, setting flag')
            g_.interrupted = True
            raise
        logger.debug( 'returnCodes: %s', returnCodes )
    return returnCodes

def purgeHostKeys( instanceRecs ):
    '''try to purgeKnownHosts; warn if any exception'''
    logger.debug( 'purgeKnownHosts for %d instances', len(instanceRecs) )
    try:
        ncs.purgeKnownHosts( instanceRecs )
    except Exception as exc:
        logger.warning( 'exception from purgeKnownHosts (%s) %s', type(exc), exc, exc_info=True )
        return 1
    else:
        return 0


if __name__ == '__main__':
    # configure logger formatting
    logger = logging.getLogger(__name__)
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    #batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity
    #startForwarders.logger.setLevel(logging.DEBUG)  # for more verbosity
    logger.setLevel(logging.INFO)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (or none, to use NCS_AUTH_TOKEN env var' )
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default = '{ "cpu-arch": "aarch64", "dar": ">=99", "storage": ">=2000000000" }'
    )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--forwarderHost', help='IP addr (or host name) of the forwarder host',
        default='localhost' )
    ap.add_argument( '--squidVersion', default=squidVersion, help='version of squid to install' )
    ap.add_argument( '--nWorkers', type=int, help='the number of proxies to launch',
        default=10 )
    ap.add_argument( '--outDataDir', required=False, help='a path to the output data dir for this run' )
    ap.add_argument( '--portRangeStart', type=int, default=7100,
        help='the beginning of the range of port numbers to forward' )
    ap.add_argument( '--supportedVersions', action='store_true', help='to list supported versions and exit' )
    ap.add_argument( '--cookie' )
    args = ap.parse_args()

    workerDirName = 'squidWorker'

    supportedVersions = ['4.6', '4.13']
    if args.supportedVersions:
        print( json.dumps( supportedVersions ) )
        sys.exit( 0 )
    squidVersion =  args.squidVersion
    if squidVersion not in supportedVersions:
        logger.error( 'version "%s" is not suppoprted; supported versions are %s',
            squidVersion, sorted( supportedVersions ) )
        sys.exit( 1 )


    outDataDir = args.outDataDir
    if not outDataDir:
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = 'data/squid_' + dateTimeTag

    # you may set forwarderHost manually here, to override auto-detect
    forwarderHost = args.forwarderHost
    if not forwarderHost:
        try:
            forwarderHost = requests.get( 'https://api.ipify.org' ).text
        except forwarderHost:
            logger.warning( 'could not get public ip addr of this host')
    if not forwarderHost:
        logger.error( 'forwarderHost not set')
        exit(1)
    
    authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN')
    instTimeLimit = 11*60

    # may create a symlink to a worker dir, if the specified workerDir is not a directory
    if not os.path.isdir( workerDirName ):
        if os.path.exists( workerDirName ) and not os.path.islink( workerDirName ):
            logger.error( 'your "%s"is neither a dir nor a symlink', workerDirName )
            sys.exit( 1 )
        targetPath = os.path.join( scriptDirPath(), workerDirName )
        if not os.path.isdir( targetPath ):
            logger.error( '"%s"" dir not found in %s', workerDirName, scriptDirPath() )
            sys.exit( 1 )
        try:
            os.symlink( targetPath, workerDirName, target_is_directory=True )
        except Exception as exc:
            logger.error( 'could not create symlink for workerDir (%s) %s', type(exc), exc)
            sys.exit( 1 )
    logger.debug( 'workerDir contents: %s', os.listdir(workerDirName) )

    try:
        # call runBatch to launch worker instances and install the proxy on them
        rc = batchRunner.runBatch(
            frameProcessor = FrameProcessor(),
            recruitOnly=True,
            pushDeviceLocs=False,
            commonInFilePath = workerDirName,
            authToken = authToken,
            cookie = args.cookie,
            sshClientKeyName=args.sshClientKeyName,
            encryptFiles=False,
            timeLimit = 60*60,
            instTimeLimit = instTimeLimit,
            filter = args.filter,
            outDataDir = outDataDir,
            nWorkers = args.nWorkers
        )
        if rc == 0:
            # get iids of instances successfully installed
            recruiterJlogFilePath = os.path.join( outDataDir, 'recruitInstances.jlog' )
            recruitedIids = []
            if os.path.isfile( recruiterJlogFilePath ):
                recruiterResults = readJLog( recruiterJlogFilePath )
                if not recruiterResults:
                    logger.warning( 'no entries in %s', recruiterJlogFilePath )
                for result in recruiterResults:
                    if 'timeout' in result:
                        logger.debug( 'recruiter timeout: %s', result )
                    elif 'returncode' in result:
                        if result['returncode'] != 0:
                            logger.debug( 'recruiter result: %s', result )
                        else:
                            recruitedIids.append( result.get( 'instanceId' ) )
            recruitedIids = set( recruitedIids )
            logger.debug( '%d recruitedIids: %s', len(recruitedIids), recruitedIids )

            portRangeStart=args.portRangeStart
            launchedJsonFilePath = outDataDir +'/recruitLaunched.json'
            launchedInstances = []
            # get details of launched instances from the json file
            with open( launchedJsonFilePath, 'r') as jsonInFile:
                try:
                    launchedInstances = json.load(jsonInFile)  # an array
                except Exception as exc:
                    logger.warning( 'could not load json (%s) %s', type(exc), exc )
                    sys.exit( 2 )
            launchedIids = [inst['instanceId'] for inst in launchedInstances ]

            startedInstances = [inst for inst in launchedInstances if inst['instanceId'] in recruitedIids ]

            #COULD check memory and available ports here

            proxyLogFilePath = '/var/log/squid/*.log'
            starterCmd = 'squid'

            configuredInstances = []
            portMap = {}
            if True:
                # configure the proxy properties on each instance
                ports = list( range( portRangeStart, portRangeStart+len(startedInstances) ) )
                for index, inst in enumerate( startedInstances ):
                    iid = inst['instanceId']
                    portMap[iid] = index + portRangeStart
                logger.info( 'configuring proxies')
                returnCodes = configureProxies( startedInstances, ports, timeLimit=600 )
                for index, code in enumerate( returnCodes ):
                    if code==0:
                        configuredInstances.append( startedInstances[index] )
                    else:
                        iid = startedInstances[index].get('instanceId')
                        logger.info( 'inst %s was not configured properly', iid[0:8] )

            # start the proxy on each instance 
            stepStatuses = tellInstances.tellInstances( configuredInstances, command=starterCmd,
                resultsLogFilePath=outDataDir +'/startProxies.jlog',
                timeLimit=30*60,
                knownHostsOnly=True
                )
            logger.debug( 'starter statuses: %s', stepStatuses )
            # make a list of instances where the proxy was started
            goodIids = []
            for status in stepStatuses:
                if isinstance( status['status'], int) and status['status'] == 0:
                    goodIids.append( status['instanceId'])
                else:
                    logger.warning( 'could not start proxy on %s', status['instanceId'][0:8] )
            #COULD check bound ports again here
            #COULD download logs from all installed instances rather than just good-started instances
            goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
            if goodInstances:
                time.sleep( 60 )
                # download the log file from each instance
                stepStatuses = tellInstances.tellInstances( goodInstances,
                    download=proxyLogFilePath, downloadDestDir=outDataDir +'/proxyLogs',
                    timeLimit=30*60,
                    knownHostsOnly=True
                    )
                logger.debug( 'download statuses: %s', stepStatuses )
                # make a list of instances where the log file was downloaded and proxy start is verified
                goodIids = []
                for status in stepStatuses:
                    if isinstance( status['status'], int) and status['status'] == 0:
                        iid = status['instanceId']
                        logFilePath = os.path.join( outDataDir, 'proxyLogs', iid, 'cache.log' )
                        try:
                            with open( logFilePath, 'r' ) as logFile:
                                contents = logFile.read().rstrip()
                                if 'Accepting ' not in contents:
                                    logger.warning( 'log for %s says it did not start', iid[0:8] )
                                    lastLine = contents.split('\n')[-1].strip()
                                    logger.warning( 'possible error: "%s"', lastLine )
                                # could do other checks here
                                else:
                                    goodIids.append( iid )
                        except Exception as exc:
                            logger.warning( 'exception reading log (%s) %s', type(exc), exc )
                    else:
                        logger.warning( 'could not download log from %s', status['instanceId'][0:8] )
                goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodIids ]
                with open( outDataDir + '/startedWorkers.json','w' ) as outFile:
                    json.dump( goodInstances, outFile, indent=2 )

                # start the ssh port-forwarding
                logger.info( 'would forward ports for %d instances', len(goodInstances) )
                forwarders = startForwarders.startForwarders( goodInstances,
                    forwarderHost=forwarderHost,
                    portMap=portMap, targetPort=3128,
                    portRangeStart=portRangeStart, maxPort=portRangeStart+100,
                    forwardingCsvFilePath=outDataDir+'/sshForwarding.csv'
                    )
                if len( forwarders ) < len( goodInstances ):
                    logger.warning( 'some instances could not be forwarded to' )
                logger.debug( 'forwarders: %s', forwarders )
                #TODO get iids only for successfully forwarded proxies
                forwardedIids = [inst['instanceId'] for inst in goodInstances ]

                goodInstances = [inst for inst in goodInstances if inst['instanceId'] in forwardedIids ]
                if goodInstances:
                    # plot map of workers
                    if os.path.isfile( outDataDir +'/startedWorkers.json' ):
                        plotInstanceMap.plotInstanceMap( goodInstances, outDataDir + "/worldMap.png" )
                        plotInstanceMap.plotInstanceMap( goodInstances, outDataDir + "/worldMap.svg" )

                unusableIids = list( set(launchedIids) - set( forwardedIids) )
                if unusableIids:
                    logger.debug( 'terminating %d unusable instances', len(unusableIids) )
                    ncs.terminateInstances( authToken, unusableIids )
                    unusableInstances = [inst for inst in launchedInstances \
                        if inst['instanceId'] in unusableIids]
                    purgeHostKeys( unusableInstances )
            if launchedInstances:
                print( 'when you want to terminate these instances, use %s %s "%s"'
                    % (sys.executable, scriptDirPath()+'/terminateProxies.py', outDataDir))
        sys.exit( rc )
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
