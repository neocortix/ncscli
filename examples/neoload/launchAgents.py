#!/usr/bin/env python3
'''launches new NCS instances and starts the NeoLoad LoadGenerator agent on them'''
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
import ncscli.tellInstances as tellInstances
import startForwarders  # expected to be in the same directory


neoloadVersion = '7.10.1'  # will be overridden by cmd-line arg
nlWebWanted = False

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

def truncateVersion( nlVersion ):
    '''drop patch-level part of version number, if any'''
    return '.'.join(nlVersion.split('.')[:-1]) if nlVersion.count('.') > 1 else nlVersion

class neoloadFrameProcessor(batchRunner.frameProcessor):
    '''defines details for installing Neotys Load Generator agent on a worker'''

    def installerCmd( self ):
        truncVersion = truncateVersion( neoloadVersion )
        scoredVersion = neoloadVersion.replace( '.', '_' )
        if neoloadVersion == '7.10':
            return 'nlAgent/install_7-10_slim.sh'
        elif neoloadVersion == '7.7':
            return 'nlAgent/install_7-7.sh'
        elif neoloadVersion == '7.6':
            return 'nlAgent/install_7-6.sh'
        else:
            return 'nlAgent/install_7-x.sh %s %s %s' % (neoloadVersion, truncVersion, scoredVersion )

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

def configureAgent( inst, port, timeLimit=500 ):
    iid = inst['instanceId']
    logger.debug( 'would configure agent on instance %s for port %d', iid[0:16], port )
    rc = 1
    # drop patch-level part of version number, if any
    truncatedVersion = truncateVersion( neoloadVersion )
    # generate a command to modify agent.properties on the instance
    configDirPath = '~/neoload%s/conf' % truncatedVersion
    if nlWebWanted:
        cmd = "cat %s/nlweb.properties >> %s/agent.properties" % tuple( [configDirPath]*2 )
    else:
        cmd = ":"  # a null command
    cmd += " && sed -i 's/NCS_LG_PORT/%d/' %s/agent.properties" % (port, configDirPath)
    cmd += " && sed -i 's/NCS_LG_HOST/%s/' %s/agent.properties" % (forwarderHost, configDirPath)
    if nlWebWanted:
        # deployment type for nlweb
        dtype = 'SAAS' if args.nlWebUrl == 'SAAS' else 'ONPREMISE'
        cmd += " && sed -i 's/NCS_NLWEB_DTYPE/%s/g' %s/agent.properties" % (dtype, configDirPath)
        # zone for nlweb
        cmd += " && sed -i 's/NCS_NLWEB_ZONE/%s/g' %s/agent.properties" % (args.nlWebZone, configDirPath)
        escapedUrl = args.nlWebUrl.replace( '/', '\/' )
        cmd += " && sed -i 's/NCS_NLWEB_TOKEN/%s/g' %s/agent.properties" % (args.nlWebToken, configDirPath)
        cmd += " && sed -i 's/NCS_NLWEB_URL/%s/' %s/agent.properties" % (escapedUrl, configDirPath)
    logger.debug( 'info: %s', cmd )
    rc = commandInstance( inst, cmd, timeLimit=timeLimit )
    return rc

def configureAgents( instances, ports, timeLimit=600 ):
    '''configure LG agents, in parallel'''
    returnCodes = []
    with futures.ThreadPoolExecutor( max_workers=len(instances) ) as executor:
        parIter = executor.map( configureAgent, instances, ports, timeout=timeLimit )
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
        default = '{ "regions": ["asia", "europe", "middle-east", "north-america", "oceania"], "dar": ">=99", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
    )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use (default is random)' )
    ap.add_argument( '--forwarderHost', help='IP addr (or host name) of the forwarder host',
        default='localhost' )
    ap.add_argument( '--neoloadVersion', default=neoloadVersion, help='version of neoload LG agent' )
    ap.add_argument( '--nlWeb', type=ncs.boolArg, default=False, help='whether to use NeoLoad Web' )
    ap.add_argument( '--nlWebToken', help='a token for authorized access to a neoload web server' )
    ap.add_argument( '--nlWebUrl', help='the URL of a neoload web server to query' )
    ap.add_argument( '--nlWebZone', help='the neoload zone that the agents should belong to',
        default='defaultzone' )
    ap.add_argument( '--nWorkers', type=int, help='the number of agents to launch',
        default=10 )
    ap.add_argument( '--outDataDir', required=False, help='a path to the output data dir for this run' )
    ap.add_argument( '--portRangeStart', type=int, default=7100,
        help='the beginning of the range of port numbers to forward' )
    ap.add_argument( '--supportedVersions', action='store_true', help='to list supported versions and exit' )
    ap.add_argument( '--cookie' )
    args = ap.parse_args()

    supportedVersions = ['7.6', '7.7', '7.10', '7.10.0', '7.10.1', '7.11.0']
    if args.supportedVersions:
        print( json.dumps( supportedVersions ) )
        sys.exit( 0 )
    neoloadVersion =  args.neoloadVersion
    if neoloadVersion not in supportedVersions:
        logger.error( 'version "%s" is not suppoprted; supported versions are %s',
            neoloadVersion, sorted( supportedVersions ) )
        sys.exit( 1 )

    nlWebWanted = args.nlWeb

    if nlWebWanted:
        # make sure all the necessary nlWeb args were passed in non-empty
        if not args.nlWebToken:
            logger.error( 'please pass a non-empty --nlWebToken if you want to use NeoLoad Web')
        if not args.nlWebUrl:
            logger.error( 'please pass a non-empty --nlWebUrl if you want to use NeoLoad Web')
        if not (args.nlWebUrl and args.nlWebUrl):
            sys.exit( 1 )


    outDataDir = args.outDataDir
    if not outDataDir:
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = 'data/neoload_' + dateTimeTag

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
    instTimeLimit = 11*60  #  if neoloadVersion in ['7.10'] else 30*60

    nlAgentDirName = 'nlAgent'
    if not os.path.isdir( nlAgentDirName ):
        if os.path.exists( nlAgentDirName ) and not os.path.islink( nlAgentDirName ):
            logger.error( 'you have an nlAgent that is neither a dir nor a symlink')
            sys.exit( 1 )
        targetPath = os.path.join( scriptDirPath(), nlAgentDirName )
        if not os.path.isdir( targetPath ):
            logger.error( 'nlAgent dir not found in %s', scriptDirPath() )
            sys.exit( 1 )
        try:
            os.symlink( targetPath, nlAgentDirName, target_is_directory=True )
        except Exception as exc:
            logger.error( 'could not create symlink for nlAgent (%s) %s', type(exc), exc)
            sys.exit( 1 )
    logger.debug( 'nlAgent contents: %s', os.listdir(nlAgentDirName) )

    if nlWebWanted and not os.path.isfile( 'nlAgent/nlweb.properties'):
        logger.error( 'the file nlAgent/nlweb.properties was not found')
        sys.exit(1)
    try:
        # call runBatch to launch worker instances and install the load generator agent on them
        rc = batchRunner.runBatch(
            frameProcessor = neoloadFrameProcessor(),
            recruitOnly=True,
            pushDeviceLocs=False,
            commonInFilePath = 'nlAgent',
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
            #startedInstances = [inst for inst in launchedInstances if inst['state'] == 'started' ]
            #logger.info( '%d instances were launched', len(startedInstances) )

            startedInstances = [inst for inst in launchedInstances if inst['instanceId'] in recruitedIids ]

            #COULD check memory and available ports here

            truncVersion = truncateVersion( neoloadVersion )
            agentLogFilePath = '/root/.neotys/neoload/v%s/logs/agent.log' % truncVersion
            starterCmd = 'cd ~/neoload7.xx/ && /usr/bin/java -Xms50m -Xmx100m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.xx/.install4j/i4jruntime.jar:$HOME/neoload7.xx/.install4j/launchera03c11da.jar:$HOME/neoload7.xx/bin/*:$HOME/neoload7.xx/lib/crypto/*:$HOME/neoload7.xx/lib/*:$HOME/neoload7.xx/lib/jdbcDrivers/*:$HOME/neoload7.xx/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgent start & sleep 30 && free --mega 1>&2'
            starterCmd = starterCmd.replace( 'neoload7.xx', 'neoload'+truncVersion )

            if neoloadVersion == '7.7':
                starterCmd = 'cd ~/neoload7.7/ && /usr/bin/java -Xms50m -Xmx100m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.7/.install4j/i4jruntime.jar:$HOME/neoload7.7/.install4j/launchera03c11da.jar:$HOME/neoload7.7/bin/*:$HOME/neoload7.7/lib/crypto/*:$HOME/neoload7.7/lib/*:$HOME/neoload7.7/lib/jdbcDrivers/*:$HOME/neoload7.7/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgent start & sleep 30'
            elif neoloadVersion == '7.6':
                starterCmd = 'cd ~/neoload7.6/ && /usr/bin/java -Dneotys.vista.headless=true -Xmx512m -Dvertx.disableDnsResolver=true -classpath $HOME/neoload7.6/.install4j/i4jruntime.jar:$HOME/neoload7.6/.install4j/launcherc0a362f9.jar:$HOME/neoload7.6/bin/*:$HOME/neoload7.6/lib/crypto/*:$HOME/neoload7.6/lib/*:$HOME/neoload7.6/lib/jdbcDrivers/*:$HOME/neoload7.6/lib/plugins/ext/* install4j.com.neotys.nl.agent.launcher.AgentLauncher_LoadGeneratorAgentService start &'

            configuredInstances = []
            portMap = {}
            if True:  # nlWebWanted
                # configure the agent properties on each instance
                ports = list( range( portRangeStart, portRangeStart+len(startedInstances) ) )
                for index, inst in enumerate( startedInstances ):
                    iid = inst['instanceId']
                    portMap[iid] = index + portRangeStart
                logger.info( 'configuring agents')
                returnCodes = configureAgents( startedInstances, ports, timeLimit=600 )
                for index, code in enumerate( returnCodes ):
                    if code==0:
                        configuredInstances.append( startedInstances[index] )
                    else:
                        iid = startedInstances[index].get('instanceId')
                        logger.info( 'inst %s was not configured properly', iid[0:8] )

            # start the agent on each instance 
            stepStatuses = tellInstances.tellInstances( configuredInstances, command=starterCmd,
                resultsLogFilePath=outDataDir +'/startAgents.jlog',
                timeLimit=30*60,
                knownHostsOnly=True
                )
            logger.debug( 'starter statuses: %s', stepStatuses )
            # make a list of instances where the agent was started
            goodIids = []
            for status in stepStatuses:
                if isinstance( status['status'], int) and status['status'] == 0:
                    goodIids.append( status['instanceId'])
                else:
                    logger.warning( 'could not start agent on %s', status['instanceId'][0:8] )
            #COULD check bound ports again here
            #COULD download logs from all installed instances rather than just good-started instances
            goodInstances = [inst for inst in startedInstances if inst['instanceId'] in goodIids ]
            if goodInstances:
                time.sleep( 60 )
                # download the agent.log file from each instance
                stepStatuses = tellInstances.tellInstances( goodInstances,
                    download=agentLogFilePath, downloadDestDir=outDataDir +'/agentLogs',
                    timeLimit=30*60,
                    knownHostsOnly=True
                    )
                logger.debug( 'download statuses: %s', stepStatuses )
                # make a list of instances where the log file was downloaded and agent start is verified
                goodIids = []
                for status in stepStatuses:
                    if isinstance( status['status'], int) and status['status'] == 0:
                        iid = status['instanceId']
                        logFilePath = os.path.join( outDataDir, 'agentLogs', iid, 'agent.log' )
                        try:
                            with open( logFilePath, 'r' ) as logFile:
                                contents = logFile.read().rstrip()
                                if ' ERROR ' in contents:
                                    lastLine = contents.split('\n')[-1].strip()
                                    logger.warning( 'log for %s indicates error "%s"', iid[0:8], lastLine )
                                elif ': Agent started' not in contents:
                                    logger.warning( 'log for %s says it did not start', iid[0:8] )
                                else:
                                    goodIids.append( iid )
                        except Exception as exc:
                            logger.warning( 'exception reading log (%s) %s', type(exc), exc )
                    else:
                        logger.warning( 'could not download log from %s', status['instanceId'][0:8] )
                goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodIids ]
                with open( outDataDir + '/startedAgents.json','w' ) as outFile:
                    json.dump( goodInstances, outFile, indent=2 )

                # plot map of workers
                if os.path.isfile( outDataDir +'/startedAgents.json' ):
                    rc2 = subprocess.call( [scriptDirPath()+'/plotAgentMap.py', '--dataDirPath', outDataDir],
                        stdout=subprocess.DEVNULL )
                    if rc2:
                        logger.warning( 'plotAgentMap exited with returnCode %d', rc2 )
                # start the ssh port-forwarding
                logger.info( 'would forward ports for %d instances', len(goodInstances) )
                forwarders = startForwarders.startForwarders( goodInstances,
                    forwarderHost=forwarderHost,
                    portMap=portMap,
                    portRangeStart=portRangeStart, maxPort=portRangeStart+100,
                    forwardingCsvFilePath=outDataDir+'/agentForwarding.csv'
                    )
                if len( forwarders ) < len( goodInstances ):
                    logger.warning( 'some instances could not be forwarded to' )
                logger.debug( 'forwarders: %s', forwarders )
                #TODO get iids only for successfully forwarded agents
                forwardedIids = [inst['instanceId'] for inst in goodInstances ]
                unusableIids = list( set(launchedIids) - set( forwardedIids) )
                if unusableIids:
                    logger.debug( 'terminating %d unusable instances', len(unusableIids) )
                    ncs.terminateInstances( authToken, unusableIids )
                    unusableInstances = [inst for inst in launchedInstances \
                        if inst['instanceId'] in unusableIids]
                    purgeHostKeys( unusableInstances )
            if launchedInstances:
                print( 'when you want to terminate these instances, use %s %s "%s"'
                    % (sys.executable, scriptDirPath()+'/terminateAgents.py', outDataDir))
        sys.exit( rc )
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
