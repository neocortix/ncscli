#!/usr/bin/env python3
''' rsync some file(s) to some target(s)'''

# standard library modules
import argparse
import collections
import datetime
import getpass
import json
import logging
import os
import re
import socket
import subprocess
import sys
import time

# third-party modules
import jinja2
import pandas as pd

# neocortix modules
import tellInstances

logger = logging.getLogger(__name__)


def boolArg( v ):
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

class eventTiming(object):
    '''stores name and beginning and ending of an arbitrary "event"'''
    def __init__(self, eventName, startDateTime=None, endDateTime=None):
        self.eventName = eventName
        self.startDateTime = startDateTime if startDateTime else datetime.datetime.now(datetime.timezone.utc)
        self.endDateTime = endDateTime
    
    def __repr__( self ):
        return str(self.toStrList())

    def finish(self):
        self.endDateTime = datetime.datetime.now(datetime.timezone.utc)

    def duration(self):
        if self.endDateTime:
            return self.endDateTime - self.startDateTime
        else:
            return datetime.timedelta(0)

    def toStrList(self):
        return [self.eventName, 
            self.startDateTime.isoformat(), 
            self.endDateTime.isoformat() if self.endDateTime else None
            ]


def triage( statuses ):
    goodOnes = []
    badOnes = []

    for status in statuses:
        if isinstance( status['status'], int) and status['status'] == 0:
            goodOnes.append( status['instanceId'])
        else:
            badOnes.append( status )
    return (goodOnes, badOnes)

def demuxResults( inFilePath ):
    byInstance = {}
    badOnes = set()
    topLevelKeys = collections.Counter()
    # demux by instance
    with open( inFilePath, 'rb' ) as inFile:
        for line in inFile:
            decoded = json.loads( line )
            for key in decoded:
                topLevelKeys[ key ] += 1
            iid = decoded.get( 'instanceId', '<unknown>')
            have = byInstance.get( iid, [] )
            have.append( decoded )
            byInstance[iid] = have
            if 'returncode' in decoded:
                rc = decoded['returncode']
                if rc:
                    #logger.info( 'returncode %d for %s', rc, iid )
                    badOnes.add( iid )
            if 'exception' in decoded:
                #logger.info( 'exception %s for %s', decoded['exception'], iid )
                badOnes.add( iid )
    return byInstance

def parseResults( byInstance, fullDetails=True, outFile=sys.stderr ):
    outcomes = {}
    for iid, data in sorted(byInstance.items()):
        if iid == '<master>':
            continue
        outcome = {'stderr': []}
        outcomes[ iid ] = outcome
        for entry in data:
            if 'exception' in entry:                   
                outcome['exception'] = entry['exception']
            elif 'operation' in entry:
                pass
            elif 'returncode' in entry:
                outcome['returncode'] = entry['returncode']
            elif 'stderr' in entry:
                if 'ttyname' not in entry['stderr']:
                    outcome['stderr'].append( entry['stderr'] )
            elif 'stdout' in entry:
                #print( entry['dateTime'], iid[0:16], 'stdout', entry['stdout'], file=outFile )
                if 'packets transmitted, ' in entry['stdout']:
                    pat = r'(.*) packets transmitted, (.*) received, .* packet loss, time (.*)ms'
                    match = re.search( pat, entry['stdout'] )
                    if match:
                        nTrans = int( match.group(1) )
                        outcome['nTrans'] = nTrans
                        nRec = int( match.group(2) )
                        outcome['nRec'] = nRec
                        elapsedMs = int( match.group(3) )
                        outcome['elapsedMs'] = elapsedMs
                    else:
                        logger.warning( 'no regex match on "packets transmitted" line')
                elif 'rtt min/avg/max/mdev' in entry['stdout']:
                    pat = r'rtt min/avg/max/mdev = (.*)/(.*)/(.*)/(.*) ms'
                    match = re.search( pat, entry['stdout'] )
                    if match:
                        try:
                            rttMinMs = float( match.group(1) )
                            outcome['rttMinMs'] = rttMinMs
                            rttAvgMs = float( match.group(2) )
                            outcome['rttAvgMs'] = rttAvgMs
                            rttMaxMs = float( match.group(3) )
                            outcome['rttMaxMs'] = rttMaxMs
                            rttMdevMs = float( match.group(4) )
                            outcome['rttMdevMs'] = rttMdevMs
                        except Exception:
                            logger.warning( 'could not parse %s', entry['stdout'] )
                    else:
                        logger.warning( 'no regex match on "rtt min/avg/max/mdev" line')
                elif 'bytes from' in entry['stdout']:
                    pat = r'\[(.*)\] ([0-9]*) bytes from .* icmp_seq=(.*) ttl=(.*) time=(.*) ms'
                    match = re.search( pat, entry['stdout'] )
                    if match:
                        rec = {}
                        timeStamp = match.group(1)
                        rec['timeStamp'] = timeStamp
                        rxBytes = int( match.group(2) )
                        rec['rxBytes'] = rxBytes
                        seq = int( match.group(3) )
                        rec['seq'] = seq
                        ttl = int( match.group(4) )
                        rec['ttl'] = ttl
                        rttMs = float( match.group(5) )
                        rec['rttMs'] = rttMs
                        pings = outcome.get('pings', [] )
                        pings.append( rec )
                        outcome['pings'] = pings
                    else:
                        logger.warning( 'no regex match on "icmp_seq" line')
                #else:
                #    logger.info( 'other stdout: %s', entry['stdout'] )
            elif 'timeout' in entry:
                outcome['timeout'] = entry['timeout']
            else:
                logger.info( 'UNRECOGNIZED %s', entry )
    return outcomes

def reportSummaryWithLocs( outcomes, instances, outFile ):
    # may not be needed
    instancesByIid = {}
    for inst in loadedInstances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst

    for iid, data in sorted(outcomes.items()):
        loc = instancesByIid[iid]['device-location']
        if not data.get('nRec', 0):
            logger.info( 'ANOMALY %s', data )
            continue
        rttAvgMs = data.get('rttAvgMs', 0)
        if not rttAvgMs:
            logger.info( 'no rttAvgMs in %s', data )
            continue
        if (rttAvgMs < 0) or (rttAvgMs >= 1000000):
            logger.info( 'bad rttAvgMs in %s', data )
            continue
        print( iid[0:16],
            loc['country-code'], loc['area'], loc['latitude'], loc['longitude'],
            data['nRec'], rttAvgMs,
            file=outFile )

def mergeInstanceData( outcomes, instances ):
    instancesByIid = {}
    for inst in instances:
        iid = inst['instanceId']
        instancesByIid[iid] = inst

    recs = []

    for iid, data in sorted(outcomes.items()):
        loc = instancesByIid[iid]['device-location']
        if not data.get('nRec', 0):
            continue
        rttAvgMs = data.get('rttAvgMs', 0)
        if not rttAvgMs:
            continue
        if (rttAvgMs < 0) or (rttAvgMs >= 1000000):
            continue
        if not loc.get('country-code'):
            continue
        rec = {'iid': iid,
            'country-code': loc['country-code'],
            'state': loc['area'],
            'locality': loc['locality'],
            'locKey': loc['country-code'] + '.' + (loc.get('area') or '<unknown>'),
            'lat': loc['latitude'], 
            'lon': loc['longitude'],
            'nRec': data['nRec'], 
            'rttAvgMs': rttAvgMs
        }
        recs.append( rec )
    return recs

def getRegionSummaries( outcomes, instances ):
    recs = mergeInstanceData( outcomes, instances )
    perInst = pd.DataFrame( recs )
    locKeys = perInst['locKey'].unique()
    logger.info( 'found %d locKeys for %d instances', len(locKeys), len(outcomes) )
    logger.info( 'locKeys %s', sorted(locKeys) )

    perRegion = pd.DataFrame()
    for locKey in locKeys:
        #print( '\nRegion: ', locKey )
        subset = perInst[ perInst.locKey == locKey ]
        summary = { 'locKey': locKey,
            'devices': len(subset),
            'lat': subset.lat.mean(),
            'lon': subset.lon.mean(),
            'nRec': subset.nRec.sum(),
            #'rttAvgMs': subset.rttAvgMs.mean()  # FIX THIS MATH
            'rttAvgMs': (subset.rttAvgMs * subset.nRec).sum() / subset.nRec.sum()
        }
        perRegion = perRegion.append( [summary] )
    #logger.info( 'perRegion %s', perRegion )

    return perRegion

def exportLocDataJson( placeInfo, outFilePath ):
    # export location info from dataframe to a json file
    locList = []
    for index, row in placeInfo.iterrows():
        #print( row.latitude, row.longitude )
        locList.append( {'lat': row.lat, 'lon': row.lon, 'rttAvgMs': row.rttAvgMs} )
    locObj = {'totPings': int(placeInfo.nRec.sum()) }
    locObj['locs'] = locList
    
    with open( outFilePath, 'w') as outFile:
        json.dump( locObj, outFile, indent=2 )

def exportLocDataJs( placeInfo, outFilePath ):
    # convert lat/lon values from dataframe into list of coordinate pairs
    pairList = []
    for index, row in placeInfo.iterrows():
        #print( row.latitude, row.longitude )
        pairList.append( [ row.lat, row.lon ])
    # jsonize
    pairListJs = json.dumps( pairList ) 
    rttJs = json.dumps( list( placeInfo.rttAvgMs) ) 
    
    with open( outFilePath, 'w') as outFile:
        print( 'var hitCount1 = %d;\n' % (placeInfo.nRec.sum()), file=outFile )
        print( 'var locations = \n%s;\n' % (pairListJs), file=outFile )
        print( 'var rtts = \n%s;\n' % (rttJs), file=outFile )

def reportResults( byInstance, fullDetails, outFile ):
    logger.info( '%d instance keys', len(byInstance) )
    for iid, data in sorted(byInstance.items()):
        if iid == '<master>':
            continue
        for entry in data:
            if 'exception' in entry:                   
                print( entry['dateTime'], iid[0:16], 'exception',
                    entry['exception']['type'], entry['exception']['msg'], file=outFile )
            elif 'operation' in entry:
                pass
            elif 'returncode' in entry:
                if entry['returncode']:
                    print( entry['dateTime'], iid[0:16], 'returncode', entry['returncode'], file=outFile )
            elif 'stderr' in entry:
                if 'ttyname' not in entry['stderr']:
                    print( entry['dateTime'], iid[0:16], 'stderr', entry['stderr'], file=outFile )
            elif 'stdout' in entry:
                #print( entry['dateTime'], iid[0:16], 'stdout', entry['stdout'], file=outFile )
                if fullDetails:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
                elif 'packets transmitted, ' in entry['stdout']:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
                elif 'rtt min/avg/max/mdev' in entry['stdout']:
                    print( entry['dateTime'], iid[0:16], entry['stdout'], file=outFile )
            elif 'timeout' in entry:
                print( entry['dateTime'], iid[0:16], 'timeout', entry['timeout'], file=outFile )
            else:
                print( entry['dateTime'], iid[0:16], 'UNRECOGNIZED', entry, file=outFile )

    #if outFilePath:
    #    with open( outFilePath, 'w') as outFile:
    #        json.dump( byInstance, outFile, indent=2 )

def renderStatsHtml( regionTable ):
    if True:
        envir = jinja2.Environment(
                loader = jinja2.FileSystemLoader(sys.path),
                autoescape=jinja2.select_autoescape(['html', 'xml'])
                )
        template = envir.get_template('stats.html.j2')
        html = template.render( regionTable=regionTable,
            otherTable=None )
    else:
        html = '<html> <body>\n%s\n</body></html>\n' % regionTable
    return html

def pingFromInstances( instanceJsonFilePath, dataDirPath, wwwDirPath, targetHost, 
    nPings, interval, timeLimit, extraTime,
    fullDetails, sshAgent
    ):
    eventTimings = []
    starterTiming = eventTiming('startup')

    os.makedirs( dataDirPath, exist_ok=True )
    os.makedirs( wwwDirPath, exist_ok=True )

    resultsLogFilePath = dataDirPath + '/pingFromInstances.jlog'
    # truncate the resultsLogFile
    with open( resultsLogFilePath, 'wb' ) as xFile:
        pass # xFile.truncate()

    loadedInstances = None
    with open( instanceJsonFilePath, 'r' ) as jsonFile:
        loadedInstances = json.load(jsonFile)  # a list of dicts
    startedInstances = [inst for inst in loadedInstances if inst['state'] == 'started' ]

    fieldsWanted = ['instanceId', 'state', 'ssh', 'device-location']
    strippedInstances = []
    for inst in startedInstances:
        stripped = { key: inst[key] for key in fieldsWanted }
        strippedInstances.append( stripped )

    #startedInstances = [inst for inst in strippedInstances if inst['state'] == 'started' ]
    goodInstances = strippedInstances

    if extraTime == None:
        extraTime = 10 + len(goodInstances)/10
    logger.info( 'using extraTime %.1f', extraTime )

    # if no targetHost specified try using this host by fully-qualified name
    # sometimes the fqdn does not work
    if not targetHost:
        targetHost = socket.getfqdn()
    logger.info( 'using targetHost %s', targetHost )

    starterTiming.finish()
    eventTimings.append(starterTiming)

    allBad = []


    pingCmd = 'ping %s -U -D -c %s -w %f -i %f' \
        % (targetHost, nPings, timeLimit,  interval )
    # could use -s for different payload size
    # could use -t (ttl) to limit nHops
    if not fullDetails:
        pingCmd += ' -q'
    # tell them to ping
    stepTiming = eventTiming('tellInstances ping')
    logger.info( 'calling tellInstances')
    stepStatuses = tellInstances.tellInstances( startedInstances, pingCmd,
        resultsLogFilePath=resultsLogFilePath,
        download=None, downloadDestDir=None, jsonOut=None, sshAgent=sshAgent,
        timeLimit=timeLimit+extraTime, upload=None
        )
    stepTiming.finish()
    eventTimings.append(stepTiming)
    (goodOnes, badOnes) = triage( stepStatuses )
    allBad.extend( badOnes )
    logger.info( 'ping %d good', len(goodOnes) )
    #logger.info( 'ping bad %s', badOnes )

    goodInstances = [inst for inst in goodInstances if inst['instanceId'] in goodOnes ]


    logger.info( 'allBad has %d instances', len(allBad) )
    if False:
        for badInst in allBad:
            status = badInst['status']
            logger.info( '%s status (%s) %s', badInst['instanceId'][0:16], type(status), status )


    resultsByInstance = demuxResults( resultsLogFilePath )
    outcomes = parseResults( resultsByInstance )

    reportResults( resultsByInstance, fullDetails, sys.stdout )
    if len(outcomes):
        merged = mergeInstanceData( outcomes, startedInstances )
        perInstance = pd.DataFrame( merged )
        perInstance.to_csv( dataDirPath+'/instanceSummaries.csv', index=False)
        if 'rttAvgMs' in perInstance:
            # exportLocDataJs( perInstance, wwwDirPath+'/locations.js')  # old way
            exportLocDataJson( perInstance, wwwDirPath+'/locInfo.json')

            perRegion = getRegionSummaries( outcomes, startedInstances )
            perRegion.to_csv( dataDirPath+'/regionSummaries.csv', index=False)
            #whatever perRegion.to_html( dataDirPath+'/regionSummaries.csv', index=False)

            #reportSummaryWithLocs( outcomes, startedInstances, sys.stderr )
            #json.dump( outcomes, sys.stdout, indent=2 )

            colsToRender = ['locKey', 'devices', 'nRec', 'lat', 'lon', 'rttAvgMs']
            if False:
                # can render one or more dataframes to html using template, if desired
                html = renderStatsHtml( perRegion[colsToRender].to_html(index=False,
                    classes=['sortable'], justify='left', float_format=lambda x: '%.1f' % x
                    ) )
                with open( wwwDirPath+'/stats.html', 'w', encoding='utf8') as htmlOutFile:
                    htmlOutFile.write( html )
            html = perRegion[colsToRender].to_html(index=False,
                classes=['sortable'], justify='left', float_format=lambda x: '%.1f' % x
                )
            # remove the deprecated "border" attribute setting from the generated html
            html = html.replace('border="1" ','')
            with open( wwwDirPath+'/areaTable.htm', 'w', encoding='utf8') as htmlOutFile:
                htmlOutFile.write( html )


    # not sure if these help
    sys.stdout.flush()
    sys.stderr.flush()
    

if __name__ == "__main__":
    # configure logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')
    tellInstances.logger.setLevel(logging.INFO)

    dataDirPath = 'data'
    wwwDirPath = 'www'

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument('instanceJsonFilePath', default=dataDirPath+'/launched.json')
    ap.add_argument('--sshAgent', type=boolArg, default=False, help='whether or not to use ssh agent')
    ap.add_argument('--extraTime', type=float, help='extra time (in seconds) for master to wait for results')
    ap.add_argument('--fullDetails', type=boolArg, default=False, help='true for full details, false for summaries only')
    ap.add_argument('--interval', type=float, default=1, help='time (in seconds) between pings by an instance')
    ap.add_argument('--nPings', type=int, default=10, help='# of ping packets to send per instance')
    ap.add_argument('--timeLimit', type=float, default=10, help='maximum time (in seconds) to take per instance' )
    ap.add_argument('--targetHost', help='the hostname or ip addr to ping (default is this host)' )
    args = ap.parse_args()
    logger.info( "args: %s", str(args) )

    startTime = time.time()

    pingFromInstances( args.instanceJsonFilePath, dataDirPath, wwwDirPath, args.targetHost, 
        args.nPings, args.interval, args.timeLimit, args.extraTime,
        args.fullDetails, args.sshAgent
        )
    elapsed = time.time() - startTime
    logger.info( 'finished; elapsed time %.1f seconds (%.1f minutes)', elapsed, elapsed/60 )
