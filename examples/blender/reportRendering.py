#!/usr/bin/env python3
"""
reports results of rendering tests using data from mongodb
"""
# standard library modules
import argparse
import collections
import datetime
import json
import logging
import math
import os
import shutil
import sys
import time

# third-party modules
import dateutil.parser
import pandas as pd
import pymongo
import requests

# neocortix module(s)
import ncs

logger = logging.getLogger(__name__)


def datetimeIsAware( dt ):
    if not dt: return None
    return (dt.tzinfo is not None) and (dt.tzinfo.utcoffset( dt ) is not None)

def universalizeDateTime( dt ):
    if not dt: return None
    if datetimeIsAware( dt ):
        #return dt
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace( tzinfo=datetime.timezone.utc )

def demuxResults( collection ):
    '''deinterleave jlog-like items into separate lists for each instance'''
    byInstance = {}
    badOnes = set()
    topLevelKeys = collections.Counter()
    # demux by instance
    for decoded in collection.find( {}, hint=[('dateTime', pymongo.ASCENDING)] ):
        for key in decoded:
            topLevelKeys[ key ] += 1
        iid = decoded.get( 'instanceId', '<unknown>')
        have = byInstance.get( iid, [] )
        have.append( decoded )
        byInstance[iid] = have
        if 'returncode' in decoded:
            rc = decoded['returncode']
            if rc:
                logger.info( 'returncode %d for %s', rc, iid )
                badOnes.add( iid )
        if 'exception' in decoded:
            logger.info( 'exception %s for %s', decoded['exception'], iid )
            badOnes.add( iid )
        if 'timeout' in decoded:
            logger.info( 'timeout %s for %s', decoded['timeout'], iid )
            badOnes.add( iid )
    logger.info( 'topLevelKeys %s', topLevelKeys )
    return byInstance, badOnes

def getCountryCodeGoogle( lat, lon ):
    apiKey = 'AIzaSyARYkShv9PYdB9lonMjaOChIKjPtoFHZFM'
    reqUrl = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=%f,%f&key=%s&result_type=country' \
        % (lat, lon, apiKey )
    resp = requests.get( reqUrl )
    time.sleep( 0.1 )  # they may not want us calling too often
    respJson = resp.json()
    if respJson.get('status') == 'OK':
        results = respJson['results'] # an array containing 1 dict
        components = results[0]['address_components'] # an array containing 1 dict
        countryCode = components[0]['short_name']
        return countryCode
    else:
        return None

def interpretDateTimeField( field ):
    if isinstance( field, datetime.datetime ):
        return universalizeDateTime( field )
    elif isinstance( field, str ):
        return dateutil.parser.parse( field )
    else:
        raise TypeError( 'datetime or parseable string required' )


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--tag', help='database collection-naming tag' )
    ap.add_argument('--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument('--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument('--outDir', help='directory path for output files', default='.')
    args = ap.parse_args()
    #logger.debug( 'args %s', args )

    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    logsDb = mclient.renderingLogs
    # get the list of all coillections in the db (avoiding using 'collections' as a global var name)
    tables = sorted(logsDb.list_collection_names())
    logger.info( 'database collections %s', tables )

    if args.tag:
        launchedCollName = 'launchedInstances_' + args.tag
        installerCollName = 'installerLog_' + args.tag
        rendererCollName = 'rendererLog_' + args.tag
    else:
        # use the latest test from the 'officialTests' collection
        officialColl = logsDb[ 'officialTests' ]
        tests = list( officialColl.find() )
        if not tests:
            sys.exit( 'no officialTests found' )
        #logger.info( 'all offical tests: %s', tests )
        testsDf = pd.DataFrame( tests )
        testsDf.sort_values( 'dateTime', inplace=True )
        latestTest = testsDf.iloc[-1]  # the last row
        logger.info( 'found %d official tests, latest at %s', len(testsDf), testsDf.dateTime.max() )
        logger.info( 'using test with tag %s, from %s', latestTest.tag, latestTest.dateTime )
        launchedCollName = latestTest.launchedInstances
        installerCollName = latestTest.installerLog
        rendererCollName = latestTest.rendererLog

    if launchedCollName not in tables:
        sys.exit( 'could not find collection ' + launchedCollName )
    if installerCollName not in tables:
        sys.exit( 'could not find collection ' + installerCollName )
    if rendererCollName not in tables:
        sys.exit( 'could not find collection ' + rendererCollName )
        
    os.makedirs( args.outDir, exist_ok=True )
    instAttemptsFilePath = args.outDir + '/instAttempts.csv'
    frameSummariesFilePath = args.outDir + '/frameSummaries.csv'
    workerSummariesFilePath = args.outDir + '/workerSummaries.csv'


    # gather instances from launchedInstances collection into a dict
    instancesAllocated = {}
    launchedColl = logsDb[launchedCollName]
    inRecs = launchedColl.find()
    for inRec in inRecs:
        if 'instanceId' not in inRec:
            logger.warning( 'no instance ID in input record')
            continue
        iid = inRec['instanceId']
        instancesAllocated[ iid ] = inRec
    logger.info( 'found %d instances in collection %s', len(instancesAllocated), launchedCollName )
    
    # get events by instance from the installer log
    (eventsByInstance, badIids) = demuxResults( logsDb[installerCollName] )
    logger.info( 'badIids (%d) %s', len(badIids), badIids )

    # update state from the installer jlog (modifies instances in instancesAllocated)
    nSucceeded = 0
    nFailed = 0
    nExceptions = 0
    nTimeout = 0
    tellInstancesDateTime = None
    for iid, events in eventsByInstance.items():
        connectingDateTime = None
        connectingDur = None
        installingDateTime = None
        stderrLines = []
        for event in events:
            if 'operation' in event and 'tellInstances' in event['operation']:
                tellInstancesDateTime = interpretDateTimeField( event['dateTime'] )
                try:
                    launchedIids = event["operation"][1]["args"]["instanceIds"]
                    logger.info( '%d instances were launched', len(launchedIids) )
                except Exception as exc:
                    logger.info( 'exception ignored for tellInstances op (%s)', type(exc) )
            if 'operation' in event and 'connect' in event['operation']:
                # start of ssh connection for this instance
                connectingDateTime = interpretDateTimeField( event['dateTime'] )
                #logger.info( 'installer connecting %s %s', iid, connectingDateTime )
                instancesAllocated[iid]['connectingDateTime'] = connectingDateTime
            if 'operation' in event and 'command' in event['operation']:
                # start of installation for this instance
                installingDateTime = interpretDateTimeField( event['dateTime'] )
                #logger.info( 'installer starting %s %s', iid, installingDateTime )
                connectingDur = (installingDateTime-connectingDateTime).total_seconds()
            if 'stderr' in event:
                stderrLines.append( event['stderr'] )

            # calculate and store duration when getting an ending event
            if ('returncode' in event) or ('exception' in event) or ('timeout' in event):
                iid = event['instanceId']
                endDateTime = interpretDateTimeField( event['dateTime'] )
                sdt = connectingDateTime
                if not connectingDateTime:
                    logger.info( 'endDateTime %s, connectingDateTime %s, iid %s',
                        endDateTime, connectingDateTime, iid )
                    sdt = tellInstancesDateTime
                dur = (endDateTime-sdt).total_seconds()
                instancesAllocated[iid]['dur'] = dur
                instancesAllocated[iid]['connectingDur'] = connectingDur
                instancesAllocated[iid]['stderrLines'] = stderrLines
            if 'returncode' in event:
                iid = event['instanceId']
                installerCode = event['returncode']
                if installerCode:
                    nFailed += 1
                else:
                    nSucceeded += 1
                state = 'installerFailed' if installerCode else 'installed'
                instancesAllocated[iid]['state'] = state
                instancesAllocated[iid]['instCode'] = installerCode
            elif 'exception' in event:
                iid = event['instanceId']
                installerCode = event['exception']['type']
                nExceptions += 1
                instancesAllocated[iid]['state'] = 'installerException'
                instancesAllocated[iid]['instCode'] = installerCode
            elif 'timeout' in event:
                iid = event['instanceId']
                installerCode = event['timeout']
                nTimeout += 1
                instancesAllocated[iid]['state'] = 'installerTimeout'
                instancesAllocated[iid]['instCode'] = installerCode
    logger.info( '%d succeeded, %d failed, %d timeout, %d exceptions',
        nSucceeded, nFailed, nTimeout, nExceptions)

    if False:
        # enable this code to print more details (mainly stderr if available) for non-good instances
        for iid in badIids:
            abbrevIid = iid[0:16]
            print()
            #logger.info( 'details for failing instance %s', abbrevIid )
            inst = instancesAllocated[ iid ]
            locInfo = inst['device-location']
            print( '>>', inst['instanceId'], inst['state'], 'with code', inst['instCode'], 'on device', inst['device-id'] )
            if locInfo:
                print( '>>>', locInfo['country-code'] )
            print( inst['stderrLines'][-5:] )

    if instAttemptsFilePath:
        dbPreexisted = os.path.isfile( instAttemptsFilePath )

        if True:  #  not dbPreexisted:
            with open( instAttemptsFilePath, 'w' ) as csvOutFile:
                print( 'eventType,devId,state,code,dateTime,dur,instanceId,country,sshAddr,storageFree,ramTotal,arch,nCores,freq1,freq2,families,appVersion,ref',
                    file=csvOutFile )

        with open( instAttemptsFilePath, 'a' ) as csvOutFile:
            for iid in instancesAllocated:
                inst = instancesAllocated[iid]
                #iid = inst['instanceId']
                arch = ''    
                families = set()
                nCores = 0
                maxFreq = 0
                minFreq = float("inf")
                devId = 0
                appVersion = 0
                ramTotal = 0
                storageFree = 0
                sshAddr = ''

                instCode = inst.get( 'instCode')
                startDateTime = inst.get( 'connectingDateTime' )
                startDateTimeStr = startDateTime.isoformat() if startDateTime else None
                #if not startDateTime:
                #    startDateTime = eventDateTime

                if 'device-id' in inst:
                    devId = inst['device-id']
                else:
                    logger.warning( 'no device id for instance "%s"', iid )

                if 'app-version' in inst:
                    appVersion = inst['app-version']['code']
                else:
                    logger.warning( 'no app-version for instance "%s"', iid )

                countryCode = None
                if 'device-location' in inst:
                    locInfo = inst['device-location']
                    countryCode = locInfo.get( 'country-code' )
                    if not countryCode:
                        #logger.info( 'mystery location %s', inst['device-location'] )
                        countryCode = getCountryCodeGoogle( locInfo['latitude'], locInfo['longitude'] )
                        if not countryCode:
                            countryCode = str(locInfo['latitude']) + ';' + str(locInfo['longitude'])
                if 'ram' in inst:
                    ramTotal = inst['ram']['total'] / 1000000
                if 'ssh' in inst:
                    sshAddr = inst['ssh']['host'] + ':' + str(inst['ssh']['port'])
                if 'storage' in inst:
                    storageFree = inst['storage']['free'] / 1000000
                if 'cpu' in inst:
                    details = inst['cpu']
                    arch = details['arch']
                    nCores = len( details['cores'] )
                    for core in details['cores']:
                        families.add( '%s-%s' % (core['vendor'], core['family']) )
                        freq = core['freq']
                        maxFreq = max( maxFreq, freq )
                        if freq:
                            minFreq = min( minFreq, freq )
                        else:
                            logger.info( 'zero freq in inst %s', inst )
                    maxFreq = maxFreq / 1000000
                    if minFreq < float("inf"):
                        minFreq = minFreq / 1000000
                    else:
                        minFreq = 0
                    #print( details['arch'], len( details['cores']), families )
                else:
                    logger.warning( 'no "cpu" info found for instance %s (dev %d)',
                        iid, devId )
                instDur = inst.get( 'dur', 0 )
                print( 'launch_install,%d,%s,%s,%s,%.0f,%s,%s,%s,%.1f,%.1f,%s,%s,%.1f,%.1f,"%s",%d,%s' %
                    (devId, inst.get('state'), instCode, startDateTimeStr,
                    instDur, iid, countryCode, sshAddr,
                     storageFree, ramTotal, arch, nCores, maxFreq, minFreq,
                     families, appVersion, installerCollName)
                    , file=csvOutFile )

    # get events by instance from the renderer jlog
    (byInstance, _badIids) = demuxResults( logsDb[rendererCollName] )
    # _badIids is expected to be always empty (because demux was designed for installer jlogs)

    blendFilePath = '<unknown>'
    allErrMsgs = collections.Counter()  # set()
    badIids = set()
    goodIids = set()
    prStartDateTime = None
    sumRecs = []  # building a list of frameSummary records
    for iid, events in byInstance.items():
        #logger.info( '%s had %d events', iid, len(events) )
        if iid in instancesAllocated:
            devId = instancesAllocated[iid].get( 'device-id' )
        else:
            devId = 0
        seemedSlow = False
        startDateTime = None
        retrievingDateTime = None
        stderrEvents = []
        for event in events:
            eventType = event.get('type')
            eventArgs = event.get('args')
            if eventType == 'operation':
                if 'parallelRender' in eventArgs:
                    parallelRenderOp = eventArgs['parallelRender']
                    logger.info( 'parallelRender %s', parallelRenderOp )
                    if blendFilePath != '<unknown>':
                        logger.warning( 'replacing blendFilePath %s' )
                    blendFilePath = parallelRenderOp['blendFilePath']
                    logger.info( 'blendFilePath %s', blendFilePath )
                    prStartDateTime = interpretDateTimeField( event['dateTime'] )
            elif eventType == 'stderr':
                stderrEvents.append( event )
            elif eventType == 'stdout':
                #logger.debug( 'stdout %s', eventArgs ) # could do something with these
                pass
            elif eventType == 'frameState':
                frameState = eventArgs['state']
                frameNum = eventArgs['frameNum']
                rc = eventArgs['rc']

                # in reporting, rsync of the blend file is treated as if it were frame # -1 (minus one)
                if frameState in ['rsyncing', 'starting']:
                    startDateTime = interpretDateTimeField( event['dateTime'] )
                if frameState == 'retrieving':
                    retrievingDateTime = interpretDateTimeField( event['dateTime'] )
                if frameState == 'seemsSlow':
                    if seemedSlow:
                        continue  # avoid verbosity for already-slow cases
                    seemedSlow = True
                # save a record if this is the end of a frame attempt
                if frameState in ['rsynced', 'retrieved', 'renderFailed', 'retrieveFailed', 'rsyncFailed']:
                    errMsg = None
                    if len( stderrEvents ):
                        errMsg = stderrEvents[-1]['args'].strip()
                        #allErrMsgs.add( errMsg )
                        allErrMsgs[errMsg] += 1
                    endDateTime = interpretDateTimeField( event['dateTime'] )
                    sdt = startDateTime
                    if not startDateTime:
                        logger.info( 'endDateTime %s, startDateTime %s, iid %s', endDateTime, startDateTime, iid )
                        sdt = prStartDateTime
                    dur = (endDateTime-sdt).total_seconds()
                    retrievingDur = None
                    if frameState in ['retrieved', 'retrieveFailed'] and retrievingDateTime:
                        retrievingDur = (endDateTime-retrievingDateTime).total_seconds()

                    sumRec = { 'instanceId': iid, 'devId': devId, 'blendFilePath': blendFilePath,
                        'frameNum': frameNum, 'frameState': frameState,
                        'startDateTime': startDateTime, 'endDateTime': endDateTime,
                        'dur': dur, 'retrievingDur': retrievingDur,
                        'rc': rc, 'slowness': seemedSlow, 'errMsg': errMsg
                    }
                    sumRecs.append( sumRec )
                    if frameState in ['renderFailed', 'retrieveFailed', 'rsyncFailed']:
                        badIids.add( iid )
                    elif frameState == 'retrieved':
                        goodIids.add( iid )

    logger.info( 'allErrMsgs %s', allErrMsgs )
    frameSummaries = pd.DataFrame( sumRecs )
    isoFormat = '%Y-%m-%dT%H:%M:%S.%f%z'
    if frameSummariesFilePath:
        frameSummaries.to_csv( frameSummariesFilePath, index=False, date_format=isoFormat )

    workerIids = [iid for iid in byInstance.keys() if '<' not in iid]
    logger.info( '%d worker instances', len(workerIids))
    logger.info( '%d good instances', len(goodIids))
    logger.info( '%d bad instances', len(badIids))
    logger.info( '%d partly-good instances', len( goodIids & badIids ))

    # traverse the workers and generate a summary table
    sumRecs = []
    for iid in sorted( workerIids ):
        devId = devId = instancesAllocated[iid].get( 'device-id' )
        subset = frameSummaries[ frameSummaries.instanceId == iid ]
        #nAttempts = len( subset )
        nAttempts = len( frameSummaries[ (frameSummaries.instanceId == iid) & (frameSummaries.frameNum >=0) ]  )
        nRenderFailed = (subset.frameState=='renderFailed').sum()
        nRetrieveFailed = (subset.frameState=='retrieveFailed').sum()
        nRsyncFailed = (subset.frameState=='rsyncFailed').sum()
        successes = subset[ subset.frameState == 'retrieved']
        meanDurGood = successes.dur.mean()
        if math.isnan( meanDurGood ):
            meanDurGood = 0  # force nans to zero
        logger.info( '%s dev %d, %d attempts, %d good, %d renderFailed, %d other, %.1f meanDurGood',
            iid[0:16], devId, nAttempts, len(successes),
            nRenderFailed, nRetrieveFailed+nRsyncFailed,
            meanDurGood
            )
        sumRec = { 'instanceId': iid, 'devId': devId, 'blendFilePath': blendFilePath,
            'nAttempts': nAttempts, 'nGood': len(successes), 'nRenderFailed': nRenderFailed,
            'nRetrieveFailed': nRetrieveFailed, 'nRsyncFailed': nRsyncFailed,
            'meanDurGood': meanDurGood
        }
        sumRecs.append( sumRec )
    workerSummaries = pd.DataFrame( sumRecs )
    if workerSummariesFilePath:
        workerSummaries.to_csv( workerSummariesFilePath, index=False )
