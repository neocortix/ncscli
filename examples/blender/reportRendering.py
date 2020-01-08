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
import devicePerformance
import ncs

logger = logging.getLogger(__name__)


def boolArg( v ):
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

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

def instanceDpr( inst ):
    #logger.info( 'NCSC Inst details %s', inst )
    # cpuarch:      string like "aarch64" or "armv7l"
    # cpunumcores:  int
    # cpuspeeds:    list of floats of length cpunumcores, each representing a clock frequency in GHz
    # cpufamily:    list of strings of length cpunumcores
    cpuarch = inst['cpu']['arch']
    cpunumcores = len( inst['cpu']['cores'])
    cpuspeeds = []
    cpufamily = []
    for core in inst['cpu']['cores']:
        cpuspeeds.append( core['freq'] / 1e9)
        cpufamily.append( core['family'] )
    
    dpr = devicePerformance.devicePerformanceRating( cpuarch, cpunumcores, cpuspeeds, cpufamily )
    #print( 'device', inst['device-id'], 'dpr', dpr )
    if dpr < 37:
        logger.info( 'unhappy dpr for dev %d with cpu %s', inst['device-id'], inst['cpu'] )
    return dpr

def interpretDateTimeField( field ):
    if isinstance( field, datetime.datetime ):
        return universalizeDateTime( field )
    elif isinstance( field, str ):
        return dateutil.parser.parse( field )
    else:
        raise TypeError( 'datetime or parseable string required' )

def summarizeInstallerLog( eventsByInstance, instancesByIid, installerCollName ):
    # update state from the installer jlog (modifies instances in instancesByIid)
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
                instancesByIid[iid]['connectingDateTime'] = connectingDateTime
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
                instancesByIid[iid]['dur'] = dur
                instancesByIid[iid]['connectingDur'] = connectingDur
                instancesByIid[iid]['stderrLines'] = stderrLines
            if 'returncode' in event:
                iid = event['instanceId']
                installerCode = event['returncode']
                if installerCode:
                    nFailed += 1
                else:
                    nSucceeded += 1
                state = 'installerFailed' if installerCode else 'installed'
                instancesByIid[iid]['state'] = state
                instancesByIid[iid]['instCode'] = installerCode
            elif 'exception' in event:
                iid = event['instanceId']
                installerCode = event['exception']['type']
                nExceptions += 1
                instancesByIid[iid]['state'] = 'installerException'
                instancesByIid[iid]['instCode'] = installerCode
            elif 'timeout' in event:
                iid = event['instanceId']
                installerCode = event['timeout']
                nTimeout += 1
                instancesByIid[iid]['state'] = 'installerTimeout'
                instancesByIid[iid]['instCode'] = installerCode
    logger.info( '%d succeeded, %d failed, %d timeout, %d exceptions',
        nSucceeded, nFailed, nTimeout, nExceptions)
    sumRecs = []
    for iid in instancesAllocated:
        inst = instancesAllocated[iid]
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
        dpr = instanceDpr( inst )
        #inst['dpr'] = dpr
        instDur = inst.get( 'dur', 0 )
        sumRec = {
            'devId': devId, 'state': inst.get('state'), 'code': instCode,
            'dateTime': startDateTimeStr,
            'dur': instDur, 'instanceId': iid, 'countryCode': countryCode, 'sshAddr': sshAddr,
            'storageFree':storageFree, 'ramTotal':ramTotal, 'arch':arch, 
            'nCores':nCores, 'freq1':maxFreq, 'freq2':minFreq, 'dpr': dpr,
            'families':families, 'appVersion':appVersion, 'installerCollName':installerCollName
        }
        sumRecs.append( sumRec )
    return sumRecs

def summarizeRenderingLog( instancesAllocated, rendererCollName, tag=None ):
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
        frameStartTimes = {}
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
                if '184331e1' in iid:
                    logger.info( 'CHECK %s %s %s', event['dateTime'], eventType, eventArgs.get('state') )
                frameState = eventArgs['state']
                frameNum = eventArgs['frameNum']
                rc = eventArgs['rc']

                # in reporting, rsync of the blend file is treated as if it were frame # -1 (minus one)
                if frameState in ['rsyncing', 'starting']:
                    startDateTime = interpretDateTimeField( event['dateTime'] )
                    frameStartTimes[frameNum] = startDateTime
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
                    if (rc == 1) and ('closed by remote host' in errMsg):
                        rc = 255
                    endDateTime = interpretDateTimeField( event['dateTime'] )
                    sdt = frameStartTimes.get(frameNum)  # startDateTime
                    if not sdt:
                        logger.warning( 'endDateTime %s, startDateTime %s, iid %s', endDateTime, sdt, iid )
                        sdt = prStartDateTime
                    dur = (endDateTime-sdt).total_seconds()
                    if dur <= 0:
                        logger.warning( 'zero duration for %s frame %d', iid, frameNum )
                    retrievingDur = None
                    if frameState in ['retrieved', 'retrieveFailed'] and retrievingDateTime:
                        retrievingDur = (endDateTime-retrievingDateTime).total_seconds()

                    sumRec = { 'instanceId': iid, 'devId': devId, 'blendFilePath': blendFilePath,
                        'frameNum': frameNum, 'frameState': frameState,
                        'startDateTime': sdt, 'endDateTime': endDateTime,
                        'dur': dur, 'retrievingDur': retrievingDur,
                        'rc': rc, 'slowness': seemedSlow, 'errMsg': errMsg,
                        'tag': tag
                    }
                    sumRecs.append( sumRec )
                    if frameState in ['renderFailed', 'retrieveFailed', 'rsyncFailed']:
                        badIids.add( iid )
                    elif frameState == 'retrieved':
                        goodIids.add( iid )
    return sumRecs

def summarizeWorkers( _workerIids, instances, frameSummaries ):
    devList = frameSummaries.devId.unique()
    devIds = set( devList )
    logger.info( 'devIds (%d), %s)', len(devIds), devIds )
    # traverse the workers and generate a summary table
    sumRecs = []
    for devId in sorted( list(devIds) ):
        subset = frameSummaries[ frameSummaries.devId == devId ]
        #logger.info( 'subset for dev %d contains %d rows', devId, len(subset) )
        iid = subset.instanceId.unique()
        #if len( iid ) > 1:
        #    logger.warning( 'more than one instanceId for device %d in frameSummaries', devId)
        iid = iid[0]
        dpr = instances[iid].get('dpr')
        blendFilePaths = subset.blendFilePath.unique()
        if len( blendFilePaths ) > 1:
            logger.warning( 'more than one blendFilePath in frameSummaries')
        blendFilePath = blendFilePaths[0]
        #nAttempts = len( subset )
        nAttempts = len( frameSummaries[ (frameSummaries.devId == devId) & (frameSummaries.frameNum >=0) ]  )
        nRenderFailed = (subset.frameState=='renderFailed').sum()
        nRetrieveFailed = (subset.frameState=='retrieveFailed').sum()
        nRsyncFailed = (subset.frameState=='rsyncFailed').sum()
        nSlow = (subset.slowness==True).sum()
        successes = subset[ subset.frameState == 'retrieved']
        meanDurGood = successes.dur.mean()
        if math.isnan( meanDurGood ):
            meanDurGood = 0  # force nans to zero
        '''
        logger.info( '%s dev %d, %d attempts, %d good, %d renderFailed, %d other, %.1f meanDurGood',
            iid[0:16], devId, nAttempts, len(successes),
            nRenderFailed, nRetrieveFailed+nRsyncFailed,
            meanDurGood
            )
        '''
        sumRec = { 'devId': devId, 'dpr': dpr, 'blendFilePath': blendFilePath,
            'nAttempts': nAttempts, 'nGood': len(successes), 'nRenderFailed': nRenderFailed,
            'nRetrieveFailed': nRetrieveFailed, 'nRsyncFailed': nRsyncFailed, 'nSlow': nSlow,
            'meanDurGood': meanDurGood
        }
        sumRecs.append( sumRec )
    return sumRecs

import matplotlib.pyplot as plt
def plotRenderTimes( fetcherTable ):
    '''plots fetcher (and some core) job timings based on file dates'''
    import matplotlib as mpl
    import matplotlib.patches as patches
    import numpy as np
    def pdTimeToSeconds( pdTime ):
        '''convert pandas (or numpy?) time stamp to seconds since epoch'''
        if isinstance( pdTime, pd.Timestamp ):
            return pdTime.to_pydatetime().timestamp()
        return 0
    fetcherCounts = fetcherTable.hostSpec.value_counts()
    fetcherNames = sorted( list( fetcherCounts.index ), reverse = True )
    #fetcherNames.append( 'core' )
    nClusters = len( fetcherNames )
    fig = plt.figure()
    ax = plt.gca()
    clusterHeight = 1
    yMargin = .25
    yMax = nClusters*clusterHeight + yMargin
    ax.set_ylim( 0, yMax )
    yTickLocs = np.arange( clusterHeight/2, nClusters*clusterHeight, clusterHeight)
    plt.yticks( yTickLocs, fetcherNames )
    tickLocator10 = mpl.ticker.MultipleLocator(60)
    ax.xaxis.set_minor_locator( tickLocator10 )
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(600) )
    ax.xaxis.set_ticks_position( 'both' )
    alpha = .6
    
    # get the xMin and xMax from the union of all cluster time ranges
    allStartTimes = pd.Series()
    allFinishTimes = pd.Series()
    for cluster in fetcherNames:
        #print( cluster )
        jobs = fetcherTable[fetcherTable.hostSpec==cluster]
        startTimes = jobs.dateTime
        finishTimes = jobs.dateTime + jobs.durTd
        allStartTimes = allStartTimes.append( startTimes )
        allFinishTimes = allFinishTimes.append( finishTimes )
    xMin = pdTimeToSeconds( allStartTimes.min() )
    xMax = pdTimeToSeconds( allFinishTimes.max() ) + 10
    xMax = max( xMax, pdTimeToSeconds( allStartTimes.max() ) ) # + 40
    #print( xMin, xMax )
    ax.set_xlim( xMin, xMax )
    ax.set_xlim( 0, xMax-xMin )
    
    #jobColors = { 'collect': 'tab:blue', 'rsync': 'mediumpurple', 'render':  'lightseagreen' }
    #jobColors = { 'collect': 'lightseagreen', 'rsync': 'mediumpurple', 'render':  'tab:blue' }
    jobColors = { 'retrieved': 'lightseagreen', 'rsynced': 'tab:blue',
                 'renderFailed': 'tab:red', 'rsyncFailed': 'tab:purple', 'retrieveFailed': 'tab:pink' }
    jiggers = { 'renderFailed': .1, 'rsyncFailed': .1, 'retrieveFailed': .1 }
  
    jobColor0 = mpl.colors.to_rgb( 'gray' )
   
    #jobBottom = clusterHeight * .1 + yMargin
    jobBottom = yMargin
    for cluster in fetcherNames:
        #print( cluster )
        jobs = fetcherTable[fetcherTable.hostSpec==cluster]
        # plot some things for each segment tied to this fetcher
        for row in jobs.iterrows():
            job = row[1]
            startSeconds = pdTimeToSeconds( job.dateTime ) - xMin
            durSeconds = job.duration
            if durSeconds < 10:
                durSeconds = 10
            color = jobColors.get( job.eventType, jobColor0 )
            jigger = jiggers.get( job.eventType, 0 )
            boxHeight = clusterHeight*.7
            #if job.eventType == 'rsync':
            #    boxHeight -= clusterHeight * .2
            ax.add_patch(
                patches.Rectangle(
                    (startSeconds, jobBottom-jigger),   # (x,y)
                    durSeconds,          # width
                    boxHeight,          # height
                    facecolor=color, edgecolor='k', linewidth=0.5,
                    alpha=alpha
                    )
                )
            if job.retrievingDur > 0:
                ax.add_patch(
                    patches.Rectangle(
                        (startSeconds+durSeconds-job.retrievingDur, jobBottom-jigger),
                        job.retrievingDur,          # width
                        boxHeight,          # height
                        facecolor=color, edgecolor='k', linewidth=0.25,
                        alpha=alpha
                        )
                    )

            if job.sequenceNum >= 0:
                label = str(job.sequenceNum)
                y = jobBottom+.1
                ax.annotate( label, xy=(startSeconds+.4, y) )
        jobBottom += clusterHeight
        
    
    
    plt.gca().grid( True, axis='x')
    plt.tight_layout()

def plotRenderingTimes( frameSummaries, outFilePath ):
    frameSummaries['hostSpec'] = frameSummaries.devId    
    frameSummaries['dateTime'] = frameSummaries.startDateTime
    frameSummaries['duration'] = frameSummaries.dur
    frameSummaries['eventType'] = frameSummaries.frameState
    frameSummaries['sequenceNum'] = frameSummaries.frameNum
    frameSummaries['durTd'] = pd.to_timedelta(frameSummaries.duration, unit='s')

    plotRenderTimes( frameSummaries )
    plt.savefig( outFilePath )

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--tag', help='database collection-naming tag' )
    ap.add_argument('--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument('--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument('--outDir', help='directory path for output files', default='.')
    ap.add_argument('--sendMail', type=boolArg, default=False, help='to email results (default=False)' )
    args = ap.parse_args()
    #logger.debug( 'args %s', args )

    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    logsDb = mclient.renderingLogs
    # get the list of all collections in the db (avoiding using 'collections' as a global var name)
    tables = sorted(logsDb.list_collection_names())
    logger.info( 'database collections %s', tables )

    os.makedirs( args.outDir, exist_ok=True )
    instAttemptsFilePath = args.outDir + '/instAttempts.csv'
    installerSummariesFilePath = args.outDir + '/installerSummaries.csv'
    frameSummariesFilePath = args.outDir + '/frameSummaries.csv'
    workerSummariesFilePath = args.outDir + '/workerSummaries.csv'
    rendererTimingPlotFilePath = args.outDir + '/rendererTiming.png'
    isoFormat = '%Y-%m-%dT%H:%M:%S.%f%z'

    if args.tag:
        launchedCollName = 'launchedInstances_' + args.tag
        installerCollName = 'installerLog_' + args.tag
        rendererCollName = 'rendererLog_' + args.tag
    else:
        # inspect the 'officialTests' collection
        officialColl = logsDb[ 'officialTests' ]
        tests = list( officialColl.find() )
        if not tests:
            sys.exit( 'no officialTests found' )
        logger.info( 'all official tests: %s', [test['tag'] for test in tests] )
        testsDf = pd.DataFrame( tests )
        testsDf.sort_values( 'dateTime', inplace=True )
        logger.info( 'testsDf: %s', testsDf.info() )
        logger.info( 'found %d official tests, latest at %s', len(testsDf), testsDf.dateTime.max() )
        #latestTest = testsDf.iloc[-1]  # the last row
        #logger.info( 'using test with tag %s, from %s', latestTest.tag, latestTest.dateTime )
        allFrameSummaries = pd.DataFrame()
        allInstallerSummaries = pd.DataFrame()
        allInstancesById = {}
        for row in testsDf.itertuples():
            #logger.info( 'row: %s', row )
            logger.info( '>>> %s; %s, %d', row.rendererLog, row.blendFilePath, row.nFramesReq )

            # gather instances from launchedInstances collection into a dict
            # replace this TEMPORARY code with instAttempts-gathering code
            instancesAllocated = {}
            launchedColl = logsDb[row.launchedInstances]
            inRecs = launchedColl.find()
            for inRec in inRecs:
                if 'instanceId' not in inRec:
                    logger.warning( 'no instance ID in input record')
                    continue
                iid = inRec['instanceId']
                dpr = instanceDpr( inRec )
                if dpr < 47:
                    logger.info( 'dpr %.1f for device %d with cpu %s',
                        dpr, inRec['device-id'], inRec['cpu']
                    )
                inRec['dpr'] = dpr
                instancesAllocated[ iid ] = inRec
            
            # get events by instance from the installer log
            (eventsByInstance, badIids) = demuxResults( logsDb[row.installerLog] )
            installerSumRecs = summarizeInstallerLog( eventsByInstance, instancesAllocated, row.installerLog )
            allInstallerSummaries = allInstallerSummaries.append( installerSumRecs )

            allInstancesById.update( instancesAllocated )
            frameSummaryRecs = summarizeRenderingLog( instancesAllocated, row.rendererLog, row.tag )
            logger.info( 'frameSummaryRecs %d', len(frameSummaryRecs) )
            allFrameSummaries = allFrameSummaries.append( frameSummaryRecs )
        #print( allFrameSummaries.info() )
        allInstallerSummaries.to_csv( installerSummariesFilePath, index=False, date_format=isoFormat )
        allFrameSummaries.to_csv( frameSummariesFilePath, index=False, date_format=isoFormat )
        workerIids = allFrameSummaries.instanceId.unique()
        sumRecs = summarizeWorkers( workerIids, allInstancesById, allFrameSummaries )
        workerSummaries = pd.DataFrame( sumRecs )
        if workerSummariesFilePath:
            workerSummaries.to_csv( workerSummariesFilePath, index=False )
        '''
        launchedCollName = latestTest.launchedInstances
        installerCollName = latestTest.installerLog
        rendererCollName = latestTest.rendererLog
        '''
        sys.exit()

    if launchedCollName not in tables:
        sys.exit( 'could not find collection ' + launchedCollName )
    if installerCollName not in tables:
        sys.exit( 'could not find collection ' + installerCollName )
    if rendererCollName not in tables:
        sys.exit( 'could not find collection ' + rendererCollName )

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

    # summarize jlog into array of dicts, with side-effect of modifying instance records
    installerSumRecs = summarizeInstallerLog( eventsByInstance, instancesAllocated, installerCollName )
    installerSummaries = pd.DataFrame( installerSumRecs )
    installerSummaries.to_csv( installerSummariesFilePath, index=False, date_format=isoFormat )
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
    if installerSummariesFilePath:
        installerSummaries.to_csv( installerSummariesFilePath, index=False )

    startingEvent = logsDb[rendererCollName].find_one( 
        {'instanceId': '<master>'}, hint=[('dateTime', pymongo.ASCENDING)] 
        )
    startDateTime = interpretDateTimeField( startingEvent['dateTime'] )


    # summarize renderer events into a table
    sumRecs = summarizeRenderingLog( instancesAllocated, rendererCollName, tag=args.tag )
    frameSummaries = pd.DataFrame( sumRecs )
    if frameSummariesFilePath:
        frameSummaries.to_csv( frameSummariesFilePath, index=False, date_format=isoFormat )

    if frameSummaries.blendFilePath.max() != frameSummaries.blendFilePath.min():
        logger.warning( 'more than 1 diofferent blendFilePath found %s', 
            list(frameSummaries.blendFilePath.unique()) )
    blendFilePath = frameSummaries.blendFilePath[0]
    workerIids = list( frameSummaries.instanceId.unique() )
    #workerIids = [iid for iid in byInstance.keys() if '<' not in iid]

    # traverse the workers and generate a summary table
    sumRecs = []
    for iid in sorted( workerIids ):
        devId = instancesAllocated[iid].get( 'device-id' )
        dpr = instancesAllocated[iid].get( 'dpr' )
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
        sumRec = { 'instanceId': iid, 'devId': devId, 'dpr': dpr,
            'blendFilePath': blendFilePath,
            'nAttempts': nAttempts, 'nGood': len(successes), 'nRenderFailed': nRenderFailed,
            'nRetrieveFailed': nRetrieveFailed, 'nRsyncFailed': nRsyncFailed,
            'meanDurGood': meanDurGood
        }
        sumRecs.append( sumRec )
    workerSummaries = pd.DataFrame( sumRecs )
    if workerSummariesFilePath:
        workerSummaries.to_csv( workerSummariesFilePath, index=False )
    plotRenderingTimes( frameSummaries, rendererTimingPlotFilePath )

    logger.info( '%d worker instances', len(workerSummaries))
    logger.info( '%d good instances', len(workerSummaries[workerSummaries.nGood > 0] ) )
    badCondition = (workerSummaries.nGood < workerSummaries.nAttempts) | (workerSummaries.nGood <= 0)
    logger.info( '%d bad instances', len(workerSummaries[badCondition] ) )
    logger.info( '%d partly-good instances',
        len(workerSummaries[(workerSummaries.nGood < workerSummaries.nAttempts) & (workerSummaries.nGood > 0) ] ) )
    #logger.info( '%d partly-good instances', len( goodIids & badIids ))
    if args.sendMail:
        import neocortixMail
        #recipients = ['mcoffey@neocortix.com']
        recipients = ['mcoffey@neocortix.com', 'lwatts@neocortix.com', 'dm@neocortix.com']
        body = ''
        body += 'startDateTime %s\n' % startDateTime.isoformat()
        body += 'installing started %s\n' % installerSummaries.dateTime.min()
        body += 'rendering started %s\n' % frameSummaries.startDateTime.min()
        body += 'rendering finished %s\n' % frameSummaries.endDateTime.max()
        body += '\n'

        body += '%d instances requested\n' % len(instancesAllocated) 
        stateCounts = installerSummaries.state.value_counts()
        #logger.info( 'instCodes %s', stateCounts )
        body += '%d installed Blender\n' % (stateCounts['installed'])
        body += '%d installerTimeouts\n' % (stateCounts['installerTimeout'])
        body += '%d installerFailed\n' % (stateCounts.get('installerFailed', 0))
        body += '%d installerException\n' % (stateCounts.get('installerException', 0))
        body += '\n'

        body += '%d did some rendering\n' % len(workerSummaries[workerSummaries.nGood > 0] )
        body += '%d frames rendered\n' % len(frameSummaries[frameSummaries.frameState == 'retrieved'] )

        attachmentPaths = [rendererTimingPlotFilePath]
        neocortixMail.sendMailWithAttachments( 'mcoffey@neocortix.com',
            recipients, 'render-test result summary', body, attachmentPaths )
