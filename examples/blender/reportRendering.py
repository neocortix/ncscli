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
import uuid

# third-party modules
import dateutil.parser
import pandas as pd
import pymongo
import requests

# neocortix module(s)
import devicePerformance
import eventTiming
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
                #logger.info( 'returncode %d for %s', rc, iid )
                badOnes.add( iid )
        if 'exception' in decoded:
            #logger.info( 'exception %s for %s', decoded['exception'], iid )
            badOnes.add( iid )
        if 'timeout' in decoded:
            #logger.info( 'timeout %s for %s', decoded['timeout'], iid )
            badOnes.add( iid )
    #logger.info( 'topLevelKeys %s', topLevelKeys )
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
    #if dpr < 37:
    #    logger.info( 'unhappy dpr for dev %d with cpu %s', inst['device-id'], inst['cpu'] )
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
                    #logger.info( '%d instances were launched', len(launchedIids) )
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
    #logger.info( '%d succeeded, %d failed, %d timeout, %d exceptions',
    #    nSucceeded, nFailed, nTimeout, nExceptions)
    sumRecs = []
    for iid in instancesAllocated:
        inst = instancesAllocated[iid]
        if inst.get( 'state' ) == 'exhausted':
            continue
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
        elif iid.startswith( '<' ):
            devId = 0
        else:
            logger.info( 'IID UUID? %s', iid )
            devId = uuid.UUID(iid).int
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
                    if 'origBlendFilePath' in parallelRenderOp:
                        blendFilePath = parallelRenderOp['origBlendFilePath']
                    else:
                        blendFilePath = parallelRenderOp['blendFilePath']
                    #logger.info( 'blendFilePath %s', blendFilePath )
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
        dpr = 0
        ramTotal = 0
        if iid not in instances:
            inst = {}
        else:
            inst = instances[iid]
            dpr = inst.get('dpr')
            if 'ram' in inst:
                ramTotal = inst['ram'].get('total', 0) / 1000000
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
        earliest = subset.startDateTime.min()
        latest = subset.endDateTime.max()
        '''
        logger.info( '%s dev %d, %d attempts, %d good, %d renderFailed, %d other, %.1f meanDurGood',
            iid[0:16], devId, nAttempts, len(successes),
            nRenderFailed, nRetrieveFailed+nRsyncFailed,
            meanDurGood
            )
        '''
        sumRec = { 'devId': devId, 'instanceId': iid, 'dpr': dpr,
            'blendFilePath': blendFilePath,
            'nAttempts': nAttempts, 'nGood': len(successes), 'nRenderFailed': nRenderFailed,
            'nRetrieveFailed': nRetrieveFailed, 'nRsyncFailed': nRsyncFailed, 'nSlow': nSlow,
            'meanDurGood': meanDurGood, 'earliest':earliest, 'latest':latest,
            'ramTotal': ramTotal
        }
        sumRecs.append( sumRec )
    return sumRecs

def summarizeInstanceTiming( instancesByIid, terminationOps, installerSumRecs, frameSumRecs ):
    sumRecs = {}
    for iid, inst in instancesByIid.items():
        if inst['state'] in ['exhausted']:  #TODO define a better test
            logger.warning( 'instance %s not started (%s)', iid, inst['state'] )
            continue
        sumRec = { 'instanceId': iid, 'devId': inst['device-id'],
            'state': inst['state'],
            'rsyncDur': 0, 'renderDur': 0, 'retrievingDur': 0, 'idleDur': float('NaN')
             }
        sumRec['started-at'] = inst['started-at']
        sumRec['launchDateTime'] = universalizeDateTime( dateutil.parser.parse( inst['started-at'], ignoretz=False ) )

        sumRecs[iid] = sumRec
    # incorporate termination times, keeping track of the latest
    latestTermDateTime = datetime.datetime.fromtimestamp( 0, datetime.timezone.utc ) # wayyy in the past
    for iid, termOp in terminationOps.items():
        if iid not in sumRecs:
            logger.warning( 'termninated iid %s not found in sumRecs', iid )
            continue
        termDateTime = termOp['dateTime']
        latestTermDateTime = max( latestTermDateTime, termDateTime )
        sumRecs[iid]['termDateTime'] = termDateTime
        sumRecs[iid]['instanceDur'] = (termOp['dateTime'] - sumRecs[iid]['launchDateTime']).total_seconds()

    # incorporate installer timing
    for installerRec in installerSumRecs:
        iid = installerRec['instanceId']
        if iid not in sumRecs:
            logger.warning( 'installer iid %s not found in sumRecs', iid )
            continue
        installerDateTime = universalizeDateTime( dateutil.parser.parse( installerRec['dateTime'], ignoretz=False ) )
        sumRecs[iid]['installerDateTime'] = installerDateTime
        installerDur = installerRec['dur']
        sumRecs[iid]['installerDur'] = installerDur
    
    # incorporate frame timing
    for frameRec in frameSumRecs:
        iid = frameRec['instanceId']
        if iid not in sumRecs:
            logger.warning( 'frame iid %s not found in sumRecs', iid )
            continue
        sumRec = sumRecs[iid]
        if frameRec['frameState'] in ['rsynced', 'rsyncFailed']:
            sumRec['rsyncDur'] += frameRec['dur']
        elif frameRec['frameState'] in ['retrieved', 'retrieveFailed', 'renderFailed']:
            sumRec['renderDur'] += frameRec['dur']
            if frameRec.get('retrievingDur'):
                sumRec['retrievingDur'] += frameRec.get('retrievingDur', 0)
        else:
            logger.info( 'frameState %s', frameRec['frameState'] )

    # revisit to compute final metrics and fill in missing items
    for iid, sumRec in sumRecs.items():
        if 'termDateTime' not in sumRec:
            logger.warning( 'patching termDateTime for %s', sumRec )
            sumRec['termDateTime'] = latestTermDateTime
        if 'instanceDur' not in sumRec:
            logger.warning( 'patching dur for %s', sumRec )
            sumRec['instanceDur'] = (sumRec['termDateTime'] - sumRec['launchDateTime']).total_seconds()
        workingDur = sumRec['installerDur'] + sumRec['rsyncDur'] + sumRec['renderDur']
        idleDur = sumRec['instanceDur'] - workingDur
        sumRec['workingDur'] = workingDur
        sumRec['idleDur'] = idleDur

    #logger.info( 'sumRecs: %s', sumRecs )
    return sumRecs

def retrieveTerminations( rendererColl, instancesByIid ):
    '''finds termination operations and returns dict by iid of dicts '''
    terminations = {}  # collects termination operations by iid

    # look for terminations due to bad install
    termEvents = rendererColl.find(
        {'args.terminateBad': {'$exists':True}, 'instanceId': '<recruitInstances>'}
        )
    for termEvent in termEvents:
        for iid in termEvent['args']['terminateBad']:
            tdt = universalizeDateTime( dateutil.parser.parse( termEvent['dateTime'], ignoretz=False ) )
            #logger.info( 'terminateBad %s at %s', iid, tdt )
            if iid in instancesByIid:
                devId = instancesByIid[iid].get( 'device-id' )
            else:
                devId = 0
            terminations[ iid ] = { 'opType': 'terminateBad',
                'dateTime': tdt, 'devId': devId }

    # look for terminations due to failed workers
    termEvents = rendererColl.find(
        {'args.terminateFailedWorker': {'$exists':True}, 'instanceId': '<master>'}
        )
    for termEvent in termEvents:
        iid = termEvent['args']['terminateFailedWorker']
        tdt = universalizeDateTime( dateutil.parser.parse( termEvent['dateTime'], ignoretz=False ) )
        #logger.info( 'terminateFailedWorker %s at %s', iid, tdt )
        if iid in instancesByIid:
            devId = instancesByIid[iid].get( 'device-id' )
        else:
            devId = 0
        if iid in terminations:
            logger.warning( 'terminateFailedWorker already had a termination %s', terminations[iid] )
        else:
            terminations[ iid ] = { 'opType': 'terminateFailedWorker',
                'dateTime': tdt, 'devId': devId }

    # look for terminations due to excess workers
    termEvents = rendererColl.find(
        {'args.terminateExcessWorker': {'$exists':True}, 'instanceId': '<master>'}
        )
    for termEvent in termEvents:
        iid = termEvent['args']['terminateExcessWorker']
        tdt = universalizeDateTime( dateutil.parser.parse( termEvent['dateTime'], ignoretz=False ) )
        #logger.info( 'terminateExcessWorker %s at %s (%s)', iid, tdt, termEvent['dateTime'] )
        if iid in terminations:
            logger.warning( 'terminateExcessWorker already had a termination %s', terminations[iid] )
        else:
            if iid in instancesByIid:
                devId = instancesByIid[iid].get( 'device-id' )
            else:
                devId = 0
            terminations[ iid ] = { 'opType': 'terminateExcessWorker',
                'dateTime': tdt, 'devId': devId }
    #logger.info( 'early terminations (%d) %s', len(terminations), terminations )
    # look for "final" terminations
    termEvents = rendererColl.find(
        {'args.terminateFinal': {'$exists':True}, 'instanceId': '<master>'}
        )
    for termEvent in termEvents:
        for iid in termEvent['args']['terminateFinal']:
            tdt = universalizeDateTime( dateutil.parser.parse( termEvent['dateTime'], ignoretz=False ) )
            #logger.info( 'terminateFinal %s at %s', iid, tdt )
            if iid in instancesByIid:
                devId = instancesByIid[iid].get( 'device-id' )
            else:
                devId = 0
            # only add it if instance wasn't already teminated
            if iid not in terminations:
                terminations[ iid ] = { 'opType': 'terminateFinal',
                    'dateTime': tdt, 'devId': devId }
    return terminations

def plotRenderTimes( framesDf, terminationOps, outFilePath ):
    '''plots rendering job timings based on frame summaries'''
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import matplotlib.patches as patches
    import numpy as np
    def pdTimeToSeconds( pdTime ):
        '''convert pandas (or numpy?) time stamp to seconds since epoch'''
        if isinstance( pdTime, pd.Timestamp ):
            return pdTime.to_pydatetime().timestamp()
        return 0

    terminationsByDev = {v['devId'] : v for k, v in terminationOps.items()}

    devCounts = framesDf.devId.value_counts()
    devIds = sorted( list( devCounts.index ), reverse = True )
    nDevices = len( devIds )
    _ = plt.figure()
    ax = plt.gca()
    rowHeight = 1
    yMargin = .25
    yMax = nDevices*rowHeight + yMargin
    alpha = .6
    
    # get the xMin and xMax from the union of all device time ranges
    allStartTimes = pd.Series()
    allFinishTimes = pd.Series()
    for devId in devIds:
        jobs = framesDf[framesDf.devId==devId]
        startTimes = jobs.startDateTime
        #finishTimes = jobs.startDateTime + jobs.durTd
        finishTimes = jobs.startDateTime + pd.to_timedelta(jobs.dur, unit='s')
        allStartTimes = allStartTimes.append( startTimes )
        allFinishTimes = allFinishTimes.append( finishTimes )
    xMin = pdTimeToSeconds( allStartTimes.min() )
    xMax = pdTimeToSeconds( allFinishTimes.max() ) + 10
    xMax = max( xMax, pdTimeToSeconds( allStartTimes.max() ) ) # + 40
    ax.set_xlim( xMin, xMax )
    ax.set_xlim( 0, xMax-xMin )

    ax.set_ylim( 0, yMax )
    yTickLocs = np.arange( rowHeight/2, nDevices*rowHeight, rowHeight)
    plt.yticks( yTickLocs, devIds )
    tickLocator1 = mpl.ticker.MultipleLocator(600)
    ax.xaxis.set_minor_locator( tickLocator1 )
    ax.xaxis.set_major_locator( mpl.ticker.MultipleLocator(1800) )
    ax.xaxis.set_ticks_position( 'both' )

    jobColors = { 'retrieved': 'tab:cyan', 'rsynced': 'tab:blue',
                 'renderFailed': 'tab:red', 'rsyncFailed': 'tab:orange', 'retrieveFailed': 'tab:pink' }
    jiggers = { 'renderFailed': .1, 'rsyncFailed': .1, 'retrieveFailed': .1 }
  
    jobColor0 = mpl.colors.to_rgb( 'gray' )
    fontsize = 4  # was 8
   
    jobBottom = yMargin
    boxHeight = rowHeight*.7
    for devId in devIds:
        jobs = framesDf[framesDf.devId==devId]
        # plot some things for each frame assigned to this device (including rsync)
        for row in jobs.iterrows():
            job = row[1]
            startSeconds = pdTimeToSeconds( job.startDateTime ) - xMin
            durSeconds = job.dur
            if durSeconds < 10:
                durSeconds = 10
            color = jobColors.get( job.frameState, jobColor0 )
            jigger = jiggers.get( job.frameState, 0 )
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

            if job.frameNum >= 0:
                label = str(job.frameNum)
                y = jobBottom+.1
                ax.annotate( label, xy=(startSeconds+.4, y), fontsize=fontsize )
        # plot termination op, if any
        if devId not in terminationsByDev:
            logger.warning( 'no early termination for dev %d', devId )
        else:
            termOp = terminationsByDev[ devId ]
            dateTime = termOp['dateTime']
            ts = pd.Timestamp( dateTime )
            seconds = pdTimeToSeconds( ts ) - xMin
            ax.add_patch(
                patches.Ellipse(
                    (seconds, jobBottom+boxHeight/2),   # (x,y)
                    60,          # width
                    boxHeight,          # height
                    facecolor=jobColor0, edgecolor='k', linewidth=0.5,
                    alpha=alpha
                    )
                )

        jobBottom += rowHeight

    plt.gca().grid( True, axis='x')
    #ax.labelsize = 4  # does not work for tick labels
    ax.tick_params(axis='both', which='major', labelsize=6)
    plt.tight_layout()
    if outFilePath:
        plt.savefig( outFilePath, dpi=250 )
    else:
        plt.show()

def plotTestHistory( testsDf, plotOutFilePath ):
    import matplotlib.pyplot as plt
    def prettify():
        plt.subplots_adjust(bottom=0.40)
        ax = plt.gca()
        ax.set_ylim( bottom=0 )
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.7, box.height])
        ax.tick_params(axis='x', which='major', labelsize=4)
        plt.legend(bbox_to_anchor=(1.0, 0.5), loc='center left')

    figsize = (8.5, 4.8)
    fontsize = 9
    ticks = list(range(0,len(testsDf)+1))

    # plot the main counting metrics as line graphs
    _ax = testsDf.plot.line(x='tag', rot=90, style=':o', xticks=ticks,
                    title='historic rendering metrics', fontsize=fontsize, figsize=figsize,
                    markersize=3, linewidth=0.5,
                    y=['nInstancesReq', 'nInstallerTimeout', 'nInstalled', 
                    'nInstallerException', 'nInstallerFailed', 'nRendering'] )
    prettify()
    plt.savefig( plotOutFilePath, dpi=250 )
    plt.savefig( plotOutFilePath+'.pdf', dpi=250 )
    #plt.savefig( plotOutFilePath+'.svg', dpi=250 ) # works, but would need visual tweaking

    # additional plots disabled for now
    '''
    testsDf['installRate'] = testsDf.nInstalled / testsDf.nInstancesReq
    testsDf['timeoutRate'] = testsDf.nInstallerTimeout / testsDf.nInstancesReq
    testsDf['successRate'] = testsDf.nRendering / testsDf.nInstancesReq

    testsDf.plot.line( x='tag', rot=90, style=':o', xticks=ticks, fontsize=fontsize, figsize=figsize,
                    y=[ 'installRate', 'timeoutRate', 'successRate'])
    prettify()

    testsDf.plot.line( x='tag', rot=90, style=':o', xticks=ticks, fontsize=fontsize, figsize=figsize,
        y='installerMeanDurGood' )
    prettify()
    '''


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--tag', help='database collection-naming tag' )
    ap.add_argument('--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument('--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument('--outDir', help='directory path for output files', default='.')
    ap.add_argument('--sendMail', help='to email results with given settings (default=no email)' )
    args = ap.parse_args()
    #logger.debug( 'args %s', args )

    eventTimings = []

    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    logsDb = mclient.renderingLogs
    # get the list of all collections in the db (avoiding using 'collections' as a global var name)
    tables = sorted(logsDb.list_collection_names())
    #logger.info( 'database collections %s', tables )

    os.makedirs( args.outDir, exist_ok=True )
    instAttemptsFilePath = args.outDir + '/instAttempts.csv'
    installerSummariesFilePath = args.outDir + '/installerSummaries.csv'
    frameSummariesFilePath = args.outDir + '/frameSummaries.csv'
    workerSummariesFilePath = args.outDir + '/workerSummaries.csv'
    instanceTimingFilePath = args.outDir + '/instanceTiming.csv'
    rendererTimingPlotFilePath = args.outDir + '/rendererTiming.png'
    isoFormat = '%Y-%m-%dT%H:%M:%S.%f%z'

    if args.tag:
        launchedCollName = 'launchedInstances_' + args.tag
        installerCollName = 'installerLog_' + args.tag
        rendererCollName = 'rendererLog_' + args.tag
    else:
        testSummariesFilePath = args.outDir + '/testSummaries.csv'
        testSummariesPngFilePath = args.outDir + '/testHistory.png'

        # analyze everything listed in the 'officialTests' collection
        officialColl = logsDb[ 'officialTests' ]
        tests = list( officialColl.find() )
        if not tests:
            sys.exit( 'no officialTests found' )
        logger.info( 'all official tests: %s', [test['tag'] for test in tests] )
        testsDf = pd.DataFrame( tests )
        testsDf.sort_values( 'dateTime', inplace=True )
        testsDf.set_index( '_id', inplace=True )  # _id is same as tag; change this if not
        testsDf['nInstancesReq'] = None
        testsDf['nInstancesLaunched'] = None
        testsDf['nInstalled'] = None
        testsDf['nInstallerException'] = None
        testsDf['nInstallerFailed'] = None
        testsDf['nInstallerTimeout'] = None
        testsDf['nRendering'] = None
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
            inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
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
            instancesLaunched = [inst for inst in inRecs if inst['state'] == 'started']
            testsDf.loc[ row.tag, 'nInstancesReq'] = len(inRecs)
            testsDf.loc[ row.tag, 'nInstancesLaunched'] = len(instancesLaunched)
            
            # get events by instance from the installer log
            (eventsByInstance, badIids) = demuxResults( logsDb[row.installerLog] )
            installerSumRecs = summarizeInstallerLog( eventsByInstance, instancesAllocated, row.installerLog )
            allInstallerSummaries = allInstallerSummaries.append( installerSumRecs )
            installerSummaries = pd.DataFrame( installerSumRecs )
            installerStateCounts = installerSummaries.state.value_counts()
            testsDf.loc[ row.tag, 'nInstalled'] = installerStateCounts['installed']
            if 'installerException' in installerStateCounts:
                testsDf.loc[ row.tag, 'nInstallerException'] = installerStateCounts['installerException']
            else:
                testsDf.loc[ row.tag, 'nInstallerException'] = 0
            testsDf.loc[ row.tag, 'nInstallerFailed'] = installerStateCounts.get('installerFailed', 0)
            testsDf.loc[ row.tag, 'nInstallerTimeout'] = installerStateCounts.get('installerTimeout', 0)
            testsDf.loc[ row.tag, 'installerMeanDurGood'] = \
                installerSummaries[installerSummaries.state=='installed'].dur.mean()

            allInstancesById.update( instancesAllocated )

            frameSummaryRecs = summarizeRenderingLog( instancesAllocated, row.rendererLog, row.tag )
            frameSummaries = pd.DataFrame( frameSummaryRecs )
            logger.info( 'frameSummaryRecs %d', len(frameSummaryRecs) )
            allFrameSummaries = allFrameSummaries.append( frameSummaryRecs )

            nFramesFinished = len( frameSummaries[frameSummaries.frameState == 'retrieved'] )
            testsDf.loc[ row.tag, 'nFramesFinished'] = nFramesFinished

            workerIids = frameSummaries.instanceId.unique()
            sumRecs = summarizeWorkers( workerIids, instancesAllocated, frameSummaries )
            workerSummaries = pd.DataFrame( sumRecs )
            nWorking = len(workerSummaries[workerSummaries.nGood > 0] )
            testsDf.loc[ row.tag, 'nRendering'] = nWorking

        #print( allFrameSummaries.info() )
        allInstallerSummaries.to_csv( installerSummariesFilePath, index=False, date_format=isoFormat )
        #print( testsDf.info() )
        #print( testsDf )
        testsDf.to_csv( testSummariesFilePath, index=False, date_format=isoFormat )
        plotTestHistory( testsDf, testSummariesPngFilePath )

        allFrameSummaries.to_csv( frameSummariesFilePath, index=False, date_format=isoFormat )
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
        dpr = instanceDpr( inRec )
        inRec['dpr'] = dpr
        instancesAllocated[ iid ] = inRec
    logger.info( 'found %d instances in collection %s', len(instancesAllocated), launchedCollName )
    
    # get events by instance from the installer log
    (eventsByInstance, badIids) = demuxResults( logsDb[installerCollName] )
    #logger.info( 'badIids (%d) %s', len(badIids), badIids )

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

    # get the parallelRender operation event
    prEvent = logsDb[rendererCollName].find_one( 
        {'args.parallelRender': {'$exists':True}, 'instanceId': '<master>'} 
        )
    prOp = prEvent['args']['parallelRender']
    nFramesReq = prOp['nFramesReq']

    finishedEvent = logsDb[rendererCollName].find_one( 
        {'args.finished': {'$exists':True}, 'instanceId': '<master>'} 
        )
    #logger.info( 'finishedEvent %s', finishedEvent )
    endDateTime = interpretDateTimeField( finishedEvent['dateTime'] )


    # summarize renderer events into a table
    frameSumRecs = summarizeRenderingLog( instancesAllocated, rendererCollName, tag=args.tag )
    frameSummaries = pd.DataFrame( frameSumRecs )
    if frameSummariesFilePath:
        frameSummaries.to_csv( frameSummariesFilePath, index=False, date_format=isoFormat )

    if frameSummaries.blendFilePath.max() != frameSummaries.blendFilePath.min():
        logger.warning( 'more than 1 different blendFilePath found %s', 
            list(frameSummaries.blendFilePath.unique()) )
    blendFilePath = frameSummaries.blendFilePath[0]
    workerIids = list( frameSummaries.instanceId.unique() )
    #workerIids = [iid for iid in byInstance.keys() if '<' not in iid]

    rsyncStartDateTime = frameSummaries.startDateTime.min() # actually first rsync start
    logger.info( 'rsyncStartDateTime: %s', rsyncStartDateTime )

    # as a fallback, use endDateTime as terminationDateTime (in case terminateFinal is missing)
    terminationDateTime = endDateTime
    # query for the terminateFinal operation, which occurs after all rendering has finished
    terminationEvent = logsDb[rendererCollName].find_one(
        {'args.terminateFinal': {'$exists':True}, 'instanceId': '<master>'}
        )
    if terminationEvent:
        terminationDateTime = interpretDateTimeField( terminationEvent['dateTime'] )
        logger.info( 'initial terminateFinal %s', terminationDateTime )

    terminations = retrieveTerminations( logsDb[rendererCollName], instancesAllocated )
    #terminationsTable = pd.DataFrame.from_dict( terminations, orient='index' )
    #terminationsTable.to_csv( 'terminations.csv', index=False, date_format=isoFormat )

    terminationCredit = 0
    for iid, termOp in terminations.items():
        if termOp['opType'] != 'terminateBad':
            tdt = termOp['dateTime'] 
            terminationCredit += (terminationDateTime - tdt).total_seconds()
    logger.info( 'terminationCredit: %.1f seconds (%.1f minutes)',
        terminationCredit, terminationCredit/60 )
    
    iTimingRecs = summarizeInstanceTiming( instancesAllocated, terminations, installerSumRecs, frameSumRecs )
    instanceTimingSummaries = pd.DataFrame.from_dict( iTimingRecs, orient='index' )
    #print( df.info() )
    instanceTimingSummaries.to_csv( instanceTimingFilePath, index=False, date_format=isoFormat )
    totalIdleDur = instanceTimingSummaries.idleDur.sum()

    #sys.exit( 'DEBUGGING')
    if True:
        sumRecs = summarizeWorkers( None, instancesAllocated, frameSummaries )
    else:
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
            '''
            logger.debug( '%s dev %d, %d attempts, %d good, %d renderFailed, %d other, %.1f meanDurGood',
                iid[0:16], devId, nAttempts, len(successes),
                nRenderFailed, nRetrieveFailed+nRsyncFailed,
                meanDurGood
                )
            '''
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
    plotRenderTimes( frameSummaries, terminations, rendererTimingPlotFilePath )

    logger.info( '%d worker instances', len(workerSummaries))
    logger.info( '%d good instances', len(workerSummaries[workerSummaries.nGood > 0] ) )
    badCondition = (workerSummaries.nGood < workerSummaries.nAttempts) | (workerSummaries.nGood <= 0)
    logger.info( '%d bad instances', len(workerSummaries[badCondition] ) )
    logger.info( '%d partly-good instances',
        len(workerSummaries[(workerSummaries.nGood < workerSummaries.nAttempts) & (workerSummaries.nGood > 0) ] ) )
    #logger.info( '%d partly-good instances', len( goodIids & badIids ))
    if args.sendMail:
        import neocortixMail
        try:
            emailSettings = json.loads( args.sendMail )
        except Exception as exc:
            logger.warning( 'did not recognize "sendMail" arg as json (%s) %s', type(exc), exc )
            logger.info( 'sendMail arg: %s', args.sendMail )

        sender = emailSettings['from']
        smtpHost = emailSettings['smtpHost']
        pwd = emailSettings.get('pwd')
        recipients = emailSettings['to']

        body = ''
        body += 'database tag is %s\n' % args.tag
        body += 'startDateTime %s\n' % startDateTime.isoformat()
        body += 'installing started %s\n' % installerSummaries.dateTime.min()
        body += 'rendering started %s\n' % frameSummaries.startDateTime.min()
        body += 'rendering finished %s\n' % frameSummaries.endDateTime.max()
        body += '\n'
        nRequested = len(instancesAllocated) 
        body += '%d instances requested\n' % nRequested
        stateCounts = installerSummaries.state.value_counts()
        #logger.info( 'instCodes %s', stateCounts )
        body += '%d installed Blender\n' % (stateCounts.get('installed', 0))
        body += '%d installerTimeout\n' % (stateCounts.get('installerTimeout', 0))
        body += '%d installerFailed\n' % (stateCounts.get('installerFailed', 0))
        body += '%d installerException\n' % (stateCounts.get('installerException', 0))
        body += '\n'

        nWorking = len(workerSummaries[workerSummaries.nGood > 0] )
        frameStateCounts = frameSummaries.frameState.value_counts()
        failureTypeCounts = frameSummaries[frameSummaries.frameState == 'renderFailed'].rc.value_counts()
        nFramesFinished = len( frameSummaries[frameSummaries.frameState == 'retrieved'] )
        #logger.info( 'frameStateCounts %s', frameStateCounts )
        body += '%d did some rendering (%.1f %% of requested instances)\n' % \
            (nWorking, nWorking*100/nRequested )
        body += '\n'
        body += '%d frame(s) requested\n' % nFramesReq
        body += '%d frame(s) rendered from %s\n' % (nFramesFinished, blendFilePath )
        if nFramesFinished < nFramesReq:
            body += '%d frame(s) were not finished\n' % (nFramesReq - nFramesFinished)
        body += '%d frame-render failure(s)\n' % frameStateCounts.get('renderFailed', 0)
        body += '(including %d timeout and %d ssh)\n' % (failureTypeCounts.get(124, 0), failureTypeCounts.get(255, 0))
        body += '%d frame-retrieve failure(s)\n' % frameStateCounts.get('retrieveFailed', 0)
        body += '%d instance(s) failed to rsync\n' % frameStateCounts.get('rsyncFailed', 0)
        body += '\n'

        meanDurGood = frameSummaries[ frameSummaries.frameState == 'retrieved'].dur.mean()
        body += 'mean duration of good frame-render was %.1f minutes\n' % (meanDurGood/60)

        totWorkerTime = frameSummaries.dur.sum()
        if frameStateCounts.get('retrieved', 0):
            body += 'overall cost of rendering was %.1f worker-minutes per frame (including failures but not idle time)\n' % \
                ((totWorkerTime/60) / frameStateCounts.get('retrieved', 0))

        renderingElapsedTime = (frameSummaries.endDateTime.max() - frameSummaries.startDateTime.min()).total_seconds()
        logger.info( 'renderingElapsedTime %d secs (%.1f minutes)', renderingElapsedTime, renderingElapsedTime/60)
        #totInstSecs = (renderingElapsedTime*stateCounts['installed']) - terminationCredit
        #if frameStateCounts.get('retrieved', 0):
        #    body += 'overall cost of rendering was %.1f worker-minutes per frame (including failures and idle time)\n' % \
        #        ((totInstSecs/60) / frameStateCounts.get('retrieved', 0))

        body += 'total idle time was %.1f instance-minutes (including installer-idle time)\n' % (totalIdleDur/60)
        
        body += '\nTiming Summary (durations in minutes)\n'
        eventTimings.append( eventTiming.eventTiming( 
            'launching', startDateTime,
                dateutil.parser.parse(installerSummaries.dateTime.min() )
        ))
        eventTimings.append( eventTiming.eventTiming( 
            'installing', dateutil.parser.parse( installerSummaries.dateTime.min() ),
                frameSummaries.startDateTime.min()
        ))
        eventTimings.append( eventTiming.eventTiming( 
            'rendering', frameSummaries.startDateTime.min(),
                frameSummaries.endDateTime.max()
        ))
        eventTimings.append( eventTiming.eventTiming( 
            'overall', startDateTime,
                endDateTime
        ))
        for ev in eventTimings:
            s1 = ev.startDateTime.strftime( '%H:%M:%S' )
            if ev.endDateTime:
                s2 = ev.endDateTime.strftime( '%H:%M:%S' )
            else:
                s2 = s1
            dur = ev.duration().total_seconds() / 60
            body += ' '.join([ s1, s2, '%7.1f' % (dur), ev.eventName ]) + '\n'

        attachmentPaths = [rendererTimingPlotFilePath]
        neocortixMail.sendMailWithAttachments( sender,
            recipients, 'render-test result summary', body, smtpHost, attachmentPaths, pwd=pwd )
