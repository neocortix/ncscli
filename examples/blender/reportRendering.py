#!/usr/bin/env python3
"""
merges instance info from a json file with success info from an inv file
"""
# standard library modules
import argparse
import collections
import datetime
import json
import logging
import os
import shutil
import sys
import time

# third-party modules
import dateutil.parser
import pymongo  # would be needed for indexing
import requests

# neocortix module(s)
import ncs

logger = logging.getLogger(__name__)


def itemsNotFound( a, b ):
    ''' return all items from iterable a not found in iterable b '''
    return [x for x in a if x not in b]

def demuxResults( collection ):
    '''deinterleave jlog items into separate lists for each instance'''
    byInstance = {}
    badOnes = set()
    topLevelKeys = collections.Counter()
    # demux by instance
    for decoded in collection.find():
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

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--tag', help='database collection-naming tag' )
    ap.add_argument('--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument('--mongoPort', help='the port of mongodb server', default=27017)
    ap.add_argument('--csvOut', default=None)
    args = ap.parse_args()
    #logger.debug( 'args %s', args )

    csvOutFilePath = args.csvOut

    if args.tag:
        launchedCollName = 'launchedInstances_' + args.tag
        installerCollName = 'installerLog_' + args.tag
    else:
        sys.exit( 'unimplemented: would get latest official test' )

    mclient = pymongo.MongoClient(args.mongoHost, args.mongoPort)
    logsDb = mclient.renderLogs
    tables = sorted(logsDb.list_collection_names())  # avpiding using 'collections' as a global var name
    logger.info( 'database collections %s', tables )

    launchedColl = logsDb[launchedCollName]

    # gather instances from launchedInstances collection into a dict
    instancesAllocated = {}
    inRecs = launchedColl.find()
    for inRec in inRecs:
        if 'instanceId' not in inRec:
            logger.warning( 'no instance ID in input record')
            continue
        iid = inRec['instanceId']
        instancesAllocated[ iid ] = inRec
    logger.info( 'found %d instances in collection %s', len(instancesAllocated), launchedCollName )
    
    # partition events by instance from the installer jlog
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
                tellInstancesDateTime = dateutil.parser.parse( event['dateTime'] )
                try:
                    launchedIids = event["operation"][1]["args"]["instanceIds"]
                    logger.info( '%d instances were launched', len(launchedIids) )
                except Exception as exc:
                    logger.info( 'exception ignored for tellInstances op (%s)', type(exc) )
            if 'operation' in event and 'connect' in event['operation']:
                # start of ssh connection for this instance
                connectingDateTime = dateutil.parser.parse( event['dateTime'] )
                #logger.info( 'installer connecting %s %s', iid, connectingDateTime )
                instancesAllocated[iid]['connectingDateTime'] = connectingDateTime
            if 'operation' in event and 'command' in event['operation']:
                # start of installation for this instance
                installingDateTime = dateutil.parser.parse( event['dateTime'] )
                #logger.info( 'installer starting %s %s', iid, installingDateTime )
                connectingDur = (installingDateTime-connectingDateTime).total_seconds()
            if 'stderr' in event:
                stderrLines.append( event['stderr'] )

            # calculate and store duration when getting an ending event
            if ('returncode' in event) or ('exception' in event) or ('timeout' in event):
                iid = event['instanceId']
                endDateTime = dateutil.parser.parse( event['dateTime'] )
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


    if csvOutFilePath:
        dbPreexisted = os.path.isfile( csvOutFilePath )

        if True:  #  not dbPreexisted:
            with open( csvOutFilePath, 'w' ) as csvOutFile:
                print( 'eventType,devId,state,code,dateTime,dur,instanceId,country,sshAddr,storageFree,ramTotal,arch,nCores,freq1,freq2,families,appVersion,ref',
                    file=csvOutFile )

        with open( csvOutFilePath, 'a' ) as csvOutFile:
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
                #logger.info( '%s,%d,%s,%s,%s,%.1f,%s',
                #    inst['state'], devId, iid, arch, nCores, maxFreq, families )
                instDur = inst.get( 'dur', 0 )
                print( 'launch_install,%d,%s,%s,%s,%.0f,%s,%s,%s,%.1f,%.1f,%s,%s,%.1f,%.1f,"%s",%d,%s' %
                    (devId, inst.get('state'), instCode, startDateTime,
                    instDur, iid, countryCode, sshAddr,
                     storageFree, ramTotal, arch, nCores, maxFreq, minFreq,
                     families, appVersion, installerCollName)
                    , file=csvOutFile )

