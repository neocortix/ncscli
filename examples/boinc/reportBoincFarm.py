'''reports details about a virtual boinc farm'''
# standard library modules
import argparse
import collections
#import contextlib
#from concurrent import futures
#import errno
import datetime
#import getpass
#import json
import logging
#import math
#import os
#import re
#import socket
#import shutil
#import signal
import socket
#import subprocess
import sys
#import threading
#import time
#import uuid

# third-party module(s)
import dateutil.parser
import lxml
import pandas as pd
import pymongo
import requests

# neocortix module(s)
import devicePerformance
import ncs


logger = logging.getLogger(__name__)


def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def datetimeIsAware( dt ):
    if not dt: return None
    return (dt.tzinfo is not None) and (dt.tzinfo.utcoffset( dt ) is not None)

def universalizeDateTime( dt ):
    if not dt: return None
    if datetimeIsAware( dt ):
        #return dt
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace( tzinfo=datetime.timezone.utc )

def interpretDateTimeField( field ):
    if isinstance( field, datetime.datetime ):
        return universalizeDateTime( field )
    elif isinstance( field, str ):
        return universalizeDateTime( dateutil.parser.parse( field ) )
    else:
        raise TypeError( 'datetime or parseable string required' )

def isNumber( sss ):
    try:
        float(sss)
        return True
    except ValueError:
        return False

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
    return dpr


def getStartedInstances( db ):
    collNames = db.list_collection_names( filter={ 'name': {'$regex': r'^launchedInstances_.*'} } )
    #logger.info( 'launched collections: %s', collNames )
    startedInstances = []
    for collName in collNames:
        #logger.info( 'getting instances from %s', collName )
        launchedColl = db[collName]
        inRecs = list( launchedColl.find() ) # fully iterates the cursor, getting all records
        if len(inRecs) <= 0:
            logger.warn( 'no launched instances found in %s', collName )
        for inRec in inRecs:
            if 'instanceId' not in inRec:
                logger.warning( 'no instance ID in input record')
            inRec['dpr'] = round( instanceDpr( inRec ) )
        startedInstances.extend( [inst for inst in inRecs if inst['state'] in ['started', 'stopped'] ] )
    return startedInstances

def getInstallerRecs( db ):
    instRecs = {}
    colls = db.list_collection_names( filter={ 'name': {'$regex': r'^startBoinc_.*'} } )
    colls = sorted( colls, reverse=False )
    for collName in colls:
        found = db[collName].find( {"instanceId": {"$ne": "<master>"} } )
        for event in found:
            iid = event['instanceId']
            if anyFound( ['exception', 'returncode', 'timeout'], event ):
                if iid in instRecs:
                    logger.warning( 'alread had instRec for %s', iid )
                if 'exception' in event:
                    event['status'] = 'exception'
                    event['exceptionType'] = event['exception']['type']  # redundant
                elif 'returncode' in event:
                    event['status'] = 'failed' if event['returncode'] else 'ok'
                elif 'timeout' in event:
                    event['status'] = 'timeout'
                instRecs[iid] = event
    return instRecs

def parseTaskLines( lines ):
    tasks = []
    curTask = {}
    firstLine = True
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        if firstLine and '== Tasks ==' in line:
            continue
        if line[0] != ' ':
            #logger.info( 'task BOUNDARY %s', line )
            numPart = line.split( ')' )[0]
            taskNum = int(numPart)
            #logger.info( 'TASK %d', taskNum )
            curTask = { 'num': taskNum }
            tasks.append( curTask )
            continue
        if ':' in line:
            # extract a key:value pair from this line
            stripped = line.strip()
            parts = stripped.split( ':', 1 )  # only the first colon will be significant
            # convert to numeric or None type, if appropriate
            val = parts[1].strip()
            if val is None:
                pass
            elif val.isnumeric():
                val = int( val )
            elif isNumber( val ):
                val = float( val )
            # store the value
            curTask[ parts[0] ] = val
            continue
        logger.info( '> %s', line )
    return tasks

def collectTaskMetrics( db ):
    allTasks = pd.DataFrame()

    collNames = sorted( db.list_collection_names(
        filter={ 'name': {'$regex': r'^get_tasks_.*'} } )
    )
    #logger.info( 'get_tasks_ collections: %s', collNames )
    for collName in collNames:
        logger.info( 'getting data from %s', collName )
        coll = db[collName]
        # get the tag from the collection name e.g. 'get_tasks_2020-04-09_230320'
        dateTimeTag = collName.split('_',2)[2]
        
        # iterate over records, each containing output for an instance
        for inRec in coll.find():
            iid = inRec['instanceId']
            eventDateTime = inRec['dateTime']
            taskLines = []
            if iid == '<master>':
                #logger.info( 'found <master> record' )
                pass
            else:
                #logger.info( 'iid: %s', iid )
                events = inRec['events']
                for event in events:
                    if 'stdout' in event:
                        stdoutStr =  event['stdout']
                        taskLines.append( stdoutStr )
                        #logger.info( '%s: %s', abbrevIid, stdoutStr )
                        #if anyFound( ['WU name:', 'fraction done', 'UNINITIALIZED'], stdoutStr ):
                        #    logger.info( "%s: %s, %s", abbrevIid, eventDateTime[0:19], stdoutStr.strip() )
            tasks = pd.DataFrame( parseTaskLines( taskLines ) )
            #print( 'tasks for', abbrevIid, 'from', eventDateTime[0:19] )
            #print( tasks  )
            tasks['dateTimeTag'] = dateTimeTag
            tasks['eventDateTime'] = eventDateTime
            tasks['instanceId'] = iid
            allTasks = allTasks.append( tasks, sort=False )
    return allTasks

def reportExceptionRecoveriesHackerly( db ):
    instHistories = {}
    taskColls = db.list_collection_names( filter={ 'name': {'$regex': r'^get_tasks_.*'} } )
    taskColls = sorted( taskColls, reverse=False )
    for collName in taskColls:
        #tcoll = db['get_tasks_2020-04-13_190241']
        tcoll = db[collName]
        found = tcoll.find( {"instanceId": {"$ne": "<master>"} } )
        for instRec in found:
            iid = instRec['instanceId']
            if iid not in instHistories:
                instHistories[iid] = []
            #logger.info( 'scanning %s', instRec['instanceId'] )
            for event in instRec['events']:
                if 'exception' in event:
                    #logger.info( 'found exception for %s', iid[0:16] )
                    #logger.info( 'exc: %s', event['exception'] )
                    #if event['exception']['type'] == 'gaierror':
                    if event['exception']['type'] == 'ConnectionRefusedError':
                        instHistories[iid].append( event )
                elif 'returncode' in event:
                    if event['returncode'] == 0:
                        instHistories[iid].append( event )
    logger.info( 'scanning event histories' )
    for iid, history in instHistories.items():
        hadExcept = False
        for event in history:
            if 'exception' in event:
                if not hadExcept:
                    print( iid, event['dateTime'] )
                    #logger.info( '%s on %s at %s', event['exception']['type'], iid[0:16], event['dateTime'][0:16] )
                hadExcept = True
            elif 'returncode' in event:
                if hadExcept:
                    logger.info('recovered %s %s', iid[0:16], event['dateTime'][0:16])
                #else:
                #    logger.info( 'noExcept' )
        #if hadExcept:
        #    logger.info( 'hadExcept: %s', iid[0:16] )

def maybeNumber( txt ):
    if txt is None:
         return None
    elif txt.isnumeric():
        return int( txt )
    elif isNumber( txt ):
        return float( txt )
    else:
        return txt
    
def retrieveAccountInfo( projUrl, authStr ):
    reqUrl = projUrl + 'show_user.php?format=xml&auth=' + authStr
    resp = requests.get( reqUrl )  # do need this
    tree = lxml.etree.fromstring(resp.content)
    
    hostInfos = []
    accountInfo = {}
    logger.info( '%d elements in xml tree', len( tree ))
    for urlElem in tree:
        children = urlElem.getchildren()
        if urlElem.tag == 'host':
            #print( 'host with %d children' % len(children)   )
            hostInfo = {}
            for child in children:
                #print( child.text )
                tag = child.tag
                hostInfo[ tag ] = maybeNumber( child.text )
            hostInfos.append( hostInfo )
        else:
            #print( urlElem.tag, urlElem.text )
            accountInfo[urlElem.tag] = maybeNumber( urlElem.text )
    accountInfo['hosts'] = hostInfos
    return accountInfo


if __name__ == "__main__":
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (required for launch or terminate)' )
    ap.add_argument( '--farm', required=True, help='the name of the virtual boinc farm' )
    ap.add_argument( '--projectKey', required=True, help='the authorization key for the boinc project')
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    args = ap.parse_args()

    farm = args.farm  # 'rosetta_2'
    
    logger.info( 'connecting to database' )
    mclient = pymongo.MongoClient( args.mongoHost )
    dbName = 'boinc_' + farm
    db = mclient[dbName]

    lookbackMinutes = 90
    thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
        - datetime.timedelta( minutes=lookbackMinutes )
    thresholdDateTimeStr = thresholdDateTime.isoformat()
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'
    
    ancientDateTime = datetime.datetime( 2020, 4, 12, tzinfo=datetime.timezone.utc )
    
    logger.info( 'getting startedInstances' )
    startedInstances = getStartedInstances( db )
    instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

    # rosetta
    projUrl = 'https://boinc.bakerlab.org/rosetta/'
    authStr = args.projectKey  # account key for boinc project ("weak" key may not work)
    
    logger.info( 'retrieving account info from %s', projUrl )
    accountInfo = retrieveAccountInfo( projUrl, authStr )
    hostInfos = accountInfo['hosts']
    hostInfoByName = { host['domain_name']: host for host in hostInfos }


    if not True:
        taskMetrics = collectTaskMetrics( db )

    nCheckedInstances = 0
    nSuccessfulInstances = 0
    coll = db['checkedInstances']
    # iterate fully into a list, so we can freely random-access it
    checkedInstances = list(coll.find().sort('checkedDateTime', -1) )

    eventCounter = collections.Counter()
    eventsByIid = {}
    pausings = []
    
    # this first loop does a little massaging of the records
    for inst in checkedInstances:
        iid = inst['_id']
        abbrevIid = iid[0:16]
        inst['instanceId'] = iid
        inst['checkedDateTime'] = interpretDateTimeField( inst['checkedDateTime'] )
        instEvents = None
        if 'events' in inst:
            instEvents = inst['events']
        elif inst['checkedDateTime'] >= ancientDateTime:  # '2020-04-12':
            logger.info( 'querying NCS for %s', abbrevIid )
            response = ncs.queryNcsSc( 'instances/%s' % iid, args.authToken )
            if response['statusCode'] == 200:
                instX = response['content']
                instEvents = instX['events']
                inst['events'] = instX['events']
                coll.update_one( {'_id': iid}, { "$set": { "events": instX['events'] } } )
        if instEvents:
            eventsByIid[ iid ] = instEvents
            lastEventStr = instEvents[-1]['category'] + '.' + instEvents[-1]['event']
            #logger.info( '%s had %d events, including "%s"', 
            #    abbrevIid, len(instEvents), lastEventStr )
            inst['lastEvent'] = lastEventStr
            unplugged = False
            for event in instEvents:
                if event['event'] == 'unplugged':
                    unplugged = True
                elif event['event'] == 'paused':
                    event['instanceId'] = iid
                    pausings.append( event )
                    inst['paused'] = True
                elif event['event'] == 'resumed':
                    event['instanceId'] = iid
                    pausings.append( event )
                    inst['paused'] = False
                elif event['event'] == 'disconnected':
                    inst['disconnected'] = True
                eventCounter[ event['category'] + '.' + event['event'] ] += 1
            inst['unplugged'] = unplugged
        instHost = inst['ssh']['host']
        inst['hostName'] = instHost
        if 'ztka' in instHost:
            logger.warning( 'DOWNLOADERR %s', inst['_id'] )
        if instHost and instHost in hostInfoByName:
            inst['totCredit'] = hostInfoByName[instHost]['total_credit']
            inst['RAC'] = hostInfoByName[instHost]['expavg_credit']
            
    # this loop prints info for recently-checked terminated instances
    print( 'recently terminated instances for farm', farm )
    for inst in checkedInstances:
        nCheckedInstances += 1
        cdt = inst['checkedDateTime']
        ldtField = inst['launchedDateTime']
        ldt = universalizeDateTime( dateutil.parser.parse( ldtField ) )
        inst['launchedDateTime'] = universalizeDateTime( ldt )
        if inst.get('terminatedDateTime'):
            inst['terminatedDateTime'] = interpretDateTimeField( inst['terminatedDateTime'] )
        inst['uptime'] = (cdt-ldt).total_seconds()
        iid = inst['_id']
        inst['instanceId'] = iid
        inst['dpr'] = instancesByIid[iid]['dpr']
        
        instSucceeded = inst.get('nTasksComputed',0) > 0
        if instSucceeded:
            nSuccessfulInstances += 1
        if cdt >= thresholdDateTime and inst['state'] == 'terminated':
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            abbrevIid = iid[0:16]
            print( '%s, %d, %d, %d, %d, %d, %s, %s, %.1f, %s' % 
                  (inst['state'], inst['devId'],
                   inst.get('nTasksComputed',0), inst['nCurTasks'],
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevIid,
                   inst['uptime']/3600, inst.get('unplugged','')) )
        #else:
        #    print( 'older' )
    checkedInstancesDf = pd.DataFrame( checkedInstances )

    print()
    print(datetime.datetime.now( datetime.timezone.utc ))
    projName = projUrl.split('/')[-2]
    print( 'project', projName, 'stats' )
    if 'total_credit' not in accountInfo:
        print( 'not available' )
        print( accountInfo.get( 'error_msg', '' ) )
    else:
        print( 'total credit:', round(accountInfo['total_credit']) )
        print( 'RAC:', round(accountInfo['expavg_credit']) )  # "recent avgerage credit"

    nCurrentlyRunning = len( checkedInstancesDf[ checkedInstancesDf.state=='checked' ] )
    print( '%d currently running instances' % 
          (nCurrentlyRunning ) )
    currentlySuccessful = checkedInstancesDf[ (checkedInstancesDf.state=='checked') & (checkedInstancesDf.nTasksComputed>0) ]
    print( '%d of those had finished tasks' % len(currentlySuccessful) )
    print( 'historically, %d out of %d instances had finished tasks' 
          % (nSuccessfulInstances, nCheckedInstances) )
    print()
    # print details for the best instances with finished tasks
    print( 'best instances' )
    for inst in checkedInstances:
        #if inst.get('nTasksComputed',0) > 0:
        if inst.get('totCredit',0) >= 7000:
            cdt = inst['checkedDateTime']
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            iid = inst['_id']
            abbrevIid = iid[0:16]
            host = inst['ssh']['host']
            hostId = inst.get( 'bpsHostId' )
            abbrevHost = host.split('.')[0]
            print( '%s, %d, %d, %d, %d, %d, %s, %s, %s, %.1f' % 
                  (inst['state'], inst['devId'],
                   inst['totCredit'], inst.get('nTasksComputed',0),
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevHost, abbrevIid,
                   inst['uptime']/3600) )
    print()

    sys.stdout.flush()
    if False:
        logger.info( 'checking dns for instances' )
        nChecked = 0
        badGais = []
        for inst in checkedInstances:
            if inst['state'] == 'checked':
                nChecked += 1
                iid = inst['_id']
                abbrevIid = iid[0:16]
                #logger.info( 'checking dns for %s', abbrevIid )
                host = inst['ssh']['host']
                port = inst['ssh']['port']
                try:
                    info = socket.getaddrinfo( host, port )
                except Exception as exc:
                    logger.warning( 'gai failed for host "%s", port %d, %s', 
                                   host, port, iid )
                    logger.warning( 'error (%d) %s', exc.errno, exc )
                    if exc.errno != socket.EAI_NONAME:
                        logger.warning( '(unusual error)' )
                    badGais.append( (inst, exc ))
        logger.info( '%d bad gai out of %d checked', len(badGais), nChecked)   
    # collect ram totals
    #ramByDevId = {}
    #for inst in startedInstances:
    #    ramByDevId[ inst['device-id'] ] = inst['ram']['total'] / 1000000
    lowRiders = checkedInstancesDf[(checkedInstancesDf.dpr<39) & (checkedInstancesDf.dpr>=24)]
    
    if False:
        reportExceptionRecoveriesHackerly( db )
            
    if False:
        instRecs = getInstallerRecs( db )
        logger.info( 'found %d installation attempts', len(instRecs) )
        installerDf = pd.DataFrame( instRecs.values() )
        installerDf['devId'] = installerDf.instanceId.map( lambda x: instancesByIid[x]['device-id'] )

    if False:
        # find instance events related to stoppage
        #for inst in startedInstances:
        for inst in checkedInstances:
            if inst['nTasksComputed'] <= 0:
                iid = inst['instanceId']
                abbrevIid = iid[0:16]
                response = ncs.queryNcsSc( 'instances/%s' % iid, args.authToken )
                if response['statusCode'] == 200:
                    inst = response['content']
                    instEvents = inst['events']
                    for event in instEvents:
                        if event['category'] == 'charger':
                            logger.info( '%s %s', abbrevIid, event )
    if 'totCredit' in lowRiders:
        print( 'sub-33 total credit:', round(lowRiders[lowRiders.dpr<33].totCredit.sum()))
