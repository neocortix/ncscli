'''reports details about a virtual folding@home farm'''
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
import re
#import socket
#import shutil
#import signal
#import socket
#import subprocess
import sys
#import threading
#import time
#import urllib
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
    #logger.info( 'inst: %s', inst['instanceId'] )
    #raise Exception( 'OBSOLETE' )
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
        inRecs = list( launchedColl.find( {}, 
            {'device-id': 1, 'cpu': 1, 'dpr': 1, 'instanceId': 1, 'state': 1 }) 
            ) # fully iterates the cursor, getting all records
        if len(inRecs) <= 0:
            logger.warn( 'no launched instances found in %s', collName )
        for inRec in inRecs:
            if 'instanceId' not in inRec:
                logger.warning( 'no instance ID in input record')
            if 'dpr' in inRec:
                dpr = inRec['dpr']
                #logger.info( 'found instance dpr %.1f', dpr )
                if dpr < 24:
                    logger.info( 'low dpr found %.1f %s', dpr, inRec['instanceId'] )
            else:
                dpr = instanceDpr( inRec )
                #if dpr < 25:
                #    logger.info( 'low dpr computed %.1f %s', dpr, inRec )
            inRec['dpr'] = round( dpr )
        startedInstances.extend( [inst for inst in inRecs if inst['state'] in ['started', 'stopped'] ] )
    return startedInstances

def getInstallerRecs( db ):
    instRecs = {}
    colls = db.list_collection_names( filter={ 'name': {'$regex': r'^startFah_.*'} } )
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
    try:
        tree = lxml.etree.fromstring(resp.content)
    except Exception as exc:
        logger.error( 'exception from lxml (%s) %s', type(exc), exc )
        return {}
    
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
    ap.add_argument( '--farm', required=True, help='the name of the virtual farm' )
    #ap.add_argument( '--projectUrl', help='the URL to the science project', default='https://stats.foldingathome.org/' )
    #ap.add_argument( '--projectKey', required=True, help='the authorization key for the science project')
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    args = ap.parse_args()

    farm = args.farm
    
    logger.info( 'connecting to database for %s', farm )
    mclient = pymongo.MongoClient( args.mongoHost )
    dbName = 'fah_' + farm
    db = mclient[dbName]

    # establish threshold datetime for "recent" terminations
    lookbackMinutes = 120
    thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
        - datetime.timedelta( minutes=lookbackMinutes )
    thresholdDateTimeStr = thresholdDateTime.isoformat()
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'
    
    # (hack) ancient datetime for instances too old for ncs details to be queried
    ancientDateTime = datetime.datetime( 2020, 7, 1, tzinfo=datetime.timezone.utc )
    
    logger.info( 'getting startedInstances' )
    startedInstances = getStartedInstances( db )
    instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

    #projUrl = args.projectUrl
    #authStr = args.projectKey  # account key for project ("weak" key may not work)
    
    '''
    logger.info( 'retrieving account info from %s', projUrl )
    accountInfo = retrieveAccountInfo( projUrl, authStr )
    hostInfos = accountInfo.get('hosts')
    if hostInfos:
        hostInfoByName = { host['domain_name']: host for host in hostInfos }
    else:
        hostInfoByName = {}


    if not True:
        taskMetrics = collectTaskMetrics( db )
    '''

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
        if inst.get('queueInfo'):
            inst['ppd'] = int(inst['queueInfo']['ppd'])
            inst['creditEstimate'] = int(inst['queueInfo']['creditestimate'])
        instEvents = None
        if 'events' in inst:
            instEvents = inst['events']
        elif inst['checkedDateTime'] >= ancientDateTime:
            logger.info( 'querying NCS for %s', abbrevIid )
            response = ncs.queryNcsSc( 'instances/%s' % iid, args.authToken )
            if response['statusCode'] == 200:
                instX = response['content']
                instEvents = instX['events']
                inst['events'] = instX['events']
                coll.update_one( {'_id': iid}, { "$set": { "events": instX['events'] } } )
        cdt = inst['checkedDateTime']
        ldtField = inst['launchedDateTime']
        ldt = universalizeDateTime( dateutil.parser.parse( ldtField ) )
        inst['launchedDateTime'] = universalizeDateTime( ldt )
        if inst.get('terminatedDateTime'):
            inst['terminatedDateTime'] = interpretDateTimeField( inst['terminatedDateTime'] )
        inst['uptime'] = (cdt-ldt).total_seconds()
        inst['uptimeHrs'] = inst['uptime'] / 3600
        inst['dpr'] = instancesByIid[iid]['dpr']
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
        #if 'ztka' in instHost:
        #    logger.warning( 'DOWNLOADERR %s', inst['_id'] )
        inst['totCredit'] = 0
        inst['RAC'] = 0
        '''
        if instHost and instHost in hostInfoByName:
            inst['totCredit'] = hostInfoByName[instHost]['total_credit']
            inst['RAC'] = hostInfoByName[instHost]['expavg_credit']
        '''
            
    checkedInstancesDf = pd.DataFrame( checkedInstances )
    checkedInstancesDf = pd.concat([checkedInstancesDf, checkedInstancesDf['simInfo'].apply(pd.Series)], axis=1)
    # drop the mysteriouos zero column created by the concat
    #checkedInstancesDf = checkedInstancesDf.drop(0,1)
    criticalDateTime = datetime.datetime( 2020, 10, 17, 17, 53, tzinfo=datetime.timezone.utc )
    checkedInstancesDf = checkedInstancesDf[ checkedInstancesDf.launchedDateTime>=criticalDateTime ]
    print( '%d instances were launched since %s', len(checkedInstancesDf), criticalDateTime )

    '''
    checkedInstancesDf['totCredit'] = 0
    checkedInstancesDf['nUploads'] = 0
    nEstimates = 0
    defaultCred = 300  # use a typical value as default
    #with open( 'uploadMsgs.log', 'w' ) as logFile:
    for index, row in checkedInstancesDf.iterrows():
        print( '.', end='' )
        iid = row.instanceId
        loggedCollName = 'clientLog_' + iid
        found = db[loggedCollName].find( {} )
        for item in found:
            msg = item['msg']
            if ':Final credit estimate' in msg:
                nEstimates += 1
                parts = msg.split()
                creditEst = float( parts[-2] ) # the part before 'points'
                #print( creditEst )
                #print( 'updating creditEst-defaultCred', 'creditEst', creditEst, 'defaultCred', defaultCred )
                checkedInstancesDf.loc[index,'totCredit'] += creditEst-defaultCred
                #checkedInstancesDf.loc[index,'totCredit'] += creditEst
            elif ':Sending unit results:' in msg:
                checkedInstancesDf.loc[index,'nUploads'] += 1
                checkedInstancesDf.loc[index,'totCredit'] += defaultCred
            #print( iid[0:8], item['dateTime'][0:19], item['msg'], file=logFile )
        # make estimate for cahses with upload but no credit est
        if checkedInstancesDf.loc[index,'nUploads'] >0 and checkedInstancesDf.loc[index,'totCredit']<=0:
            totCred = checkedInstancesDf.loc[index,'nUploads'] * defaultCred
            print( 'estimating %d credit for %s' % (totCred, row.instanceId[0:8]) )
            checkedInstancesDf.loc[index,'totCredit'] = totCred
    print( 'sum of totCredit: ', checkedInstancesDf['totCredit'].sum() )
    print( 'number of credit estimates', nEstimates )
    print( 'number of uploads', checkedInstancesDf.nUploads.sum() )
    '''
    logger.info( 'counting uploads' )
    # search for "upload complete" in logs and store counts in 'nUploads' column
    checkedInstancesDf['nUploads'] = 0
    checkedInstancesDf['nUpFails'] = 0
    downloadMsgsByIid = {}
    uploadMsgsByIid = {}
    pat = r':Completed (.*) out of'
    for index, row in checkedInstancesDf.iterrows():
        print( '.', end='' )
        iid = row.instanceId
        loggedCollName = 'clientLog_' + iid
        
        found = db[loggedCollName].find( {
                "mType": "complete",
                "msg": {'$regex': '.*:Completed.*'} }
            )
        for item in found:
            msg = item['msg']
            if '500000 steps' not in msg:
                print( 'NSTEPS', msg )
            numPart = re.search( pat, msg ).group(1)
            nSteps = int( numPart )
            if nSteps > 50000:
                checkedInstancesDf.loc[index,'nSteps'] = nSteps
            #if nSteps > 490000:
            #    print( iid, item['dateTime'], msg )
        
        nSigInts = db[loggedCollName].count_documents( 
                {"mType": None, "msg": {'$regex': '.*Caught signal SIGINT.*'} }
                )
        if nSigInts > 0:
            logger.warning( '%d SIGINT for %s', nSigInts, iid[0:8] )
            
        downloadMsgsByIid[ iid ] = []
        # find 'received unit" messages
        found = db[loggedCollName].find( {
                #"mType": 'upload',
                "msg": {'$regex': '.*:Received Unit:*'} }
            )
        for item in found:
            msg = item['msg']
            downloadMsgsByIid[ iid ].append( msg )

        uploadMsgsByIid[ iid ] = []
        # find 'sending" messages
        found = db[loggedCollName].find( {
                "mType": 'upload',
                "msg": {'$regex': '.*Sending unit results.*'} }
            )
        for item in found:
            msg = item['msg']
            uploadMsgsByIid[ iid ].append( msg )
            #db[loggedCollName].update_one( {'_id': item['_id' ]}, 
            #  {'$set': { 'mType': 'upload' } }  )
            if ' error:FAILED ' in msg:
                checkedInstancesDf.loc[index,'nUpFails'] += 1
            else:
                logger.info( '"upload" msg for inst %s, %s', iid[0:8], msg )
            checkedInstancesDf.loc[index,'nUploads'] += 1
            if checkedInstancesDf.loc[index].get('nSteps',0) > 40000:
                creditEst = checkedInstancesDf.loc[index].get('creditEstimate', 800)
                logger.info( 'creditEst for inst %s, %s', iid[0:8], creditEst )
            else:
                creditEst = 1
            checkedInstancesDf.loc[index,'totCredit'] += creditEst
            #print( iid[0:8], item['dateTime'][0:19], item['msg'], file=logFile )
    
    logger.info( 'counting uploads done' )
    print()
    # this loop prints info for recently-checked terminated instances
    print( 'recently terminated instances for farm', farm )
    for _, inst in checkedInstancesDf.iterrows():
        nCheckedInstances += 1
        cdt = inst['checkedDateTime']
        iid = inst['instanceId']
        
        instSucceeded = inst.get('nUploads',0) > inst.get('nUpFails',0)
        if instSucceeded:
            nSuccessfulInstances += 1
        if cdt >= thresholdDateTime and inst['state'] == 'terminated':
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            abbrevIid = iid[0:16]
            print( '%s, %d, %d, %d, %d, %d, %s, %s, %.1f, %s' % 
                  (inst['state'], inst['devId'],
                   inst.get('nTasksComputed',0), inst.get('nCurTasks', 0),
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevIid,
                   inst['uptime']/3600, inst.get('unplugged','')) )
        #else:
        #    print( 'older' )

    print()
    print(datetime.datetime.now( datetime.timezone.utc ))
    print('folding farm', args.farm)
    '''
    projName = projUrl.split('/')[-2]
    projNetLoc = urllib.parse.urlparse( projUrl ).netloc
    #print( 'project', projNetLoc, 'stats' )
    print( 'stats for project %s (%s)' % (projName, projNetLoc)  )
    if 'total_credit' not in accountInfo:
        print( 'not available' )
        print( accountInfo.get( 'error_msg', '' ) )
    else:
        print( 'total credit:', round(accountInfo['total_credit']) )
        print( 'RAC:', round(accountInfo['expavg_credit']) )  # "recent avgerage credit"
        '''
    #criticalDateTime = datetime.datetime( 2020, 10, 17, 17, 53, tzinfo=datetime.timezone.utc )
    #recentInstances = checkedInstancesDf[ checkedInstancesDf.launchedDateTime>=criticalDateTime ]
    #print( '%d instances were launched since %s', len(recentInstances), criticalDateTime )

    nCurrentlyRunning = len( checkedInstancesDf[ checkedInstancesDf.state=='checked' ] )
    print( '%d currently running instances' % 
          (nCurrentlyRunning ) )
    currentlySuccessful = checkedInstancesDf[ (checkedInstancesDf.state=='checked') 
        & (checkedInstancesDf.nUploads>checkedInstancesDf.nUpFails) ]
    print( '%d of those had good uploads' % len(currentlySuccessful) )
    print( 'historically, %d out of %d instances had attempted uploads' 
          % (nSuccessfulInstances, nCheckedInstances) )
    print( '%d total uploads' % (checkedInstancesDf.nUploads.sum()-checkedInstancesDf.nUpFails.sum()) )
    print( 'estimated total credit:', checkedInstancesDf.totCredit.sum() )
    goldDpr = 48
    lowRiders = checkedInstancesDf[(checkedInstancesDf.dpr<goldDpr) & (checkedInstancesDf.dpr>=24)]
    if 'totCredit' in lowRiders:
        print( 'silver (sub-%d) total credit: %d'
              % (goldDpr, round(lowRiders.totCredit.sum()) )
              )
    print()
    # print details for the best instances with finished tasks
    if len( checkedInstancesDf ) > 20:
        bestCreditThresh = max( checkedInstancesDf.totCredit.sort_values(ascending=False).iloc[20], 1 )
    else:
        bestCreditThresh = 1
    print( 'best instances' )
    for _, inst in checkedInstancesDf.sort_values(['totCredit', 'nUploads', 'progress' ], ascending=False).iterrows():
        if inst.get('totCredit',0) >= bestCreditThresh:  # 12000 150 300
        #if inst.nUploads or inst.get('progress', 0) >= .5:
            cdt = inst['checkedDateTime']
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            iid = inst['_id']
            abbrevIid = iid[0:8]
            host = inst['ssh']['host']
            hostId = inst.get( 'bpsHostId' )
            abbrevHost = host.split('.')[0]
            print( '%s, %d, %d, %.1f, %d, %d, %s, %s, %s, %.1f' % 
                  (inst['state'], inst['devId'],
                   #inst['totCredit'], inst.get('nTasksComputed',0),
                   inst['nUploads']-inst['nUpFails'], inst.get('totCredit',0),  # 'progress'
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevHost, abbrevIid,
                   inst['uptime']/3600) )
    print()
    #%%
    print( 'longest-dur instances' )
    for _, inst in checkedInstancesDf.sort_values(['uptime', 'totCredit'], ascending=False).iterrows():
        if inst['uptime'] >= 36*3600:
            cdt = inst['checkedDateTime']
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            iid = inst['_id']
            abbrevIid = iid[0:8]
            host = inst['ssh']['host']
            hostId = inst.get( 'bpsHostId' )
            abbrevHost = host.split('.')[0]
            print( '%s, %d, %d, %.1f, %d, %d, %s, %s, %s, %.1f' % 
                  (inst['state'], inst['devId'],
                   #inst['totCredit'], inst.get('nTasksComputed',0),
                   inst['nUploads']-inst['nUpFails'], inst.get('totCredit',0),  # 'progress'
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevHost, abbrevIid,
                   inst['uptime']/3600) )

    sys.stdout.flush()
    
    fahResp = requests.get( 'https://api.foldingathome.org/cpus', params={'query': 'neocortix_0'} )
    fahJson = fahResp.json()
    fahDf = pd.DataFrame( fahJson )
    fahProjectDf = fahDf[ fahDf.project==16813 ].drop( ['team', 'slot', 'os'], 1 )
    print( 'credits via api' )
    print( fahProjectDf )
    

    # collect ram totals
    #ramByDevId = {}
    #for inst in startedInstances:
    #    ramByDevId[ inst['device-id'] ] = inst['ram']['total'] / 1000000
                
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
