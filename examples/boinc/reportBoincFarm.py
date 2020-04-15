'''reports details about a virtual boinc farm'''
# standard library modules
import argparse
#import collections
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
import pandas as pd
import pymongo


logger = logging.getLogger(__name__)


def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def isNumber( sss ):
    try:
        float(sss)
        return True
    except ValueError:
        return False

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
        startedInstances.extend( [inst for inst in inRecs if inst['state'] == 'started'] )
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


if __name__ == "__main__":
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--farm', required=True, help='the name of the virtual boinc farm' )
    ap.add_argument( '--mongoHost', help='the host of mongodb server', default='localhost')
    ap.add_argument( '--mongoPort', help='the port of mongodb server', default=27017)
    args = ap.parse_args()

    farm = 'rosetta_2'
    mclient = pymongo.MongoClient( args.mongoHost )
    dbName = 'boinc_' + farm
    db = mclient[dbName]

    lookbackMinutes = 240
    thresholdDateTime = datetime.datetime.now( datetime.timezone.utc ) \
        - datetime.timedelta( minutes=lookbackMinutes )
    thresholdDateTimeStr = thresholdDateTime.isoformat()
    dateTimeTagFormat = '%Y-%m-%d_%H%M%S'
    
    startedInstances = getStartedInstances( db )
    instancesByIid = {inst['instanceId']: inst for inst in startedInstances }

    print( 'recently checked instances for farm', farm )
    nCheckedInstances = 0
    nSuccessfulInstances = 0
    coll = db['checkedInstances']
    checkedInstances = list( coll.find().sort('checkedDateTime', -1) )
    
    for inst in checkedInstances:
        nCheckedInstances += 1
        cdtField = inst['checkedDateTime']
        cdt = dateutil.parser.parse( cdtField )
        ldtField = inst['launchedDateTime']
        ldt = dateutil.parser.parse( ldtField )
        inst['uptime'] = (cdt-ldt).total_seconds()
        
        instSucceeded = inst.get('nSuccTasks',0) > 0
        if instSucceeded:
            nSuccessfulInstances += 1
        if cdtField >= thresholdDateTimeStr:
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            iid = inst['_id']
            abbrevIid = iid[0:16]
            print( '%s, %d, %d, %d, %d, %d, %s, %s, %.1f' % 
                  (inst['state'], inst['devId'],
                   inst.get('nSuccTasks',0), inst['nCurTasks'],
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevIid,
                   inst['uptime']/3600) )
        #else:
        #    print( 'older' )
    checkedInstancesDf = pd.DataFrame( checkedInstances )
    nCurrentlyRunning = len( checkedInstancesDf[ checkedInstancesDf.state=='checked' ] )
    print( 'there were %d checked instances; %d currently running' % 
          (nCheckedInstances, nCurrentlyRunning ) )
    print( '\n%d instances had finished tasks' % nSuccessfulInstances )
    
    # print details for instances with finished tasks
    for inst in checkedInstances:
        if inst.get('nSuccTasks',0) > 0:
            cdtField = inst['checkedDateTime']
            cdt = dateutil.parser.parse( cdtField )
            cdtAbbrev = cdt.strftime( dateTimeTagFormat )[5:-2]
            iid = inst['_id']
            abbrevIid = iid[0:16]
            host = inst['ssh']['host']
            hostId = inst.get( 'bpsHostId' )
            abbrevHost = host.split('.')[0]
            print( '%s, %d, %d, %d, %d, %d, %s, %s, %s, %.1f' % 
                  (inst['state'], inst['devId'],
                   inst.get('nSuccTasks',0), inst['nCurTasks'],
                   inst.get('nFailures',0), inst.get('nExceptions',0),
                   cdtAbbrev, abbrevHost, abbrevIid,
                   inst['uptime']/3600) )
    print()
    sys.stdout.flush()
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
    # collect ram titals
    #ramByDevId = {}
    #for inst in startedInstances:
    #    ramByDevId[ inst['device-id'] ] = inst['ram']['total'] / 1000000
    
    if False:
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
            
    if True:
        instRecs = getInstallerRecs( db )
        logger.info( 'found %d installation attempts', len(instRecs) )
        installerDf = pd.DataFrame( instRecs.values() )
        installerDf['devId'] = installerDf.instanceId.map( lambda x: instancesByIid[x]['device-id'] )
        