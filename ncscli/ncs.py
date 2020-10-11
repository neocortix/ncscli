#!/usr/bin/env python3
"""
Command-line interface for Neocortix Scalable Compute
"""
# standard library modules
import argparse
import collections
from concurrent import futures
import json
import logging
import os
import signal
import sys
import random
import time
import uuid

# third-party modules
import requests

__version__ = '0.12.10'
logger = logging.getLogger(__name__)


# possible place for globals is this class's attributes
class g_:
    signaled = False

def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.warning( 'SIGTERM received; will try to shut down gracefully' )

def sigtermSignaled():
    return g_.signaled

def sigtermNotSignaled():
    return not sigtermSignaled()


def boolArg( v ):
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def ncscReqHeaders( authToken ):
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Neocortix-Cloud-API-Version": "1",
        "X-Neocortix-Cloud-API-AuthToken": authToken
    }

def queryNcsSc( urlTail, authToken, reqParams=None, maxRetries=20 ):
    #if random.random() > .75:
    #    raise requests.exceptions.RequestException( 'simulated exception' )
    # set long timeouts for requests.get() as a tuple (connection timeout, read timeout) in seconds
    timeouts = (30, 120)
    #timeouts = (1, 2)
    headers = ncscReqHeaders( authToken )
    url = 'https://cloud.neocortix.com/cloud-api/sc/' + urlTail
    # jsonize the reqParams, but only if it's not a string (to avoid jsonizing if already json)
    if not isinstance( reqParams, str ):
        reqParams = json.dumps( reqParams )
    if False:
        logger.info( 'querying url <%s> with data <%s> and headers <%s>', 
            url, reqParams, headers )
    try:
        resp = requests.get( url, headers=headers, data=reqParams, timeout=timeouts )
    except requests.ConnectionError as exc:
        logger.warning( 'exception (%s) %s', type(exc), exc )
        if maxRetries > 0:
            time.sleep( 10 )
            return queryNcsSc( urlTail, authToken, reqParams, maxRetries-1 )
        else:
            return { 'content': {}, 'statusCode': 599 }
    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
        logger.info( 'error url "%s"', url )
        if resp.status_code in range( 500, 600 ):
            if maxRetries > 0:
                time.sleep( 10 )
                return queryNcsSc( urlTail, authToken, reqParams, maxRetries-1 )
    try:
        content = resp.json()
    except Exception:
        content = {}
    return { 'content': content, 'statusCode': resp.status_code }

def getAppVersions( authToken ):
    response = queryNcsSc( 'info/mobile-app-versions', authToken)
    respContent = response['content']
    versions = [x['value'] for x in respContent ]
    logger.debug( 'appVersions: %s', versions )
    return versions

def _updateFromJson( dataDict, jsonStr ):
    if jsonStr:
        try:
            jx = json.loads( jsonStr )
        except Exception:
            logger.error( 'invalid json in filter "%s"', jsonStr )
            raise
        else:
            if jx:
                if not isinstance( jx, dict ):
                    logger.error( 'json in arg is not a dict "%s"', jsonStr )
                    raise TypeError('json in arg is not a dict')
                dataDict.update( jx )

def getAvailableDeviceCount( authToken, filtersJson=None, encryptFiles=True ):
    reqParams = { 'encrypt_files': encryptFiles }
    if filtersJson:
        _updateFromJson( reqParams, filtersJson )
    
    response = queryNcsSc( 'instances', authToken, reqParams )
    respContent = response['content']
    nAvail = respContent.get('available', 0)
    return nAvail

def listSshClientKeys( authToken ):
    headers = ncscReqHeaders( authToken )
    url = 'https://cloud.neocortix.com/cloud-api/profile/ssh-keys'
    logger.info( 'listing keys' )
    resp = requests.get( url, headers=headers )
    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'response code %s', resp.status_code )
        return []
    logger.info( 'response %s', resp.text )
    try:
        keys = resp.json()
    except Exception:
        logger.warning( 'got bad json' )
        return []
    else:
        return keys

def uploadSshClientKey( authToken, keyName, keyContents, maxRetries=20 ):
    headers = ncscReqHeaders( authToken )
    reqData = {
        'title': keyName,
        'key': keyContents
        }
    reqDataStr = json.dumps( reqData )
    url = 'https://cloud.neocortix.com/cloud-api/profile/ssh-keys'
    logger.debug( 'uploading key "%s" %s...', keyName, keyContents[0:16] )
    try:
        resp = requests.post( url, headers=headers, data=reqDataStr )
    except Exception as exc:
        wouldRetry = True
        logger.warning( 'got exception uploading %s (%s) %s', keyName, type(exc), exc )
    else:
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warn( 'response code %s uploading %s', resp.status_code, keyName )
            wouldRetry = resp.status_code in range( 500, 600 )  # 5xx responses are server errors
        else:
            wouldRetry = False
    if wouldRetry and maxRetries > 0:
        time.sleep( 10 )
        logger.info( 'retrying %s (up to %d retries)', keyName, maxRetries )
        return uploadSshClientKey( authToken, keyName, keyContents, maxRetries-1 )
    elif wouldRetry:
        # giving up
        logger.error( 'could not upload %s within maximum retries', keyName )
        return 503  # "service unavailable", but maybe should be different if gotException
    return resp.status_code

def deleteSshClientKey( authToken, keyName, maxRetries=20 ):
    headers = ncscReqHeaders( authToken )
    reqData = {
        'title': keyName,
        }
    reqDataStr = json.dumps( reqData )
    url = 'https://cloud.neocortix.com/cloud-api/profile/ssh-keys/'
    logger.debug( 'deleting SshClientKey %s', keyName )
    try:
        resp = requests.delete( url, headers=headers, data=reqDataStr )
    except Exception as exc:
        wouldRetry = True
        logger.warning( 'got exception (%s) %s', type(exc), exc )
    else:
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warn( 'response code %s', resp.status_code )
            wouldRetry = resp.status_code in range( 500, 600 )  # 5xx responses are server errors
        else:
            wouldRetry = False
    if wouldRetry and maxRetries > 0:
        time.sleep( 10 )
        logger.info( 'retrying %s (up to %d retries)', keyName, maxRetries )
        return deleteSshClientKey( authToken, keyName, maxRetries-1 )
    elif wouldRetry:
        # giving up
        logger.error( 'could not succeed within maximum retries' )
        return 503  # "service unavailable", but maybe should be different if gotException
    return resp.status_code

def launchScInstancesAsync( authToken, encryptFiles, numReq=1,
        regions=[], abis=[], sshClientKeyName=None, jsonFilter=None,
        jobId=None, okToContinueFunc=None, maxRetries=20 ):
    def shouldBreak():
        if okToContinueFunc and not okToContinueFunc():
            #logger.warning( 'not okToContinue')
            return True
        return False
    appVersions = getAppVersions( authToken )
    if not appVersions:
        # something is very wrong
        logger.error( 'could got get AppVersions from server')
        return {}
    #minAppVersion = 1623
    #latestVersion = max( appVersions )

    headers = ncscReqHeaders( authToken )

    if jobId:
        reqId = jobId
    else:
        reqId = str( uuid.uuid4() )

    reqData = {
        #"user":"hacky.sack@gmail.com",
        #'mobile-app-versions': [minAppVersion, latestVersion],
        'abis': abis,
        'encrypt_files': encryptFiles,
        'id': reqId,
        'regions': regions,
        'ssh_key': sshClientKeyName,
        'count': numReq
        }
    if jsonFilter:
        try:
            filters = json.loads( jsonFilter )
        except Exception:
            logger.error( 'invalid json in filter "%s"', jsonFilter )
            raise
        else:
            if filters:
                if not isinstance( filters, dict ):
                    logger.error( 'json in filter is not a dict "%s"', jsonFilter )
                    raise TypeError('json in filter is not a dict')
                reqData.update( filters )
    reqDataStr = json.dumps( reqData )

    logger.debug( 'reqData: %s', reqDataStr )
    url = 'https://cloud.neocortix.com/cloud-api/sc/jobs'
    #logger.info( 'posting with auth %s', authToken )
    try:
        resp = requests.post( url, headers=headers, data=reqDataStr )
    except requests.ConnectionError as exc:
        wouldRetry = True
        logger.warning( 'got ConnectionError from post (%s) %s', type(exc), exc )
    else:
        wouldRetry = False
    if wouldRetry and maxRetries > 0:
        time.sleep( 10 )
        logger.info( 'retrying post (up to %d retries)', maxRetries )
        return launchScInstancesAsync( authToken, encryptFiles, numReq=numReq,
            regions=regions, abis=abis, sshClientKeyName=sshClientKeyName, jsonFilter=jsonFilter,
            jobId=jobId, okToContinueFunc=okToContinueFunc, maxRetries=maxRetries-1 )
    elif wouldRetry:
        # giving up
        logger.error( 'could not post within maximum retries' )
        return {'serverError': 503, 'reqId': reqId}  # "service unavailable", but maybe should be different

    #logger.info( 'response code %s', resp.status_code )
    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
        #TODO need retry code here, but only for specific response codes
        return {'serverError': resp.status_code, 'reqId': reqId}
    else:
        logger.info( 'job request returned (%s) %s', resp.status_code, resp.text )
    queryNeeded = resp.status_code == 200
    logger.debug( 'resp.status_code %d; queryNeeded %s ', resp.status_code, queryNeeded )
    timeLimit = 600 # seconds
    deadline = time.time() + timeLimit
    while queryNeeded:
        jobId = resp.json()['id']
        try:
            logger.debug( 'getting instance list for job %s', jobId )
            resp2 = queryNcsSc( 'jobs/'+jobId, authToken, maxRetries=maxRetries )
        except Exception as exc:
            # the caller must be responsible for killing these
            logger.warning( 'exception getting list of instances (%s) "%s"',
                type(exc), exc )
            return {'serverError': 503, 'reqId': jobId}  # service not available
        else:
            if (resp2['statusCode'] < 200) or (resp2['statusCode'] >= 300):
                # in case of persistent error, return the last error code
                logger.info( 'returning server error')
                return {'serverError': resp2['statusCode'], 'reqId': jobId}
            else:
                #logger.info( 'resp2 content %s', resp2['content'].keys() )
                queryNeeded = resp2['content']['launching']
                if not queryNeeded:
                    return resp2['content']['instances']
                nAllocated = len(resp2['content']['instances'])
                logger.debug( "resp2['content']['launching']: %s", resp2['content']['launching'] )
                if shouldBreak() and nAllocated == 0:
                    logger.info( 'breaking wait-allocate loop because not shouldBreak and no instances' )
                    return {'serverError': 404, 'reqId': jobId}
                if time.time() >= deadline:
                    logger.info( 'breaking wait-allocate loop because of time limit' )
                    if nAllocated > 0:
                        return resp2['content']['instances']
                    else:
                        return {'serverError': 404, 'reqId': jobId}
                logger.info( 'waiting for server (%d instances allocated)', nAllocated )
                time.sleep( 10 )
    return resp.json()

def launchScInstances( authToken, encryptFiles, numReq=1,
        regions=[], abis=[], sshClientKeyName=None, jsonFilter=None,
        jsonOutFile=None, jobId=None, okToContinueFunc=None ):
    def shouldBreak():
        if okToContinueFunc and not okToContinueFunc():
            logger.warning( 'not okToContinue')
            return True
        return False
    if not jobId:
        jobId = str( uuid.uuid4() )
    instances = []
    try:
        try:
            infos = launchScInstancesAsync( authToken, encryptFiles, numReq,
                sshClientKeyName=sshClientKeyName,
                regions=regions, abis=abis, jsonFilter=jsonFilter,
                jobId=jobId, okToContinueFunc=okToContinueFunc )
            if 'serverError' in infos:
                logger.error( 'got error %d', infos['serverError'])
                logger.info( 'attempting to terminate launched instances (please wait)' )
                time.sleep(30)  # possible race condition here
                terminateJobInstances( authToken, infos['reqId'] )
                if jsonOutFile:
                    print( '[]', file=jsonOutFile)
                return infos['serverError']
        except Exception as exc:
            logger.error( 'exception launching instances (%s) "%s"',
                type(exc), exc, exc_info=True )
            return 13  # error 13
        for info in infos:
            instances.append( info )

        # collect the ID of the created instances
        iids = []
        for inst in instances:
            iids.append( inst['id'] )
            #logger.debug( 'created instance %s', inst['id'] )
        logger.info( 'allocated %d instances', len(iids) )

        reqParams = {"show-device-info":True}
        startedInstances = {}
        # wait while instances are still starting, but with a timeout
        timeLimit = 600 # seconds
        deadline = time.time() + timeLimit
        startedSet = set()
        failedSet = set()
        while True:
            starting = False
            launcherStates = collections.Counter()
            for iid in iids:
                if shouldBreak():
                    logger.warning( 'incomplete launch due to okToContinueFunc' )
                    break
                if iid in startedSet:
                    continue
                if iid in failedSet:
                    continue
                try:
                    details = queryNcsSc( 'instances/%s' % iid, authToken, reqParams )['content']
                except Exception as exc:
                    logger.warning( 'exception checking instance state (%s) "%s"',
                        type(exc), exc )
                    continue
                if 'state' in details:
                    iState = details['state']
                else:
                    iState = '<unknown>'
                    logger.warning( 'no "state" in content of response (%s)', details )
                launcherStates[ iState ] += 1
                if iState == 'started':
                    startedSet.add( iid )
                    startedInstances[ iid ] = details
                if iState in ['exhausted', 'ise', 'timedout']:
                    failedSet.add( iid )
                    logger.warning( 'instance state %s for %s', iState, iid )
                if iState == 'initial':
                    logger.debug( '%s %s', iState, iid )
                #if iState in ['initial', 'starting']:
                if iState != 'started':
                    starting = True
                    #logger.debug( '%s %s', iState, iid )
            logger.info( '%d instance(s) launched so far; %s',
                len( startedSet ), launcherStates )
            if not starting:
                break
            if time.time() > deadline:
                logger.warning( 'took too long for some instances to start' )
                break
            if shouldBreak():
                logger.warning( 'incomplete launch due to okToContinueFunc' )
                break
            time.sleep( 10 )


        #nStillStarting = len(iids) - (len(startedSet) + len(failedSet))
        logger.info( 'started %d Instances; %s',
            len(startedSet), launcherStates )

        logger.info( 'querying for device-info')
        # print details of created instances to a json output file
        if jsonOutFile:
            print( '[', file=jsonOutFile )
            jsonFirstElem=True
        for iid in iids:
            try:
                #reqParams = {"show-device-info":True}
                if iid in startedInstances:
                    #logger.debug( 'reusing instance info')
                    details = startedInstances[iid]
                else:
                    logger.info( 're-querying instance info for %s', iid )
                    details = queryNcsSc( 'instances/%s' % iid, authToken, reqParams )['content']
            except Exception as exc:
                logger.error( 'exception getting instance details (%s) "%s"',
                    type(exc), exc )
                continue
            #logger.debug( 'NCSC Inst details %s', details )
            if jsonOutFile:
                outRec = details.copy()
                outRec['instanceId'] = iid
                if jsonFirstElem:
                    jsonFirstElem = False
                else:
                    print( ',', end=' ', file=jsonOutFile)
                print( json.dumps( outRec ), file=jsonOutFile )
            if shouldBreak():
                break
        if jsonOutFile:
            print( ']', file=jsonOutFile)
        logger.debug( 'finished')
        return 0 # no err
    except KeyboardInterrupt:
        logger.warning( 'a launch request was interrupted; %d instances may have been launched', numReq )
        logger.info( 'attempting to terminate launched instances (please wait a half minute)' )
        time.sleep(30)  # possible race condition here
        terminateJobInstances( authToken, jobId )
        raise

def terminateNcscInstance( authToken, iid, maxRetries=1000 ):
    headers = ncscReqHeaders( authToken )
    url = 'https://cloud.neocortix.com/cloud-api/sc/instances/' + iid
    #logger.debug( 'deleting instance %s', iid )
    try:
        resp = requests.delete( url, headers=headers )
    except Exception as exc:
        wouldRetry = True
        logger.warning( 'got exception terminating %s (%s) %s', iid, type(exc), exc )
    else:
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warn( 'response code %s terminating %s', resp.status_code, iid )
            wouldRetry = resp.status_code in range( 500, 600 )  # 5xx responses are server errors
            #wouldRetry = resp.status_code in [502, 504]  # "bad gateway", "gateway timeout"
        else:
            wouldRetry = False
    if wouldRetry and maxRetries > 0:
        time.sleep( 10 )
        logger.info( 'retrying %s (up to %d retries)', iid, maxRetries )
        return terminateNcscInstance( authToken, iid, maxRetries-1 )
    elif wouldRetry:
        # giving up
        logger.error( 'could not terminate %s within maximum retries', iid )
        return 503  # "service unavailable", but maybe should be different if gotException
    return resp.status_code

def terminateJobInstances( authToken, jobId, maxRetries=1000 ):
    headers = ncscReqHeaders( authToken )
    url = 'https://cloud.neocortix.com/cloud-api/sc/jobs/' + jobId
    logger.info( 'deleting instances for job %s', jobId )
    try:
        resp = requests.delete( url, headers=headers )
    except Exception as exc:
        wouldRetry = True
        logger.warning( 'got exception (%s) %s', type(exc), exc )
    else:
        if (resp.status_code < 200) or (resp.status_code >= 300):
            logger.warn( 'response code %s', resp.status_code )
            wouldRetry = resp.status_code in range( 500, 600 )  # 5xx responses are server errors
        else:
            wouldRetry = False
    if wouldRetry and maxRetries > 0:
        time.sleep( 10 )
        logger.info( 'retrying %s (up to %d retries)', jobId, maxRetries )
        return terminateJobInstances( authToken, jobId, maxRetries-1 )
    elif wouldRetry:
        # giving up
        logger.error( 'could not succeed within maximum retries' )
        return 503  # "service unavailable", but maybe should be different if gotException
    return resp.status_code

def doCmdLaunch( args ):
    authToken = args.authToken

    instanceAbis = []
    instanceType = args.itype  # armeabi-v7a
    if instanceType:
        logger.info( 'requested instance type "%s" (might work)', instanceType)
        instanceAbis = [instanceType]

    instances = []
    try:
        infos = launchScInstancesAsync( authToken, args.encryptFiles, args.count,
            sshClientKeyName=args.sshClientKeyName,
            regions=args.region, abis=instanceAbis, jsonFilter=args.filter,
            jobId=args.jobId, okToContinueFunc=sigtermNotSignaled )
        if 'serverError' in infos:
            logger.error( 'got serverError %d', infos['serverError'])
            if args.json:
                print( '[]')
            return infos['serverError']
    except Exception as exc:
        logger.error( 'exception launching instances (%s) "%s"',
            type(exc), exc, exc_info=True )
        return 13  # error 13
    # regions=['russia-ukraine-belarus']  abis=['arm64-v8a']
    for info in infos:
        instances.append( info )

    # collect the ID of the created instances
    iids = []
    for inst in instances:
        iids.append( inst['id'] )
        #logger.debug( 'created instance %s', inst['id'] )
    logger.info( 'allocated %d instances', len(iids) )

    reqParams = {"show-device-info":True}
    startedInstances = {}
    # wait while instances are still starting, but with a timeout
    timeLimit = 600 # seconds
    deadline = time.time() + timeLimit
    startedSet = set()
    failedSet = set()
    try:
        while True:
            starting = False
            launcherStates = collections.Counter()
            for iid in iids:
                if sigtermSignaled():
                    logger.warning( 'incomplete launch due to sigterm' )
                    break
                if iid in startedSet:
                    continue
                if iid in failedSet:
                    continue
                try:
                    details = queryNcsSc( 'instances/%s' % iid, authToken, reqParams )['content']
                except Exception as exc:
                    logger.warning( 'exception checking instance state (%s) "%s"',
                        type(exc), exc )
                    continue
                if 'state' in details:
                    iState = details['state']
                else:
                    iState = '<unknown>'
                    logger.warning( 'no "state" in content of response (%s)', details )
                launcherStates[ iState ] += 1
                if iState == 'started':
                    startedSet.add( iid )
                    startedInstances[ iid ] = details
                if iState in ['exhausted', 'ise', 'timedout']:
                    failedSet.add( iid )
                    logger.warning( 'instance state %s for %s', iState, iid )
                if iState == 'initial':
                    logger.debug( '%s %s', iState, iid )
                #if iState in ['initial', 'starting']:
                if iState != 'started':
                    starting = True
                    #logger.debug( '%s %s', iState, iid )
            logger.info( '%d instance(s) launched so far; %s',
                len( startedSet ), launcherStates )
            if not starting:
                break
            if time.time() > deadline:
                logger.warning( 'took too long for some instances to start' )
                break
            if sigtermSignaled():
                logger.warning( 'incomplete launch due to sigterm' )
                break
            time.sleep( 10 )
    except KeyboardInterrupt:
        logger.info( 'caught SIGINT (ctrl-c), skipping ahead' )

    #nStillStarting = len(iids) - (len(startedSet) + len(failedSet))
    logger.info( 'started %d Instances; %s',
        len(startedSet), launcherStates )

    logger.info( 'querying for device-info')
    # print details of created instances to stdout
    if args.json:
        print( '[')
        jsonFirstElem=True
    for iid in iids:
        try:
            #reqParams = {"show-device-info":True}
            if iid in startedInstances:
                #logger.debug( 'reusing instance info')
                details = startedInstances[iid]
            else:
                logger.info( 're-querying instance info for %s', iid )
                details = queryNcsSc( 'instances/%s' % iid, authToken, reqParams )['content']
        except Exception as exc:
            logger.error( 'exception getting instance details (%s) "%s"',
                type(exc), exc )
            continue
        except KeyboardInterrupt:
            logger.info( 'caught SIGINT (ctrl-c), skipping ahead' )
            break
        #logger.debug( 'NCSC Inst details %s', details )
        if args.json:
            outRec = details.copy()
            outRec['instanceId'] = iid
            if jsonFirstElem:
                jsonFirstElem = False
            else:
                print( ',', end=' ')
            print( json.dumps( outRec ) )
        else:
            print( "%s,%s,%s" % (iid, iState, details['job']) )
        if g_.signaled:
            break
    if args.json:
        print( ']')
    logger.debug( 'finished')
    return 0 # no err

def listNcsScInstances( authToken ):
    try:
        response = queryNcsSc( 'instances', authToken)
    except Exception as exc:
        logger.error( 'exception getting list of instances (%s) "%s"',
            type(exc), exc )
        raise
    instancesJson = response['content']
    logger.debug( 'response %s', instancesJson )
    if 'running' in instancesJson:
        runningInstances = instancesJson['my'] # 'running'
    else:
        runningInstances = []
    logger.info( 'found %d allocated instances', len( runningInstances ) )
    iids = [inst['id'] for inst in runningInstances]
    return iids

def doCmdList( args ):
    authToken = args.authToken

    if args.instanceId and (args.instanceId != ['ALL']):
        iids = args.instanceId
    else:
        try:
            response = queryNcsSc( 'instances', authToken)
        except Exception as exc:
            logger.error( 'exception getting list of instances (%s) "%s"',
                type(exc), exc )
            return
        instancesJson = response['content']
        logger.debug( 'response %s', instancesJson )
        if 'running' in instancesJson:
            runningInstances = instancesJson['my'] # 'running'
        else:
            runningInstances = []
        logger.info( 'found %d allocated instances', len( runningInstances ) )
        iids = [inst['id'] for inst in runningInstances]

    if args.json:
        print( '[')
        jsonFirstElem=True
    for iid in iids:
        try:
            reqParams = {"show-device-info":True}
            response = queryNcsSc( 'instances/%s' % iid, authToken, reqParams )
        except Exception as exc:
            logger.error( 'exception getting instance details (%s) "%s"',
                type(exc), exc )
            continue
        respCode = response['statusCode']
        if (respCode < 200) or (respCode >= 300):
            logger.warning( 'instanceId %s not found', iid)
            continue
        details = response['content']
        instState = details['state']
        #logger.info( 'NCSC Inst details %s', details )
        if 'app-version' in details:
            logger.info( 'iid: %s version: %s', iid, details['app-version']['code'] )
        #if 'ram' in details:
        #    logger.info( 'ram %.1f M (tot); storage %.1f M (free); cores %d', details['ram']['total']/1000000,
        #        details['storage']['free']/1000000, len( details['cpu']['cores'] ) )
        #else:
        #    logger.warning( 'no "ram" listed for inst %s (which was %s)', iid, details['state']  )
        #if 'events' in details:
        #    logger.info( 'state: %s, events: %s', instState, details['events'] )                
        #else:
        #    logger.warning( 'no "events" listed for inst %s (which was %s)', iid, details['state']  )
        if 'failure' in details:
            logger.warning( 'failure: %s', details['failure'] )                

        if 'progress' in details:
            if instState != 'started' or 'SC instance launched' not in details['progress']:
                logger.warning( '"progress": %s', details['progress'] )                
        #else:
        #    logger.warning( 'no "progress" listed for inst %s (which was %s)', iid, details['state']  )

        if args.json:
            outRec = details.copy()
            outRec['instanceId'] = iid
            if (not args.showPasswords) and ('ssh' in outRec):
                outRec['ssh']['password'] = '*'
            if jsonFirstElem:
                jsonFirstElem = False
            else:
                print( ',', end=' ')
            print( json.dumps( outRec ) )
        else:
            port = details['ssh']['port'] if 'ssh' in details else 0
            host = details['ssh']['host'] if 'ssh' in details else 'None'
            pw = details['ssh']['password'] if 'ssh' in details else ''
            jobId = details['job']
            if not args.showPasswords:
                pw = '*'
            print( '%s,%s,%d,%s,%s,%s' % ( iid, details['state'], port, host, pw, jobId ) )
            #print( '%s,"%s",%s,%d,%s,%s' % ( iid, inst['name'], details['state'], port, host, pw ) )
            #print( iid, inst['name'], details['state'], port, host, sep=',' )
    if args.json:
        print( ']')

def terminateInstances( authToken, instanceIds ):
    def terminateOne( iid ):
        logger.debug( 'terminating %s', iid )
        terminateNcscInstance( authToken, iid )
    if instanceIds and (len(instanceIds) >0) and (isinstance(instanceIds[0], str )):
        nWorkers = 4
        with futures.ThreadPoolExecutor( max_workers=nWorkers ) as executor:
            parIter = executor.map( terminateOne, instanceIds )
            parResultList = list( parIter )
    
def doCmdTerminate( args ):
    authToken = args.authToken

    startTime = time.time()
    threading = True
    if args.jobId and args.instanceId:
        logger.error( 'combining instance id with job id is not supported for terminate' )
    elif args.jobId:
        logger.info( 'terminating instances for job %s', args.jobId )
        terminateJobInstances( authToken, args.jobId )
    elif args.instanceId == ['ALL']:
        try:
            response = queryNcsSc( 'instances', authToken)
        except Exception as exc:
            logger.error( 'exception getting list of instances (%s) "%s"',
                type(exc), exc )
            return
        logger.info( 'response content %s', response['content'].keys() )
        instancesJson, respCode = (response['content'], response['statusCode'] )
        if (respCode < 200) or (respCode >= 300):
            logger.error( 'could not terminate instances')
            return

        runningInstances = instancesJson['my']  # 'running'
        #logger.info( 'runningInstances %s', runningInstances )
        logger.info( 'found %d running instances', len( runningInstances ) )
        if threading:
            runningIids = [inst['id'] for inst in runningInstances]
            #logger.info( 'runningIids %s', runningIids )
            terminateInstances( authToken, runningIids )
        else:
            for inst in runningInstances:
                iid = inst['id']
                logger.info( 'terminating %s "%s"', iid, inst['name'] )
                terminateNcscInstance( authToken, iid )
    elif not args.instanceId:
        logger.error( 'no instance ID provided for terminate' )
    else:
        if threading:
            terminateInstances( authToken, args.instanceId )
        else:
            for iid in args.instanceId:
                logger.info( 'terminating %s', iid )
                terminateNcscInstance( authToken, iid )
    logger.info( 'took %.1f seconds', time.time() - startTime)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@',
        #formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    ap.add_argument( 'subcommand', help='subcommand (only "sc" is allowed)' )
    ap.add_argument( 'action', help='the action to perform', 
        choices=['launch', 'list', 'terminate']
        )
    ap.add_argument('--version', action='version', version=__version__)
    #ap.add_argument('--verbose', '-v', action='count', default=0)
    ap.add_argument( '--count', type=int, default=1, help='the number of instances required (default=1)' )
    ap.add_argument( '--instanceId', type=str, nargs='+', help='one or more instance IDs (or ALL to terminate all)' )
    ap.add_argument( '--encryptFiles', type=boolArg, default=None, help='whether to encrypt files on launched instances' )
    ap.add_argument( '--filter', help='json to filter instances for launch' )
    ap.add_argument( '--json', action='store_true', help='for json-format output' )
    ap.add_argument( '--jobId', help='unique job id for launch or terminate' )
    ap.add_argument( '--region', nargs='+', help='the geographic region(s) to target' )
    ap.add_argument( '--showPasswords', action='store_true', help='if you want launch or list to show passwords' )
    ap.add_argument( '--sshClientKeyName', help='the name of the uploaded ssh client key to use' )
    ap.add_argument( '--itype', default=None, help='the instance type to create' )
    ap.add_argument( '--authToken', type=str, default=None,
        help='the NCS authorization token to use' )
    args = ap.parse_args()
    #logger.info( 'args %s', args ) # be careful not to leak authToken
    
    logger.debug( 'setting SIGTERM handler' )
    signal.signal( signal.SIGTERM, sigtermHandler )

    if args.authToken == None:
        tok = os.getenv( 'NCS_AUTH_TOKEN' )
        if tok:
            args.authToken = tok
        else:
            sys.exit( 'no authToken found' )

    if args.subcommand != 'sc':
        sys.exit( 'sc is the only available subcommand')

    if args.action == 'launch':
        if args.encryptFiles == None:
            sys.exit( os.path.basename(sys.argv[0]) + ': error: no encryptFiles arg passed for launch' )
        exitCode = doCmdLaunch( args )
        if exitCode > 400:
            exitCode -= 400
        sys.exit( exitCode )
    elif args.action == 'list':
        doCmdList( args )
    elif args.action == 'terminate':
        doCmdTerminate( args )
    else:
        sys.exit( 'unrecognized action %s' % (args.action) )
       
