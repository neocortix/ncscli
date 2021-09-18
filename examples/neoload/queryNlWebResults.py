#!/usr/bin/env python3
"""
check instances with installed LG agents; terminate some that appear unusable
"""

# standard library modules
import argparse
#import datetime
#import json
import logging
import os
#import subprocess
import sys
# third-party modules
#import psutil
import requests
# neocortix modules
#import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def queryNlWeb( nlWebUrl, nlWebToken, urlTail ):
    headers = {  "Accept": "application/json", "accountToken": nlWebToken }
    apiUrl = os.path.join( nlWebUrl, 'v3' )

    url = os.path.join( apiUrl, urlTail )

    logger.debug( 'querying: %s', url )
    # set long timeouts for requests.get() as a tuple (connection timeout, read timeout) in seconds
    timeouts = (30, 120)
    try:
        resp = requests.get( url, headers=headers, timeout=timeouts )
    except requests.ConnectionError as exc:
        logger.warning( 'ConnectionError exception (%s) %s', type(exc), exc )
        return None
    except Exception as exc:
        logger.warning( 'Exception (%s) %s', type(exc), exc )
        return None

    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
        return None
    return resp.json()

def queryNlWebForResources( nlWebUrl, nlWebToken ):
    headers = {  "Accept": "application/json", "accountToken": nlWebToken }
    url = nlWebUrl+'/v3/resources/zones'

    logger.info( 'querying: %s', nlWebUrl )
    # set long timeouts for requests.get() as a tuple (connection timeout, read timeout) in seconds
    timeouts = (30, 120)
    try:
        resp = requests.get( url, headers=headers, timeout=timeouts )
    except requests.ConnectionError as exc:
        logger.warning( 'ConnectionError exception (%s) %s', type(exc), exc )
        return None
    except Exception as exc:
        logger.warning( 'Exception (%s) %s', type(exc), exc )
        return None

    if (resp.status_code < 200) or (resp.status_code >= 300):
        logger.warning( 'error code from server (%s) %s', resp.status_code, resp.text )
    else:
        nlWebZones = resp.json()
        logger.debug( 'nlWeb api zones: %s', nlWebZones )
        for zone in nlWebZones:
            logger.info( 'zone id: %s name: "%s"', zone['id'], zone['name'] )
            for controller in zone['controllers']:
                logger.info( '  Controller "%s" %s %s', controller['name'], controller['version'], controller['status'] )
            for lg in zone['loadgenerators']:
                logger.debug( '  LG "%s" %s %s', lg['name'], lg['version'], lg['status'] )
            logger.info( '  %d LGs listed by nlweb in Zone %s', len(zone['loadgenerators']), zone['id'] )
            logger.info( '  %d controllers listed by nlweb in Zone %s', len(zone['controllers']), zone['id'] )


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    #ap.add_argument( '--dataDirPath', help='the path to the directory for input and output data' )
    #ap.add_argument( '--neoloadVersion', default ='7.10', help='version of neoload to check for' )
    ap.add_argument( '--nlWebUrl', help='the URL of a neoload web server to query' )
    ap.add_argument( '--nlWebToken', help='a token for authorized access to a neoload web server' )
    ap.add_argument( '--workspaceId', help='the workspace to query' )
    ap.add_argument( '--resultId', help='the result ID to query' )
    #ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    args = ap.parse_args()

    #logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    # make sure all the necessary nlWeb args were passed in non-empty
    if not args.nlWebToken:
        logger.error( 'please pass a non-empty --nlWebToken')
    if not args.nlWebUrl:
        logger.error( 'please pass a non-empty --nlWebUrl')
    if not (args.nlWebUrl and args.nlWebUrl):
        sys.exit( 1 )
    
    nlWebToken = args.nlWebToken
    nlWebUrl = args.nlWebUrl
    if nlWebUrl == 'SAAS':
        nlWebUrl = 'https://neoload-api.saas.neotys.com'

    #queryNlWebForResources( nlWebUrl, args.nlWebToken )

    workspaceId = args.workspaceId
    resultId = args.resultId

    # the "monitors" api is where we get per-LG information
    monitorsUrlTail = 'workspaces/%s/test-results/%s/monitors' % (workspaceId, resultId)

    counterIdDict = {}
    counterSpecs = []
    monitorsData = queryNlWeb( nlWebUrl, nlWebToken, monitorsUrlTail )
    if not monitorsData:
        logger.warning( 'no monitors data at %s', monitorsUrlTail )
        sys.exit( 1 )
    logger.info( 'monitorsData type: %s, len: %d', type(monitorsData), len(monitorsData) )
    for monitor in monitorsData:
        logger.debug( 'monitor name: %s', monitor['name'] )
        path = monitor.get( 'path', [] )
        pathStr = '/'.join( path )
        if 'LG ' not in pathStr:
            logger.info( 'ignoring path "%s"', pathStr )
            continue
        if monitor['name'] == 'User Load':
            counterId = monitor['id']
            lgSpec = path[1]
            #print( lgSpec, counterId )
            counterSpecs.append({ 'lg': lgSpec, 'counter': counterId })
            counterIdDict[ (lgSpec, 'User Load' ) ] = counterId
        #elif monitor['name'] == 'Throughput':
        else:
            counterId = monitor['id']
            lgSpec = path[1]
            #print( lgSpec, counterId )
            #counterSpecs.append({ 'lg': lgSpec, 'counter': counterId })
            #counterIdDict[ (lgSpec, 'Throughput' ) ] = counterId
            counterIdDict[ (lgSpec, monitor['name'] ) ] = counterId
    logger.debug( 'counterSpecs: %s', counterSpecs )
    logger.debug( 'counterIdDict: %s', counterIdDict )
    logger.info( 'querying monitors for %d LGs', len( counterSpecs ) )
    for counterSpec in counterSpecs:
        counterUrlTail = os.path.join( monitorsUrlTail, counterSpec['counter'], 'values' )
        values = queryNlWeb( nlWebUrl, nlWebToken, counterUrlTail )
        #print( values )
        print( counterSpec['lg'], 'user load max:', values['max'], 'avg:', '%.2f' % values['avg'] )
        tpCounterId = counterIdDict.get( (counterSpec['lg'], 'Throughput') )
        if tpCounterId:
            #logger.info( 'tpCounterId: %s', tpCounterId )
            counterUrlTail = '/'.join([ monitorsUrlTail, tpCounterId, 'values' ])
            values = queryNlWeb( nlWebUrl, nlWebToken, counterUrlTail )
            #logger.info( 'values: %s', values )
            print( counterSpec['lg'], 'throughput max:', '%.3f' % values['max'], 'avg:', '%.3f' % values['avg'] )

    logger.info( 'finished' )
