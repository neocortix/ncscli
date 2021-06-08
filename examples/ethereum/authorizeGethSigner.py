#!/usr/bin/env python3
"""
authorize or deauthorize a given geth signing node
"""

# standard library modules
import argparse
from concurrent import futures
import datetime
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
# third-party modules
import psutil
import requests
# neocortix modules
import ncscli.ncs as ncs
import ncscli.tellInstances as tellInstances
import ncsgeth  # assumed to be in the same directory as this script


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class g_:
    signaled = False
    interrupted = False

def sigtermSignaled():
    return False

def boolArg( v ):
    '''use with ArgumentParser add_argument for (case-insensitive) boolean arg'''
    if v.lower() == 'true':
        return True
    elif v.lower() == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parseLogLevel( arg ):
    '''return a logging level (int) for the given case-insensitive level name'''
    arg = arg.lower()
    map = { 
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
        }
    if arg not in map:
        logger.warning( 'the given logLevel "%s" is not recognized (using "info" level, instead)', arg )
    setting = map.get( arg, logging.INFO )

    return setting

def findAuthorizers( instances, savedSigners, badIids ):
    '''return subset of instances capable of voting on authorization'''
    authorizers = []
    #for inst in (anchorInstances + liveInstances):
    for inst in instances:
        iid = inst['instanceId']
        if (iid in savedSigners) and (iid not in badIids):
            if 'host' not in inst['ssh']:
                logger.warning( 'no host for authorizer %s', iid )
            else:
                authorizers.append( inst )
                logger.info( 'including authorizer %s', iid )
    return authorizers


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--dataDirPath', required=True, help='the path to the directory for input and output data' )
    ap.add_argument( '--auth', type=boolArg, required=True, help='true to authorize, false to deauthorize' )
    ap.add_argument( '--instanceId', required=False, help='id of the instance to auth or deauth' )
    ap.add_argument( '--configName', required=True, help='the name of the geth configuration' )
    ap.add_argument( '--account', help='the account to auth or deatuh (default is determined by instance' )
    ap.add_argument( '--invFile', help='the path an ansible inventory file in json form' )
    #ap.add_argument( '--ncsInstances', help='the path an ncs instances file' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    args = ap.parse_args()

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    tellInstances.logger.setLevel( logLevel )
    logger.debug('the logger is configured')

    invFilePath = args.invFile
    if not invFilePath:
        logger.error( 'no --invFile was given' )
        sys.exit( 1 )

    shouldAuth = args.auth
    victimIid = args.instanceId
    if not victimIid:
        logger.info( 'no --instanceId was given' )
        #sys.exit( 1 )

    if not victimIid and not args.account:
        logger.error( 'neither --instanceId nor --account was given' )
        sys.exit( 1 )
    if shouldAuth and not victimIid:
        logger.error( 'not willing to authorize one without an iid' )
        sys.exit( 1 )

    configName = args.configName

    savedSignersFilePath = os.path.join( args.dataDirPath, 'savedSigners.json' )

    # get details of launched instances from the json file
    if not os.path.isfile( invFilePath ):
        logger.error( 'file "%s"', invFilePath )
        sys.exit( 1 )
    inventory = None
    with open( invFilePath, 'r') as jsonInFile:
        try:
            inventory = json.load(jsonInFile)  # a dict containing "all" containing "hosts"
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
    instances = []
    logger.debug( 'inventory: %s', inventory)
    if not 'all' in inventory:
        logger.warning( 'no "all" in inventory')
    else:
        all = inventory['all']
        hosts = all['hosts']
        for (name, val) in hosts.items():
            port = val.get( 'ansible_ssh_host', 22 )
            user = val.get('ansible_user')
            host = val.get('ansible_host')
            #logger.info( '%s: %s @ %s : %d', name, user, host, port )
            sshSpecs = { 'user': user, 'host': host, 'port': port }
            #logger.info( 'sshSpecs: %s', sshSpecs )
            inst = {'instanceId': name, 'ssh': sshSpecs }
            instances.append( inst )
    logger.info( 'inventory instances: %s', instances)

    launchedInstancesFilePath = args.dataDirPath + '/recruitLaunched.json'
    if not os.path.isfile( invFilePath ):
        logger.error( 'no filefile "%s"', invFilePath )
        sys.exit( 1 )
    launchedInstances = []
    with open( launchedInstancesFilePath, 'r') as jsonInFile:
        try:
            launchedInstances = json.load(jsonInFile)  # an array
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )

    liveNodesFilePath = os.path.join( args.dataDirPath, 'liveNodes.json' )
    ncsInstances = []
    if not os.path.isfile( liveNodesFilePath ):
        logger.warning( 'did not find "%s"', liveNodesFilePath )
    else:
        with open( liveNodesFilePath, 'r') as jsonInFile:
            try:
                ncsInstances = json.load(jsonInFile)  # an array
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        if ncsInstances:
           instances.extend( ncsInstances )
    allIids = [inst['instanceId'] for inst in instances]
    if (victimIid != 'ALL') and  (victimIid not in allIids):
        logger.warning( '%s was not found in the given set of instances', victimIid )
        #sys.exit(1)
    if (victimIid == 'ALL') and  (shouldAuth != False ):
        logger.error( 'not willing to authorize all at once' )
        sys.exit(1)


    if instances:
        instancesByIid = {inst['instanceId']: inst for inst in instances }
        # get proposals from each instance
        propsFound = ncsgeth.collectProposals( instances, configName )
        propSummary = propsFound['summary']
        proposees = set( propSummary.keys() )
        logger.info( 'proposees: %s', proposees )

        # get authorized signers from each instance
        authSigners = ncsgeth.collectAuthSigners( instances, configName )
        logger.info( '%d authSigners: %s', len(authSigners), authSigners )

        allSigners = proposees.union( authSigners )
        logger.info( '%d allSigners: %s', len(allSigners), allSigners )

        nonauth = proposees - authSigners
        logger.info( '%d unauth: %s', len(nonauth), nonauth )

        # get primary account from each instance
        instanceAccountPairs = ncsgeth.collectPrimaryAccounts( instances, configName )
        instancesByAccount = {pair['accountAddr']: instancesByIid[pair['instanceId']]
            for pair in instanceAccountPairs if pair.get( 'accountAddr' )
            }
        accountsByIid = {pair['instanceId']: pair['accountAddr'] for pair in instanceAccountPairs }
        logger.info( '%d instanceAccountPairs: %s', len(instanceAccountPairs), instanceAccountPairs)
        #logger.info( 'instancesByAccount: %s', instancesByAccount )

        if victimIid != 'ALL':
            victimAccount = args.account or accountsByIid.get( victimIid )
            authStr = 'authorize' if shouldAuth else 'deauthorize'
            if not victimAccount:
                logger.error( 'can not %s account %s of inst %s', authStr, victimAccount, victimIid[0:16] )
                sys.exit(1)
            if victimIid:
                # start or stop miner
                if shouldAuth :
                    logger.info( 'will start miner on inst %s', victimIid[0:16] )
                    victimInst = instancesByIid[victimIid]
                    ncsgeth.tellNodes( [victimInst], configName, '"miner.start(1)"' )
                else:
                    victimInst = instancesByIid.get(victimIid)
                    if victimInst:
                        logger.info( 'stopping miner on inst %s', victimIid[0:16] )
                        ncsgeth.stopMiners( [victimInst], configName, )
            logger.info( 'will %s account %s of inst %s', authStr, victimAccount, victimIid )
            # execute upvote/downvote on each instance
            # maybe should do this only on (proposed or authorized) signers
            results = ncsgeth.authorizeSigner( instances, configName, victimAccount, shouldAuth )
            logger.debug( 'results: %s', results )
            
            curSigners = ncsgeth.collectAuthSigners( instances, configName )
            nowAuth = victimAccount in curSigners
            logger.info( 'now authorized? %s', nowAuth )
            timeLimit = 15 * 60
            deadline = time.time() + timeLimit
            while nowAuth != shouldAuth:
                if time.time() >= deadline:
                    logger.warning( 'took too long for auth/deauth to propagate')
                    break
                curSigners = ncsgeth.collectAuthSigners( instances, configName )
                nowAuth = victimAccount in curSigners
                logger.info( 'now authorized? %s', nowAuth )
        else:
            logger.info( 'want to deauth ALL')
            # load saved signers
            savedSigners = {}
            if os.path.isfile( savedSignersFilePath ):
                with open( savedSignersFilePath, 'r') as jsonInFile:
                    try:
                        savedSigners = json.load(jsonInFile) # a dict of lists, indexed by iid
                    except Exception as exc:
                        logger.warning( 'could not load savedSigners json (%s) %s', type(exc), exc )
            logger.info( '%d savedSigners: %s', len( savedSigners), savedSigners )
            '''
            signerInfos = ncsgeth.collectSignerInstances( anchorInstances+goodInstances, args.configName )  # a list
            logger.info( '%d signerInfos: %s', len( signerInfos), signerInfos )
            signersByIid = {signer['instanceId']: signer for signer in signerInfos }
            # conversion coide to prioduce legacy savedSigners
            savedSigners = {}
            for signerInfo in signerInfos:
                savedSigners[ signerInfo['instanceId'] ] = [signerInfo['accountAddr']]
            logger.info( '%d savedSigners: %s', len( savedSigners), savedSigners )
            if not savedSigners:
                logger.info( 'there are NO signers' )
            '''


            authorizers = findAuthorizers( instances, savedSigners, [] )
            for inst in launchedInstances:
                iid = inst['instanceId']
                wasSigner = iid in savedSigners
                logger.info( 'instance %s saved signer? %s ', iid[0:16], wasSigner)
                #logger.info( 'saved signer? %s', wasSigner )
                if wasSigner:
                    # victim is first account in savedSigners list for this instance
                    victimAccount = savedSigners[iid][0]
                    logger.info( 'deauthorizing %s account %s', iid[0:16], victimAccount )
                    results = ncsgeth.authorizeSigner( authorizers, configName, victimAccount, False )
                    logger.info( 'authorizeSigner returned: %s', results )

                    curSigners = ncsgeth.collectAuthSigners( authorizers, configName )
                    nowAuth = victimAccount in curSigners
                    logger.info( 'now authorized? %s', nowAuth )
                    timeLimit = 5 * 60
                    deadline = time.time() + timeLimit
                    while nowAuth != shouldAuth:
                        if time.time() >= deadline:
                            logger.warning( 'took too long for auth/deauth to propagate')
                            break
                        curSigners = ncsgeth.collectAuthSigners( authorizers, configName )
                        nowAuth = victimAccount in curSigners
                        logger.info( 'now authorized? %s', nowAuth )
                    logger.info( 'sleeping for 90 seconds' )
                    time.sleep( 90 )


        #signingIids = [instancesByAccount[acct]['instanceId'] for acct in allSigners]
        #logger.info( 'signingIids: %s', signingIids )
        #signingInsts = [instancesByAccount.get(acct) for acct in allSigners]

        

    '''
    with open( dataDirPath + '/liveNodes.json','w' ) as outFile:
        json.dump( stillLive, outFile, indent=2 )
    '''
    logger.info( 'finished' )
