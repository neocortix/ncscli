#!/usr/bin/env python3
"""
functions for working with geth nodes via ssh
"""

# standard library modules
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
#import psutil
#import requests
# neocortix modules
#import ncscli.ncs as ncs
#import ncscli.tellInstances as tellInstances


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class g_:
    signaled = False
    interrupted = False

def sigtermSignaled():
    return False


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


def executeCmdOnInstance( cmd, inst, timeLimit=60 ):
    iid = inst['instanceId']
    abbrevIid = iid[0:16]
    logger.debug( 'starting %s', abbrevIid )
    logLevel = logger.getEffectiveLevel()

    stdout = ''
    returnCode = None

    def trackStderr( proc ):
        for line in proc.stderr:
            if (logLevel <= logging.INFO) and line.strip():
                print( '<stderr>', abbrevIid, line.strip(), file=sys.stderr )
            #logStderr( line.rstrip(), iid )

    def trackStdout( proc ):
        nonlocal stdout
        for line in proc.stdout:
            stdout += line
            if (logLevel <= logging.DEBUG) and line.strip():
                print( '<stdout>', abbrevIid, line.strip(), file=sys.stderr )
    if True:
        if cmd:
            sshSpecs = inst['ssh']
            logger.debug( 'commanding %s to %s', sshSpecs['host'], cmd )

            with subprocess.Popen(['ssh', # '-t',
                                '-p', str(sshSpecs['port']),
                                '-o', 'ServerAliveInterval=360',
                                '-o', 'ServerAliveCountMax=3',
                                sshSpecs['user'] + '@' + sshSpecs['host'], cmd],
                                encoding='utf8',
                                stdout=subprocess.PIPE,  # subprocess.PIPE subprocess.DEVNULL
                                stderr=subprocess.PIPE) as proc:
                startTime = time.time()
                deadline = startTime + timeLimit
                stdoutThr = threading.Thread(target=trackStdout, args=(proc,))
                stdoutThr.start()
                stderrThr = threading.Thread(target=trackStderr, args=(proc,))
                stderrThr.start()
                while time.time() < deadline:
                    proc.poll() # sets proc.returncode
                    if proc.returncode == None:
                        if time.time() > startTime + 30:
                            logger.info( 'polling %s', abbrevIid )
                            rightNow = datetime.datetime.now(datetime.timezone.utc)
                    else:
                        if proc.returncode == 0:
                            logger.debug( 'rc zero on %s', abbrevIid )
                            curFrameRendered = True
                        else:
                            logger.warning( 'instance %s gave returnCode %d', abbrevIid, proc.returncode )
                        break
                    if sigtermSignaled():
                        break
                    if g_.interrupted:
                        logger.info( 'exiting polling loop because interrupted' )
                        break
                    time.sleep(1)
                returnCode = proc.returncode if proc.returncode != None else 124
                if returnCode:
                    #logger.warning( 'cmd failed with rc %d for on %s', returnCode, iid )
                    time.sleep(1) # maybe we should retire this instance; at least, making it sleep so it is less competitive
                else:
                    logger.debug( 'finished %s', iid )
                    #g_.framesFinished.append( frameNum )  # too soon

                proc.terminate()
                try:
                    proc.wait(timeout=5)
                    if proc.returncode:
                        logger.debug( 'ssh return code %d', proc.returncode )
                except subprocess.TimeoutExpired:
                    logger.warning( 'ssh did not terminate in time' )
                stdoutThr.join()
                stderrThr.join()
        #if returnCode:
    return {'returnCode': returnCode, 'stdout': stdout}

def tellNodes( instances, configName, cmd ):
    '''tell each instance to execute a geth command, in parallel'''
    cmd = ('geth attach ether/%s/geth.ipc --exec ' % configName ) + cmd
    nInstances = len( instances )
    with futures.ThreadPoolExecutor( max_workers=nInstances ) as executor:
        parIter = executor.map( executeCmdOnInstance, [cmd]*nInstances, instances )
        results = list( parIter )
    return results

def authorizeSigner( instances, configName, victimAccount, shouldAuth ):
    '''execute upvote/downvote for each instance to authorize or deauthorize a signer account'''
    logger.info( 'configName: %s, account: %s, shouldAuth: %s', configName, victimAccount, shouldAuth )
    if not isinstance( victimAccount, str ):
        logger.warning( 'given victimAccount is not a string (%s)', victimAccount )
    jsBoolStr = 'true' if shouldAuth else 'false'
    cmd = '"clique.propose(\'%s\',%s)"' % (victimAccount, jsBoolStr )
    logger.info('authCmd: %s', cmd )
    logger.info('telling %d nodes', len(instances) )
    results = tellNodes( instances, configName, cmd )
    return results

def collectProposals( instances, configName ):
    '''get proposed (or downvoted) signers from each instance'''
    propSummary = {}
    if instances:
        # get proposals from each instance
        cmd = 'clique.proposals'
        results = tellNodes( instances, configName, cmd )
        for result in results:
            if result['returnCode']:
                continue
            stdout = result['stdout']
            # fix the geth output to make it legal json
            cleaned = re.sub( r'(0x[^:]*):', r'"\g<1>":', stdout )
            # the json contains a dict of boolean values (for up/down vote) indexed by account addr
            props = json.loads( cleaned )
            logger.info( 'props: %s', props )
            for account in props:
                # OR this value into the cumulative dict
                propSummary[account] = propSummary.get( account, False ) or props[account]
    return {'summary': propSummary }

def collectAuthSigners( instances, configName ):
    '''get authorized signers from each instance'''
    signers = []
    if instances:
        # get authorized signers from each instance
        cmd = '"clique.getSigners()"'
        results = tellNodes( instances, configName, cmd )
        for result in results:
            if result['returnCode'] != 0:
                continue
            stdout = result['stdout']
            sigs = json.loads( stdout )
            logger.debug( 'sigs: %s', sigs )
            signers.extend( sigs )
    return set( signers )

def collectPrimaryAccounts( instances, configName ):
    '''get primary account from each instance'''
    if not instances:
        return []
    instanceAccountPairs = []
    cmd = 'eth.accounts'
    results = tellNodes( instances, configName, cmd )
    logger.debug( 'results: %s', results )
    for ii, result in enumerate( results ):
        inst = instances[ii]
        iid = inst['instanceId']
        abbrevIid = iid[0:16]
        if result['returnCode'] != 0:
            #logger.warning( 'got non-zero returnCode %s', result )
            continue
        stdout = result['stdout']
        accts = json.loads( stdout )
        if len( accts ) != 1:
            logger.warning( 'instance %s has %d accounts', abbrevIid, len( accts ) )
        if not accts:
            continue
        account = accts[0]
        logger.info( '%s account: %s', abbrevIid, account )
        instanceAccountPairs.append( {'instanceId': iid, 'accountAddr': account })
    return instanceAccountPairs

def loadAnsibleInstances( invFilePath ):
    '''load instances from a json-style ansible inventory file'''
    instances = []
    if not os.path.isfile( invFilePath ):
        return instances
    inventory = None
    with open( invFilePath, 'r') as jsonInFile:
        try:
            inventory = json.load(jsonInFile)  # a dict containing "all" containing "hosts"
        except Exception as exc:
            logger.warning( 'could not load json (%s) %s', type(exc), exc )
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
    return instances

def loadInstances( jsonInFilePath ):
    '''load instances from a json-style ncs instances file or ansible inventory'''
    instances = []
    if jsonInFilePath:
        decoded = None
        with open( jsonInFilePath, 'r') as jsonInFile:
            try:
                decoded = json.load(jsonInFile)  # an array for ncs or a dict for ansible
            except Exception as exc:
                logger.warning( 'could not load json (%s) %s', type(exc), exc )
        if decoded:
            if isinstance( decoded, dict ) and 'all' in decoded:
               logger.info( 'loading ansible inventory %s', jsonInFilePath )
               instances = loadAnsibleInstances( jsonInFilePath )
            else:
                logger.info( 'decoded type "%s"', type( decoded ) )
                instances = decoded
    return instances
