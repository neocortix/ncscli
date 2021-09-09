#!/usr/bin/env python3
"""
start ssh port-forwarding processes for NCS workers
"""

# standard library modules
import argparse
import json
import logging
import subprocess


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# possible place for globals is this class's attributes
class g_:
    serverAliveInterval = 30
    serverAliveCountMax = 6

def startForwarders( agentInstances, forwarderHost='localhost',
    portRangeStart=7100, maxPort = 7199,
    portMap=None,
    forwardingCsvFilePath = 'agentForwarding.csv'
    ):
    forwarders = []
    mappings = []
    with open( forwardingCsvFilePath, 'w' ) as csvOutFile:
        print( 'forwarding', 'instanceId', 'instHost', 'instSshPort', 'assignedPort',
            sep=',', file=csvOutFile
            )
        assignedPort = portRangeStart
        for inst in agentInstances:
            iid = inst['instanceId']
            if portMap:
                assignedPort = portMap[ iid ]
                logger.debug( 'assigning mapped port %d', assignedPort )
            else:
                logger.info( 'assigning incremental port %d', assignedPort )

            if assignedPort > maxPort:
                logger.warning( 'port number exceeded maxPort (%d vs %d)', assignedPort, maxPort )
                break
            iidAbbrev = iid[0:8]
            sshSpecs = inst['ssh']
            instHost = sshSpecs['host']
            instPort = sshSpecs['port']
            user = sshSpecs['user']
            logger.info( '%d ->%s %s@%s:%s', assignedPort, iidAbbrev, user, instHost, instPort )
            cmd = ['ssh', '-fNT', '-o', 'ExitOnForwardFailure=yes', '-p', str(instPort),
                '-o', 'ServerAliveInterval=%d' % g_.serverAliveInterval,
                '-o', 'ServerAliveCountMax=%d' % g_.serverAliveCountMax,
                '-L', '*:'+str(assignedPort)+':localhost:'+str(assignedPort), 
                '%s@%s' % (user, instHost)
            ]
            logger.debug( 'cmd: %s', cmd )
            logLevel = logger.getEffectiveLevel()
            # will force ssh process to be quiet unless we are in a debug-like logLevel
            stderr = None if logLevel < logging.INFO else subprocess.DEVNULL

            rc = subprocess.call( cmd, shell=False,
                stdin=subprocess.DEVNULL, stderr=stderr,
                )
            if rc:
                logger.warning( 'could not forward to %s (rc %d)', iid, rc )
            else:
                mapping = '%s:%d' % (forwarderHost, assignedPort)
                mappings.append( mapping )
                forwarder = {
                    'instanceId': iid, 'host': instHost,
                    'port': assignedPort, 'mapping': mapping
                }
                forwarders.append( forwarder )
                print( mapping, iid, instHost, instPort, assignedPort,
                    sep=',', file=csvOutFile
                    )
            assignedPort += 1
    logger.info( 'forwarding ports for %d agents', len(mappings) )
    if mappings:
        print( 'forwarding:', ', '.join(mappings) )
    return forwarders


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'agentsFilePath', help='input file path of json instance descriptions' )
    ap.add_argument( '--forwarderHost', help='IP addr (or host name) of the forwarder host',
        default='localhost' )
    ap.add_argument( '--portRangeStart', type=int, help='first port number to forward',
        default=7100 )
    ap.add_argument( '--maxPort', type=int, help='maximum port number to forward',
        default=7199 )
    ap.add_argument( '--forwardingCsvFilePath', help='output CSV file for later reference',
        default='agentForwarding.csv' )
    args = ap.parse_args()

    inFilePath = args.agentsFilePath
    with open( inFilePath ) as inFile:
        instances = json.load( inFile )
        if instances:
            logger.info( 'read %d instances from %s', len(instances), inFilePath )
            startForwarders( instances, forwarderHost=args.forwarderHost,
                portRangeStart=args.portRangeStart, maxPort=args.maxPort,
                forwardingCsvFilePath=args.forwardingCsvFilePath
                )
