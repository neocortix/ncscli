#!/usr/bin/env python3
"""
start ssh port-forwarding processes for NCS workers
"""

# standard library modules
import argparse
import contextlib
import json
import logging
import socket
import subprocess
# third-party modules
import psutil


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# possible place for globals is this class's attributes
class g_:
    serverAliveInterval = 30
    serverAliveCountMax = 12


# some port-reservation code adapted from https://github.com/Yelp/ephemeral-port-reserve

def preopen(ip, port):
    ''' open socket with SO_REUSEADDR and listen on it'''
    port = int(port)
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    logger.debug( 'binding ip %s port %d', ip, port )
    s.bind((ip, port))

    # the connect below deadlocks on kernel >= 4.4.0 unless this arg is greater than zero
    s.listen(1)
    return s

def preclose(s):
    sockname = s.getsockname()
    # get the port into a TIME_WAIT state
    with contextlib.closing(socket.socket()) as s2:
        s2.connect(sockname)
        s.accept()
    s.close()
    # return sockname[1]

def preopenPorts( startPort, maxPort, nPortsReq, ipAddr='0.0.0.0' ):
    results = {}
    sockets = []
    ports = []
    '''
    gotPorts = False
    while not gotPorts:
        try:
            for port in range( startPort, startPort+nPorts ):
                logger.info( 'preopening port %d', port )
                sock = preopen( ipAddr, port )
                sockets.append( sock )
            gotPorts = True
        except OSError as exc:
            logger.warning( 'got exception (%s) %s', type(exc), exc, exc_info=False )
            startPort += nPorts
            sockets = []
            if startPort >= maxPort:
                break
    if not gotPorts:
        logger.error( 'search for available ports exceeded maxPort (%d)', maxPort )
        return results
    '''
    for port in range( startPort, maxPort+1 ):
        #logger.info( 'would preopen port %d', port )
        try:
            sock = preopen( ipAddr, port )
        except OSError as exc:
            if exc.errno == 98:
                logger.info( 'port %d already in use', port )
            else:
                logger.info( 'got exception (%s) %s', type(exc), exc, exc_info=False )
        else:
            ports.append( port )
            sockets.append( sock )
        if len( ports ) >= nPortsReq:
            logger.info( 'success')
            break
    if ports:
        results['ports'] = ports
        results['sockets'] = sockets
    return results

def preclosePorts( preopened ):
    '''preclose ports pereopened by preopenPorts'''
    if not preopened: return []
    if not preopened.get('ports'): return []
    if not preopened.get('sockets'): return []
    if len( preopened['ports']) != len(preopened['sockets'] ):
        logger.warning( 'mismatched length of ports and sockets lists' )
        return []
    preclosedPorts = []
    for index, sock in enumerate( preopened['sockets'] ):
        try:
            preclose( sock )
        except Exception as exc:
            logger.warning( 'exception (%s) preclosing %s', type(exc), sock, exc_info=False )
        else:
            preclosedPorts.append( preopened['ports'][index] )
    return preclosedPorts

def findForwarders():
    mappings = []
    for proc in psutil.process_iter():
        try:
            procInfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            continue
        if 'ssh' == procInfo['name']:
            #logger.info( 'procInfo: %s', procInfo )
            cmdLine = procInfo['cmdline']
            #TODO maybe a better way to identify forwarders
            if '-fNT' in cmdLine:
                logger.debug( 'cmdLine: %s', cmdLine )
                mapping = {}
                for arg in cmdLine:
                    # 'neocortix.com' is expected in the hostname of each NCS instance
                    if 'neocortix.com' in arg:
                        host = arg.split('@')[1]
                        #logger.info( 'forwarding to host %s', host )
                        mapping['host'] = host
                        mapping['pid'] = procInfo['pid']
                    if ':localhost:' in arg:
                        part = arg.split( ':localhost:')[0].split(':')[1]
                        assignedPort = int( part )
                        #logger.info( 'forwarding port %d', assignedPort)
                        mapping['port'] = assignedPort
                if mapping:
                    #logger.debug( 'forwarding port %d to %s', mapping['port'], mapping['host'] )
                    mappings.append( mapping )
    logger.debug( 'mappings: %s', mappings )
    return mappings

def startForwarders( agentInstances, forwarderHost='localhost',
    portRangeStart=7100, maxPort = 7199,
    portMap=None, targetPort=None,
    forwardingCsvFilePath = 'agentForwarding.csv'
    ):
    forwarders = []
    mappings = []
    with open( forwardingCsvFilePath, 'w' ) as csvOutFile:
        print( 'forwarding', 'instanceId', 'instHost', 'instSshPort', 'assignedPort', 'forwarderHost',
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
            finalPort = targetPort if targetPort else assignedPort
            iidAbbrev = iid[0:8]
            sshSpecs = inst['ssh']
            instHost = sshSpecs['host']
            instPort = sshSpecs['port']
            user = sshSpecs['user']
            logger.info( '%d ->%s %s@%s:%s', assignedPort, iidAbbrev, user, instHost, instPort )
            cmd = ['ssh', '-fNT', '-o', 'ExitOnForwardFailure=yes', '-p', str(instPort),
                '-o', 'ServerAliveInterval=%d' % g_.serverAliveInterval,
                '-o', 'ServerAliveCountMax=%d' % g_.serverAliveCountMax,
                '-L', '*:'+str(assignedPort)+':localhost:'+str(finalPort),
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
                print( mapping, iid, instHost, instPort, assignedPort, forwarderHost,
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
