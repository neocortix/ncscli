#!/usr/bin/env python3
"""
terminates instances and purges them from known_hosts
"""
# standard library modules
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
# third-part
import psutil
# neocortix modules
import ncscli.ncs as ncs
import sshForwarding  # expected to be in the same directory


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    logger.debug( 'the logger is configured' )

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'inFilePath', help='file path of json instance descriptions' )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (default uses env var)' )
    args = ap.parse_args()

    # use authToken env var if none given as arg
    authToken = args.authToken or os.getenv('NCS_AUTH_TOKEN')
    if not authToken:
        logger.error( 'no authToken given, so not terminating')
        sys.exit(1)
    inFilePath = args.inFilePath
    if os.path.isdir( inFilePath ):
        inFilePath = os.path.join( inFilePath, 'recruitLaunched.json' )
        logger.debug( 'a directory path was given; reading from %s', inFilePath )
    respCode = None
    with open( inFilePath ) as inFile:
        instances = json.load( inFile )
        if not instances:
            logger.info( 'no instances found' )
            respCode = 204
        else:
            forwarders = sshForwarding.findForwarders()
            forwardersByHost = { fw['host']: fw for fw in forwarders }
            for inst in instances:
                iid = inst['instanceId']
                instHost = inst['ssh']['host']
                if instHost in forwardersByHost:
                    pid = forwardersByHost[instHost].get('pid')
                    if pid:
                        logger.debug( 'canceling forwarding (pid %d) for %s', pid, iid[0:8] )
                        os.kill( pid, signal.SIGTERM )

            jobId = instances[0].get('job')
            # terminate only if there's a job id
            if jobId:
                logger.info( 'terminating instances for job %s', jobId )
                respCode = ncs.terminateJobInstances( authToken, jobId )
            else:
                logger.warning( 'no job id in instances file')
                respCode = 500
            ncs.purgeKnownHosts( instances )
    if respCode in [200, 204]:
        logger.info( 'finished' )
        sys.exit(0)
    else:
        logger.error( 'error code: %s', respCode )
        sys.exit(2)
