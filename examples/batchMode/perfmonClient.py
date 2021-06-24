#!/usr/bin/env python3
'''a simple client for the undera perfmon service (which JMeter plugin also uses)'''

import argparse
import csv
import logging
import os
import socket
import sys
import signal
import time


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class g_:
    signaled = False

def sigtermHandler( sig, frame ):
    g_.signaled = True
    logger.info( 'SIGTERM received; will exit gracefully' )

def sigtermSignaled():
    return g_.signaled


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logging.captureWarnings(True)
    #logger.setLevel(logging.DEBUG)  # for more verbosity

    try:
        ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
        ap.add_argument( '--pmHost', help='the host of perfmon server', default='localhost')
        ap.add_argument( '--pmPort', type=int, help='the port of perfmon server', default=4444)
        ap.add_argument( '--outFilePath', default='perfmonOut.csv', help='file name for output csv file' )
        args = ap.parse_args()

        pmHost = args.pmHost
        pmPort = args.pmPort
        logger.info( 'collecting metrics from %s:%d', pmHost, pmPort  )

        metricSpecs = [
            'cpu:combined', 'cpu:idle', 'cpu:irq', 'cpu:nice', 'cpu:softirq',
            'cpu:stolen', 'cpu:system', 'cpu:user', 'cpu:iowait',
            'disks:available', 'disks:queue', 'disks:readbytes', 'disks:reads', 'disks:service',
            'disks:writebytes', 'disks:writes', 'disks:files', 'disks:free',
            'disks:freefiles', 'disks:total', 'disks:useperc', 'disks:used',
            'memory:actualfree', 'memory:actualused', 'memory:free', 'memory:freeperc',
            'memory:ram', 'memory:total', 'memory:used', 'memory:usedperc',
            'network:bytesrecv', 'network:rxdrops', 'network:rxerr', 'network:rxframe', 'network:rxoverruns',
            'network:rx', 'network:bytessent', 'network:txcarrier', 'network:txcollisions', 'network:txdrops',
            'network:txerr', 'network:txoverruns', 'network:used', 'network:speed', 'network:tx',
            'swap:pagein', 'swap:pageout', 'swap:free', 'swap:total', 'swap:used',
            'tcp:bound', 'tcp:close', 'tcp:close_wait', 'tcp:closing', 'tcp:estab',
            'tcp:fin_wait1', 'tcp:fin_wait2', 'tcp:idle', 'tcp:inbound', 'tcp:last_ack',
            'tcp:listen', 'tcp:outbound', 'tcp:syn_recv', 'tcp:time_wait',
        ]
        metricSpecs = sorted( set( metricSpecs ) )
        # for friendlier output, generate names with dots instead of colons
        metricNames = [s.replace(':','.') for s in metricSpecs]

        outFilePath = args.outFilePath  # 'perfmonOut.csv'

        signal.signal( signal.SIGTERM, sigtermHandler )

        try:
            conn = socket.create_connection( (pmHost, pmPort), timeout=30 )
        except Exception as exc:
            if isinstance( exc, ConnectionRefusedError ):
                logger.error( 'could not connect to port %d of host %s %s',
                    pmPort, pmHost, exc
                )
            elif isinstance( exc, socket.gaierror ):
                logger.error( 'could not address port %d of host %s %s',
                    pmPort, pmHost, exc
                )
            elif isinstance( exc, socket.timeout ):
                logger.error( 'timeout while connecting to port %d of host %s %s',
                    pmPort, pmHost, exc
                )
                logger.info( 'could be caused by a firewall or an incorrect port number')
            else:
                logger.error( 'could not connect to port %d of host %s (%s) %s',
                    pmPort, pmHost, type(exc), exc
                )
            sys.exit( 1 )

        cmd = 'metrics:' + '\t'.join( metricSpecs ) + '\n'
        logger.debug( 'cmd: %s', cmd )

        conn.sendall( cmd.encode('utf8') )
        logger.info( 'capturing metrics to file %s', os.path.realpath(outFilePath)  )
        with open( outFilePath, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow( ['timeStamp'] + metricNames )

            sockFile = conn.makefile( 'r' )
            for line in sockFile:
                timeNow = time.time()  # float seconds since UTC epoch
                timeNowMs = timeNow * 1000
                line = line.strip()
                parts = line.split('\t')
                numbers = [float(part) for part in parts]
                numbers.insert( 0, timeNowMs )
                writer.writerow( numbers )
                outfile.flush()
                if sigtermSignaled():
                    break
    except Exception as exc:
        logger.error( 'an exception occurred (%s) %s',
            type(exc), exc
        )
        sys.exit(2)
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
