#!/usr/bin/env python3
"""
posts rendering logs to a mongodb
"""

# standard library modules
import argparse
import logging
import os
import subprocess

# third-party module(s)
import pymongo  # would be needed for indexing

logger = logging.getLogger(__name__)


def postCollection( fileName, collName=None ):
    # uses the golobal args from the ArgumentParser
    srcFilePath = os.path.join( args.dataDir, args.tag, fileName )
    if not collName:
        collName = os.path.splitext( os.path.basename( fileName ) )[0]
    collectionName = collName + '_' + args.tag

    cmd = [
        'mongoimport', '--host', args.server, '--port', str(args.port),
        '--drop',
        '-d', 'renderLogs', '-c', collectionName,
        srcFilePath
    ]
    if fileName.endswith( '.json' ):
        cmd.append( '--jsonArray' )
    logger.info( 'cmd: %s', cmd )
    subprocess.check_call( cmd )

    collection = logsDb[collectionName]
    print( '%s has %d documents' % (collectionName, collection.count_documents({})) )
    collection.create_index( 'dateTime' )
    collection.create_index( 'instanceId' )
    collection.create_index( 'type' )

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--dataDir', help='input data darectory', default='./aniData/' )
    ap.add_argument( '--tag', required=True, help='tag for data dir and collection names' )
    ap.add_argument( '--server', default='localhost', help='the host of the mongodb server' )
    ap.add_argument( '--port', default=27017, help='the port for the mongodb server' )
    args = ap.parse_args()

    mclient = pymongo.MongoClient(args.server, args.port)
    logsDb = mclient.renderLogs
    collections = sorted(logsDb.list_collection_names())
    logger.info( 'existing collections %s', collections )


    postCollection( 'recruitLaunched.json', 'launchedInstances' )
    postCollection( 'recruitInstances.jlog', 'installerLog' )
    postCollection( 'animateWholeFrames_results.jlog', 'rendererLog' )
