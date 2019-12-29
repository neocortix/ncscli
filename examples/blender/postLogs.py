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
    #logger.info( 'cmd: %s', cmd )
    subprocess.check_call( cmd )

    collection = logsDb[collectionName]
    logger.info( '%s has %d documents', collectionName, collection.count_documents({}) )
    #collection.create_index( 'dateTime' )
    #collection.create_index( 'instanceId' )
    #collection.create_index( 'type' )
    return collection

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( '--dataDir', help='input data darectory', default='./aniData/' )
    ap.add_argument( '--tag', required=True, help='tag for data dir and collection names' )
    ap.add_argument( '--server', default='localhost', help='the host of the mongodb server' )
    ap.add_argument( '--port', default=27017, help='the port for the mongodb server' )
    ap.add_argument( '--official', action='store_true', help='to add this to the table of official tests' )
    args = ap.parse_args()

    mclient = pymongo.MongoClient(args.server, args.port)
    logsDb = mclient.renderLogs
    collections = sorted(logsDb.list_collection_names())
    logger.info( 'existing collections %s', collections )


    record = {'tag': args.tag, '_id': args.tag }

    coll = postCollection( 'recruitLaunched.json', 'launchedInstances' )
    record['launchedInstances'] = coll.name
    coll.create_index( 'instanceId' )

    coll = postCollection( 'recruitInstances.jlog', 'installerLog' )
    record['installerLog'] = coll.name
    coll.create_index( 'instanceId' )
    coll.create_index( 'dateTime' )
   
    coll = postCollection( 'animateWholeFrames_results.jlog', 'rendererLog' )
    record['rendererLog'] = coll.name
    coll.create_index( 'instanceId' )
    coll.create_index( 'dateTime' )
    coll.create_index( 'type' )

    if args.official:
        # get info about the test from the renderer log collection
        query = {'type': 'operation'}
        firstOp = coll.find_one( query, hint=[('dateTime', pymongo.ASCENDING)] )
        record['dateTime'] = firstOp['dateTime']
        opArgs = firstOp['args']
        startingArgs = opArgs.get( 'starting' )
        if startingArgs:
            record['blendFilePath'] = startingArgs.get('blendFilePath')
            startFrame = startingArgs.get('startFrame', 0)
            endFrame = startingArgs.get('endFrame')
            frameStep = startingArgs.get('frameStep')
            if endFrame and frameStep:
                nFrames = len( range( startFrame, endFrame+1, frameStep) )
                record['nFramesReq'] = nFrames
        logger.info( 'storing officially as %s', record['_id'] )
        officialColl = logsDb['officialTests']
        result = officialColl.replace_one( {'_id': record['_id']}, record, upsert=True )
        #officialColl.insert_one( record )
        #logger.info( 'result %s', result )
