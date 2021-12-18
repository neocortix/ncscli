#!/usr/bin/env python3
'''launches instances and runs JMeter on them'''
import argparse
import datetime
from concurrent import futures
import glob
#import hashlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import uuid

# neocortix modules
import ncscli.ncs as ncs
import ncscli.batchRunner as batchRunner
import jmxTool  # assumed to be in the same dir as this script


logger = logging.getLogger(__name__)


def scriptDirPath():
    '''returns the absolute path to the directory containing this script'''
    return os.path.dirname(os.path.realpath(__file__))

def runJMeterBatch( batch, outDataDir ):
    binPath = scriptDirPath()+'/runDistributedJMeter.py'
    filter = batch['filter']
    regionsStr = '+'.join( filter.get('regions', []) )
    if not regionsStr:
        regionsStr = 'any'
    filtersJson = json.dumps( filter )

    randomPart = str( uuid.uuid4() )[0:18]
    subDir = 'batch_' + regionsStr + '_' + randomPart
    subDirPath  = os.path.join( outDataDir, subDir )

    cmd = [ binPath,
        '--filter', filtersJson,
        '--workerDir', batch['workerDir'],
        '--jmxFile', batch['jmxFile'],
        '--planDuration', str(batch['planDuration']), 
        '--outDataDir', subDirPath,
        '--nWorkers', str(batch['nWorkers'])
    ]
    if batch.get( 'jtlFile' ):
        cmd.extend( ['--jtlFile', batch['jtlFile']] )
    logger.info( 'cmd: %s', cmd )
    proc = subprocess.run( cmd )
    rc = proc.returncode
    logger.info( 'returnCode %d from runDistributedJMeter for %s', rc, subDir )
    return rc

if __name__ == '__main__':
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.INFO)
    #batchRunner.logger.setLevel(logging.DEBUG)  # for more verbosity

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( '--authToken', help='the NCS authorization token to use (or none, to use NCS_AUTH_TOKEN env var' )
    ap.add_argument( '--projDir', required=True, help='a path to the input project data dir for this run (required)' )
    ap.add_argument( '--outDataDir', required=True, help='a path to the output data dir for this run (required)' )
    # for analysis and plotting
    ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration of ramp step, in seconds' )
    ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
    ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.5, help='SLO RT threshold, in seconds' )
    # environmental
    ap.add_argument( '--jmeterBinPath', help='path to the local jmeter.sh for generating html report' )
    ap.add_argument( '--cookie' )
    args = ap.parse_args()

    authToken = args.authToken or os.getenv( 'NCS_AUTH_TOKEN' )
    if not authToken:
        logger.error( 'no authToken was given as argument or $NCS_AUTH_TOKEN' )
        sys.exit( 1 )
    if not ncs.validAuthToken( authToken ):
        logger.error( 'the given authToken was not an alphanumeric ascii string' )
        sys.exit( 1 )

    projDirPath = args.projDir
    if not projDirPath:
        logger.error( 'please provide a --projDir (project directory path) to read from')
        sys.exit( 1 )
    with open( os.path.join( projDirPath, 'jobPlan.json' ), 'r') as jsonInFile:
        try:
            jobPlan = json.load(jsonInFile)  # a dict
        except Exception as exc:
            logger.error( 'could not load jobPlan.json (%s) %s', type(exc), exc )
            sys.exit( 1 )
    logger.info( 'jobPlan: %s', jobPlan )
    if 'batches' not in jobPlan:
            logger.error( 'no batches were defined in the jobPlan' )
            sys.exit( 1 )
    if 'defaults' not in jobPlan:
            logger.error( 'no defaults were defined in the jobPlan' )
            sys.exit( 1 )

    jmeterBinPath = args.jmeterBinPath
    if not jmeterBinPath:
        jmeterVersion = '5.4.1'  # 5.3 and 5.4.1 have been tested, others may work as well
        jmeterBinPath = scriptDirPath()+'/apache-jmeter-%s/bin/jmeter.sh' % jmeterVersion

    defaults = jobPlan['defaults']
    batches = jobPlan['batches']
    # in the defaults make any directly specified regions override any found in default filter
    if 'regions' in defaults:
        #TODO do more checking for unhealthy inputs
        if 'filter' in defaults:
            defaults['filter']['regions'] = defaults['regions']
        else:
            defaults['filter'] = {'regions': defaults['regions']}
    # for each batch, check input values and fill in default values
    jtlFilePaths = []
    for batch in batches:
        if 'filter' not in batch:
            if 'filter' not in defaults:
                logger.error( 'please provide a filter in defaults or in each batch (%s)', batch )
                sys.exit( 1 )
            batch['filter'] = defaults.get('filter', {}).copy()
        #if not batch['filter']: # actually an empty filter is allowed
        #    logger.error( 'no filter for batch %s', batch )
        if batch.get('regions'):
            regions = batch.get('regions')
            if isinstance( regions, str ):
                regions = [regions]
            elif not isinstance( regions, list ):
                logger.error( 'the regions in a jobPlan batch is not a list (%s)', batch)
                sys.exit( 1 )
            batch['filter']['regions'] = regions
        workerDirPath = batch.get('workerDir')
        if not workerDirPath:
            workerDirPath = defaults.get('workerDir')
        if not workerDirPath:
            logger.error( 'this version requires a workerDirPath' )
            sys.exit( 1 )
        workerDirPath = workerDirPath.rstrip( '/' )  # trailing slash could cause problems with rsync
        if workerDirPath:
            # interpret as relative path, prepending projDir
            workerDirPath = projDirPath + '/' + workerDirPath
            if not os.path.isdir( workerDirPath ):
                logger.error( 'the workerDirPath "%s" is not a directory', workerDirPath )
                sys.exit( 1 )
            batch['workerDir'] = workerDirPath
        else:
            logger.error( 'this version requires a workerDirPath' )
            sys.exit( 1 )
        logger.debug( 'workerDirPath: %s', workerDirPath )

        jmxFilePath = batch.get('jmxFile')
        if not jmxFilePath:
            jmxFilePath = defaults.get('jmxFile')
        jmxFullerPath = os.path.join( workerDirPath, jmxFilePath )
        if not os.path.isfile( jmxFullerPath ):
            logger.error( 'the jmx file "%s" was not found in %s', jmxFilePath, workerDirPath )
            sys.exit( 1 )
        batch['jmxFile'] = jmxFilePath
        logger.debug( 'using test plan "%s"', jmxFilePath )

        # parse the jmx file so we can find duration and jtl file references
        jmxTree = jmxTool.parseJmxFile( jmxFullerPath )

        # use given planDuration unless it is not positive, in which case extract from the jmx
        planDuration = batch.get('planDuration', None)
        if planDuration is None:
            planDuration = defaults.get('planDuration')
        if planDuration <= 0:
            planDuration = jmxTool.getDuration( jmxTree )
            logger.debug( 'jmxDur: %s seconds', planDuration )
        batch['planDuration'] = planDuration
        frameTimeLimit = max( round( planDuration * 1.5 ), planDuration+8*60 ) # some slop beyond the planned duration
        batch['frameTimeLimit'] = frameTimeLimit

        #jtlFilePath = None
        jtlFilePath = batch['jtlFile'] if 'jtlFile' in batch else defaults.get('jtlFile')
        if jtlFilePath:
            #jtlFilePath = args.jtlFile
            if ':' in jtlFilePath:
                logger.error( 'a colon was found in the jtlFile path' )
                sys.exit( 1 )
            # for now, reject any backslashes because they do not work on linux
            if '\\' in jtlFilePath:
                logger.error( 'backslashes are not allowed in the jtlFile path' )
                sys.exit( 1 )
            # replace backslash with slash, even though backslash is technically legal in posix
            jtlFilePath = jtlFilePath.replace( '\\', '/' )
            # normalize it (removes redundant slashes and other weirdness)
            jtlFilePath = os.path.normpath( jtlFilePath )
            # make sure it is not an absolute path
            if jtlFilePath == os.path.abspath( jtlFilePath ):
                logger.error( 'absolute paths are not supported for jtlFile path' )
                sys.exit( 1 )
            if '../' in jtlFilePath:
                logger.error( '"../" is not supported for jtlFile path' )
                sys.exit( 1 )

            planJtlFiles = jmxTool.findJtlFileNames( jmxTree )
            logger.debug( 'planJtlFiles: %s', planJtlFiles )
            normalizedJtlFiles = planJtlFiles
            # don't replace backslashes for now
            #normalizedJtlFiles = [path.replace( '\\', '/' ) for path in planJtlFiles]
            normalizedJtlFiles = [os.path.normpath(path) for path in normalizedJtlFiles]
            if jtlFilePath not in normalizedJtlFiles:
                prepended = os.path.join( 'jmeterOut', jtlFilePath )
                if prepended in normalizedJtlFiles:
                    # a hack to make old examples work
                    jtlFilePath = prepended
                else:
                    logger.error( 'the given jtlFile was not found in the test plan' )
                    sys.exit( 1 )

            batch['jtlFile'] = jtlFilePath
        jtlFilePaths.append( jtlFilePath or '' )
        logger.debug( 'jtlFilePath: %s', jtlFilePath )

        nWorkers = batch.get('nWorkers') or defaults.get('nWorkers')
        if not nWorkers:
            logger.error( 'please provide nWorkers in jobPlan defaults or in each batch' )
            sys.exit( 1 )
        batch['nWorkers'] = nWorkers
        
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = args.outDataDir
    logger.info( 'processed jobPlan: %s', jobPlan )

    # decide a jtlFilePath name to merge from, based on the list of those specified
    jtlFilePath = 'TestPlan_results.csv'
    if jtlFilePaths:
        if min( jtlFilePaths ) != max( jtlFilePaths ):
            logger.warning( 'more than one jtl file was specified; merge may not work as expected')
        elif jtlFilePaths[0]:
            jtlFilePath = jtlFilePaths[0]
    logger.info( 'will merge %s files, given files %s', jtlFilePath, jtlFilePaths )

    # abort if outDataDir is not empty enough
    if os.path.isdir( outDataDir) \
        and os.listdir( outDataDir ):
        logger.error( 'please use a different outDataDir for each run' )
        sys.exit( 1 )

    logger.info( 'ready to run %d batches', len( batches ))
    # run batches in parallel (set fals for sequential debugging)
    if True:
        nBatches = len(batches)
        with futures.ThreadPoolExecutor( max_workers=nBatches ) as executor:
            parIter = executor.map( runJMeterBatch, batches, [outDataDir]*nBatches )
            resultCodes = list( parIter )
    else:
        resultCodes = []
        for batch in batches:
            rc = runJMeterBatch( batch, outDataDir)
            resultCodes.append( rc )
    if all( resultCodes ):
        logger.error( 'all batches gave bad return codes')
        sys.exit( 1 )
    if True:
        jtlFileName = os.path.basename( jtlFilePath )
        if jtlFileName:
            nameParts = os.path.splitext(jtlFileName)
            mergedJtlFileName = nameParts[0]+'_merged_' + nameParts[1]
            rc = subprocess.call( [sys.executable, scriptDirPath()+'/mergeBatchOutput.py',
                '--dataDirPath', outDataDir, '--multibatch', 'True',
                '--csvPat', 'jmeterOut_%%03d/%s' % jtlFileName,
                '--mergedCsv', mergedJtlFileName
                ], stdout=subprocess.DEVNULL
                )
            if rc:
                logger.warning( 'mergeMultibatchOutput.py exited with returnCode %d', rc )
            else:
                if not os.path.isfile( jmeterBinPath ):
                    logger.info( 'no jmeter installed for producing reports (%s)', jmeterBinPath )
                else:
                    rcx = subprocess.call( [jmeterBinPath,
                        '-g', os.path.join( outDataDir, mergedJtlFileName ),
                        '-o', os.path.join( outDataDir, 'htmlReport' ),
                        '--jmeterlogfile', os.path.join( outDataDir, 'genHtml.log' ),  # like -j
                        '--jmeterproperty', 'jmeter.reportgenerator.overall_granularity=15000', # like -J
                        ], stderr=subprocess.DEVNULL
                    )
                    if rcx:
                        logger.warning( 'jmeter reporting exited with returnCode %d', rcx )
        rampStepDuration = args.rampStepDuration
        SLODuration = args.SLODuration
        SLOResponseTimeMax = args.SLOResponseTimeMax
        rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/plotJMeterOutput.py',
            '--dataDirPath', outDataDir, '--multibatch', 'True',
            '--rampStepDuration', str(rampStepDuration), '--SLODuration', str(SLODuration),
            '--SLOResponseTimeMax', str(SLOResponseTimeMax)
            ],
            stdout=subprocess.DEVNULL )
        if rc2:
            logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )

    '''
    try:
        rc = batchRunner.runBatch( ...
        )
        if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
    
        sys.exit( rc )
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
    '''
    logger.info( 'finished' )
