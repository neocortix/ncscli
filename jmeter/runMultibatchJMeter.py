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
    logger.info( 'cmd: %s', cmd )
    if True:
        proc = subprocess.run( cmd )
        rc = proc.returncode
        logger.info( 'returnCode %d from runDistributedJMeter for %s', rc, subDir )

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
    ap.add_argument( '--outDataDir', required=True, help='a path to the output data dir for this run (required)' )
    '''
    ap.add_argument( '--filter', help='json to filter instances for launch',
        default = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }'
        )
    ap.add_argument( '--jmxFile', required=True, help='the JMeter test plan file path (required)' )
    ap.add_argument( '--jtlFile', help='the file name of the jtl file produced by the test plan (if any)',
        default=None
        )
    ap.add_argument( '--planDuration', type=float, default=0, help='the expected duration of the test plan, in seconds' )
    ap.add_argument( '--workerDir', help='the directory to upload to workers',
        default='jmeterWorker'
        )
    ap.add_argument( '--nWorkers', type=int, default=6, help='the number of Load-generating workers' )
    # for analysis and plotting
    ap.add_argument( '--rampStepDuration', type=float, default=60, help='duration of ramp step, in seconds' )
    ap.add_argument( '--SLODuration', type=float, default=240, help='SLO duration, in seconds' )
    ap.add_argument( '--SLOResponseTimeMax', type=float, default=2.5, help='SLO RT threshold, in seconds' )
    '''
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

    #TODO get jobPlan from json (or yml) file
    jobPlan = {
        'defaults': {
            'filter': { "dar": ">=99", "storage": ">=2000000000" },
            'workerDir': 'locationWorker',
            'jmxFile': 'locationDemo.jmx',
            'planDuration': 300,
            'nWorkers': 3
        },
        'batches': [
            {'filter': { "regions": ["usa-east", "usa-west"], "dar": ">=99", "storage": ">=2000000000" },
                'nWorkers': 6},
            {'filter': { "regions": ["russia"], "dar": ">=99", "storage": ">=2000000000" },
                'nWorkers': 3},
        ]
    }
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
    # for each batch, check input values and ill in default values
    for batch in batches:
        workerDirPath = batch.get('workerDir')
        if not workerDirPath:
            workerDirPath = defaults.get('workerDir')
        if not workerDirPath:
            logger.error( 'this version requires a workerDirPath' )
            sys.exit( 1 )
        workerDirPath = workerDirPath.rstrip( '/' )  # trailing slash could cause problems with rsync
        if workerDirPath:
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
            logger.info( 'planJtlFiles: %s', planJtlFiles )
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
        if not jtlFilePath:
            jtlFilePath = 'TestPlan_results.csv'
        logger.info( 'jtlFilePath: %s', jtlFilePath )

        nFrames = batch.get('nWorkers') or defaults.get('nWorkers')
        nWorkers = math.ceil(nFrames*1.5) if nFrames <=10 else round( max( nFrames*1.12, nFrames + 5 * math.log10( nFrames ) ) )
        batch['nInstances'] = nWorkers
        batch['nFrames'] = nFrames
        
        dateTimeTag = datetime.datetime.now().strftime( '%Y-%m-%d_%H%M%S' )
        outDataDir = args.outDataDir
    logger.info( jobPlan )

    # abort if outDataDir is not empty enough
    if os.path.isdir( outDataDir) \
        and os.listdir( outDataDir ):
        logger.error( 'please use a different outDataDir for each run' )
        sys.exit( 1 )

    logger.info( 'ready to run batches')
    # would run batches in parallel
    if True:
        nBatches = len(batches)
        with futures.ThreadPoolExecutor( max_workers=nBatches ) as executor:
            parIter = executor.map( runJMeterBatch, batches, [outDataDir]*nBatches )
            results = list( parIter )
    else:
        for batch in batches:
            runJMeterBatch( batch, outDataDir)

    '''
    try:
        rc = batchRunner.runBatch(
            frameProcessor = JMeterFrameProcessor(),
            commonInFilePath = JMeterFrameProcessor.workerDirPath,
            authToken = args.authToken or os.getenv( 'NCS_AUTH_TOKEN' ) or 'YourAuthTokenHere',
            cookie = args.cookie,
            encryptFiles=False,
            timeLimit = frameTimeLimit + 40*60,
            instTimeLimit = 6*60,
            frameTimeLimit = frameTimeLimit,
            filter = args.filter,
            #filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=3800000000", "storage": ">=2000000000" }',
            #filter = '{ "regions": ["usa", "india"], "dar": "==100", "dpr": ">=48", "ram": ">=2800000000", "app-version": ">=2.1.11" }',
            outDataDir = outDataDir,
            startFrame = 1,
            endFrame = nFrames,
            nWorkers = nWorkers,
            limitOneFramePerWorker = True,
            autoscaleMax = 1
        )
        if (rc == 0) and os.path.isfile( outDataDir +'/recruitLaunched.json' ):
            rampStepDuration = args.rampStepDuration
            SLODuration = args.SLODuration
            SLOResponseTimeMax = args.SLOResponseTimeMax

            rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/plotJMeterOutput.py',
                '--dataDirPath', outDataDir,
                '--rampStepDuration', str(rampStepDuration), '--SLODuration', str(SLODuration),
                '--SLOResponseTimeMax', str(SLOResponseTimeMax)
                ],
                stdout=subprocess.DEVNULL )
            if rc2:
                logger.warning( 'plotJMeterOutput exited with returnCode %d', rc2 )
    
            jtlFileName = os.path.basename( jtlFilePath )
            if jtlFileName:
                nameParts = os.path.splitext(jtlFileName)
                mergedJtlFileName = nameParts[0]+'_merged_' + dateTimeTag + nameParts[1]
                rc2 = subprocess.call( [sys.executable, scriptDirPath()+'/mergeBatchOutput.py',
                    '--dataDirPath', outDataDir,
                    '--csvPat', 'jmeterOut_%%03d/%s' % jtlFileName,
                    '--mergedCsv', mergedJtlFileName
                    ], stdout=subprocess.DEVNULL
                    )
                if rc2:
                    logger.warning( 'mergeBatchOutput.py exited with returnCode %d', rc2 )
                else:
                    if not os.path.isfile( jmeterBinPath ):
                        logger.info( 'no jmeter installed for producing reports (%s)', jmeterBinPath )
                    else:
                        rcx = subprocess.call( [jmeterBinPath,
                            '-g', os.path.join( outDataDir, mergedJtlFileName ),
                            '-o', os.path.join( outDataDir, 'htmlReport' )
                            ], stderr=subprocess.DEVNULL
                        )
                        try:
                            shutil.move( 'jmeter.log', os.path.join( outDataDir, 'genHtml.log') )
                        except Exception as exc:
                            logger.warning( 'could not move the jmeter.log file (%s) %s', type(exc), exc )
                        if rcx:
                            logger.warning( 'jmeter reporting exited with returnCode %d', rcx )
        sys.exit( rc )
    except KeyboardInterrupt:
        logger.warning( 'an interuption occurred')
    '''
