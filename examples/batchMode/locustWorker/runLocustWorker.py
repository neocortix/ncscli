#!/usr/bin/env python3
print( 'runLocust starting' )

import os
import sys
import time

sys.path.append( os.path.expanduser('~/locust'))

import locust
from locust.main import main

class g_:
    csvFilePrefix = None
    reqsOutFile = None
    reqsBadOutFile = None

def on_request_success( request_type, name, response_time, response_length, **kwargs ):
    if g_.reqsOutFile:
        # subtract response_time (milliseconds) from time.time() (seconds)
        requestTime = time.time()-(response_time / 1000.0)

        print( '%.3f'% requestTime, request_type, '%.1f'% response_time,
            response_length, '"'+name+'"', sep=',',
            file=g_.reqsOutFile )

def on_request_failure( request_type, name, response_time, exception, **kwargs ):
    #if kwargs:
    #    print( 'KWARGS', kwargs, file=sys.stderr )
    if g_.reqsBadOutFile:
        # subtracted response_time (milliseconds) from time.time() (seconds)
        requestTime = time.time()-(response_time / 1000.0)

        print( '%.3f'% requestTime, request_type, '%.1f'% response_time,
            '"'+name+'"', type(exception).__name__, '"%s"'%exception, sep=',',
            file=g_.reqsBadOutFile )


if '--csv' in sys.argv:
    index = sys.argv.index( '--csv')
    if len(sys.argv) > index+1:
        g_.csvFilePrefix = sys.argv[index+1]
        print( '--csv prefix', g_.csvFilePrefix )

if g_.csvFilePrefix:
    # open csv output for good requests
    try:
        outFilePath = g_.csvFilePrefix + '_results_good.csv'
        g_.reqsOutFile = open( outFilePath, 'w')
        print( 'timeStamp,method,elapsed,bytes,URL', file=g_.reqsOutFile )
    except Exception as exc:
        print( 'exception opening requests.csv', exc, file=sys.stderr )
    else:
        locust.events.request_success += on_request_success
    # open csv output for bad requests
    try:
        outFilePath = g_.csvFilePrefix + '_results_bad.csv'
        g_.reqsBadOutFile = open( outFilePath, 'w')
        print( 'timeStamp,method,elapsed,URL,failureType,failureMessage', file=g_.reqsBadOutFile )
    except Exception as exc:
        print( 'exception opening requests.csv', exc, file=sys.stderr )
    else:
        locust.events.request_failure += on_request_failure

try:
    retCode =  main()
except Exception as exc:
    print( 'runLocust got exception from main() (%s) %s' % (type(exc), exc),
        file=sys.stderr  )
    sys.exit( 123 )  # arbitrary value, but distinct from more common errors
else:
    print( 'main() returned code %d' % retCode, file=sys.stderr )
print( 'runLocust exiting' )
sys.exit( 0 )  # this is fine
