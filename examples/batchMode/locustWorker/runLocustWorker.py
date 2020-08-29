#!/usr/bin/env python3
print( 'runLocust starting' )

import os
import sys

sys.path.append( os.path.expanduser('~/locust'))

from locust.main import main

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
