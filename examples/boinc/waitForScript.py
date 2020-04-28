#!/usr/bin/env python3
"""
wait for a python script to finish
"""
# standard imports
import argparse
import datetime
import logging
import os
import shutil
import sys
import time
# third-party imports
import psutil

logger = logging.getLogger(__name__)


def waitForScript( target ):
    print( 'waiting for', target, file=sys.stderr )
    myPid = os.getpid()
    while True:
        otherProc = None
        for proc in psutil.process_iter():
            try:
                procInfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
            except psutil.NoSuchProcess:
                continue
            if 'python' in procInfo['name']:
                scriptName = procInfo['cmdline'][1] if len(procInfo['cmdline']) >1 else '<none>'
                #if procInfo['pid'] == os.getpid():
                #    print( 'THIS process:' )
                if (target in scriptName) and (procInfo['pid'] != myPid):
                    otherProc = procInfo['pid']
                    logger.info( 'waiting for: %s %s', procInfo['pid'], scriptName )
                    #print( 'OTHER process:', procInfo['pid'], scriptName )
                #print(procInfo['pid'], scriptName )
        if not otherProc:
            break
        time.sleep( 10 )



if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__ )
    ap.add_argument( 'target', help='the name (pr partial name) of the script to wait for' )
    args = ap.parse_args()
    logger.info( 'args: %s', str(args) )

    waitForScript( args.target )
