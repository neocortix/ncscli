#!/usr/bin/env python3
"""
extracts the "play recap" and some related info from the YAML-style output of ansible-playbook
"""
# standard library modules
import argparse
import csv
import datetime
import json
import logging
import os
import re
import sys
# third-party modules
#import dateutil
import dateutil.parser


logger = logging.getLogger(__name__)

def extractRecap( inFilePath ):
    recap = []
    taskName = ''
    taskNames = []
    taskNum = 0
    foundRecap = False
    deadHost = ''
    taskHost = ''
    inStdout = False
    taskStdout = ''
    stdouts = {}
    inMsg = False
    taskMsg = ''
    msgs = {}
    with open( inFilePath ) as ansiblefile:
        for line in ansiblefile:
            unstrippedLine = line
            line = line.strip()
            logger.debug( 'line: %s', line )
            if line.startswith( 'PLAY RECAP *****'):
                foundRecap = True
                inStdout = False  # redundant, in case other indicator missed
                continue
            if line.startswith( 'TASK [') and '] *****' in line:
                # extract the taskName as the part within [brackets]
                taskName = re.search( r'\[.*\]', line ).group(0)[1:-1]
                if 'create log file' in taskName:
                    logger.info( 'IGNORING task %s', taskName )
                else:
                    logger.info( 'task %d: %s', taskNum, taskName )
                    taskNames.append( taskName )
                    taskNum += 1
                inStdout = False  # redundant, in case other indicator missed
            if unstrippedLine.startswith( 'fatal: ' ):
                taskHost = re.search( r'\[.*\]', line ).group(0)[1:-1] # captrure the part inside [brackets]
                deadHost = re.search( r'\[.*\]', line ).group(0)[1:-1]
                #logger.debug( 'found fatal for %s', deadHost )
            elif unstrippedLine.startswith( 'ok:' ):
                taskHost = re.search( r'\[.*\]', line ).group(0)[1:-1] # captrure the part inside [brackets]
                deadHost = ''
            elif unstrippedLine.startswith( r'  msg: |-' ):
                inMsg = True
                #logger.debug( 'found msg')
            elif unstrippedLine.startswith( r'  msg: ' ) and deadHost:  # may not need this special case
                taskMsg = line.split( 'msg: ' )[1]
                msgs[ deadHost ] = taskMsg
                taskMsg = ''
                #logger.debug( 'found msg "%s"', taskMsg )
            elif unstrippedLine.startswith( r'  msg: ' ):
                taskMsg = line.split( 'msg: ' )[1]
                logger.info( 'MSG for non-dead host "%s" %s', taskHost, taskMsg.split(',') )
                msgs[ taskHost ] = taskMsg
            elif unstrippedLine.startswith( r'  stdout: |-' ):
                inStdout = True
                #logger.debug( 'found stdout')
            elif unstrippedLine.startswith( r'  stdout: ' ):
                taskStdout = line.partition(': ')[2].strip("'")
            elif unstrippedLine.startswith( '  stdout_lines:' ):
                #logger.info( 'finishing stdout')
                inStdout = False
                stdouts[ deadHost ] = taskStdout
                taskStdout = ''
            elif inStdout:
                taskStdout += line + '\n'
            elif not unstrippedLine.startswith( '    ' ):
                if inMsg:
                    msgs[ deadHost ] = taskMsg
                    taskMsg = ''
                inMsg = False
            elif inMsg:
                #logger.debug( 'inMsg adding: %s', line )
                taskMsg += line + '\n'
            if not foundRecap:
                continue
            if len( line ) <= 0:
                break
            parts = line.split()
            #logger.info( 'parts: %s', parts )
            host = parts[0]
            rec = { 'host': host }
            for expr in parts[2:]:
                #logger.info( ' %s', expr)
                terms = expr.split('=')
                rec[terms[0]] = int(terms[1])
            logger.debug( rec )
            recap.append( rec )
            if rec['host'] in msgs:
                rec['msg'] = msgs[ rec['host'] ]
            if rec['failed'] or rec['unreachable']:
                ok = rec['ok']
                logger.info( 'host %s unsuccessful task "%s"', host, taskNames[ok])
                if rec['failed']:
                    rec['failedOn'] = taskNames[ok]
                elif rec['unreachable']:
                    rec['unreachableOn'] = taskNames[ok]
                    #rec['unreachableOn'] = taskNames[ok] if ok > 0 else '.immediately'
                if rec['host'] in stdouts:
                    rec['stdout'] = stdouts[ rec['host'] ]
    #logger.debug( 'stdouts %s', stdouts.keys() )
    #logger.debug( 'msgs %s', msgs.keys() )
    return recap

def getGoodInstances( recap ):
    ''' return dict of instances from a recap that had "ok" and neither failed nor unreachable '''
    goodList = [x for x in recap if (x['ok'] > 0) and not (x['failed'] or x['unreachable']) ]
    goodDict = {}
    for rec in goodList:
        ansibleName = rec['host']
        if ansibleName.startswith('phone_'):
            iid = ansibleName.replace( 'phone_', '' )
        else:
            iid = ansibleName
        goodDict[iid] = rec
    return goodDict


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    logger.setLevel(logging.INFO)
    logger.debug('the logger is configured')

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument('ansibleResultFilePath', help='path to a file containing ansible stdout' )
    args = ap.parse_args()
    #logger.info( 'args %s', args )

    recap = extractRecap( args.ansibleResultFilePath )
    json.dump( recap, sys.stdout, sort_keys=True, indent=2 )
