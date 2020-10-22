import json
import os
import sys

if True:
  deviceLocFilePath = os.path.expanduser( '~/.neocortix/device-location.json' )
  with open( deviceLocFilePath, 'r') as jsonInFile:
      try:
          deviceLocation = json.load( jsonInFile, encoding='utf8' )  # a dict
      except Exception as exc:
          sys.exit( 'could not load json (%s) %s' % (type(exc), exc) )
      else:
          # extract components of the deviceLocation
          cc = deviceLocation.get('country-code') or '<unknown>'
          area = deviceLocation.get('area') or '<unknown>'
          locality = deviceLocation.get('locality') or '<unknown>'
          locStr = '.'.join( [cc, area, locality] )
          # print a message
          print( 'Hello from "%s" (%s)' % (deviceLocation['display-name'], locStr) )
          print( deviceLocation )
