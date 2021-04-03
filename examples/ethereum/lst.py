#!/usr/bin/env python3
# standard library modules
import argparse
import datetime
import logging
# third-party modules
from web3 import Web3
from web3.middleware import geth_poa_middleware


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__, fromfile_prefix_chars='@' )
    ap.add_argument( 'configName', help='the name of the network configuration' )
    args = ap.parse_args()

    configName = args.configName  # 'priv_5'

    w3 = Web3(Web3.IPCProvider( 'ether/%s/geth.ipc' % configName ))
    #w3 = Web3(Web3.HTTPProvider("http://34.222.xx.yy:8545") )

    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    eth = w3.eth

    abbrevLen = 16
    latestBlockNumber = eth.block_number
    print( 'latestBlockNumber', latestBlockNumber )
    allAccounts = set()
    allContracts = set()

    if latestBlockNumber:
        timeStamp = eth.get_block(1).timestamp
        # all ethereum timestamps are seconds since utc epoch
        dt = datetime.datetime.fromtimestamp(timeStamp )
        print( 'block 1 dateTime (iso)', dt.isoformat() )

    for blk in range( latestBlockNumber+1 ):  # latestBlockNumber+1
        block = eth.get_block( blk, True )
        timeStamp = block.timestamp
        iso = datetime.datetime.fromtimestamp( timeStamp )
        if (block.number % 1000) == 0:
            print( 'checking', block.number, 'of', latestBlockNumber,
                iso, 'gasLimit', block.gasLimit  )
            #print( block.keys() )
        for element in block.transactions:
            #print( element )
            allAccounts.add( element['from'] )
            allAccounts.add( element['to'] if element['to'] else '' )
            src = element['from'][0:abbrevLen] if element['from'] else None
            to = element['to'][0:abbrevLen] if element['to'] else None
            #if element.input:
            #    print( 'input: ', element.input[0:8])
            if to:
                print( iso, element.hash.hex()[0:abbrevLen],
                            'from', src, 'to', to,
                            'gasPrice', element.gasPrice,
                            'value', element.value
                            )
                code = eth.get_code( element['to'] )
                if len(code) > 0:
                    #print( 'contract address: ', element['to'] )
                    allContracts.add( element['to'] )
            else:
                print( iso, element.hash.hex(),
                            'from', src,
                            'value', element.value
                            )
                #print( 'tx keys:', element.keys() )

    print( 'all Accounts found' )
    for account in ( list( allAccounts ) ):  # sorted
        if account:
            bal = w3.fromWei( eth.get_balance( account ), 'ether' )
        else:
            bal = 0
        print( 'acct', account or 'null', 'bal', bal )
    print( 'all Contracts found' )
    print( sorted( list( allContracts ) ) )
