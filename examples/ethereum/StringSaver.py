#!/usr/bin/env python3
"""
deploys or uses a StringSaver contract
"""
import argparse
import datetime
import json
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware


if __name__ == "__main__":
    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'action', help='the action to perform', 
        choices=['deploy', 'get', 'set']
        )
    ap.add_argument( 'configName', help='the network configuration to use' )
    ap.add_argument( '--addr', help='the contract address (for get and set)' )
    ap.add_argument( '--params', nargs='+', help='one or more argumenets for the contract call' )
    args = ap.parse_args()

    #print( args.params )

    w3 = Web3(Web3.IPCProvider( 'ether/%s/geth.ipc' % args.configName ))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    eth = w3.eth

    contractFilePath = 'scripts/StringSaver.json'
    metacontract = None
    with open( contractFilePath, 'r') as jsonInFile:
        try:
            metacontract = json.load(jsonInFile)  # a dict
        except Exception as exc:
            #logger.warning( 'could not load json (%s) %s', type(exc), exc )
            raise

    contractAddress = args.addr
    #contractAddress = '0xdFA8460EaAFD983f7b0Fd73Fcae0d605eDaE47Af'
    if args.action == 'deploy':
        eth.default_account = eth.accounts[0]
        bytecode = metacontract['data']['bytecode']['object']
        contractor = eth.contract(abi=metacontract['abi'], bytecode=bytecode)
        tx = contractor.constructor().transact( {'gasPrice': 1} )
        print( 'transaction hash:', tx.hex() )
        print( 'waiting for receipt')
        receipt = eth.waitForTransactionReceipt( tx )
        print( 'contract address:', receipt.contractAddress )

    elif args.action == 'get':
        if not contractAddress:
            sys.exit( 'error: no addr passed for set')
        getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        result = getter.functions.get().call()
        print( 'result', result )
    elif args.action == 'set':
        if not args.params:  # should test for  None instead
            sys.exit( 'error: no param passed for set')
        if not contractAddress:
            sys.exit( 'error: no addr passed for set')
        eth.default_account = eth.accounts[0]
        setter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        tx = setter.functions.set( args.params[0] ).transact({ 'gasPrice': 1 })
        print( 'waiting for receipt')
        receipt = eth.waitForTransactionReceipt( tx )
        #print( 'receipt', receipt )
        print( 'transaction hash:', receipt.transactionHash.hex() )



'''
abbrevLen = 10;
latestBlockNumber = eth.block_number
print( 'latestBlockNumber', latestBlockNumber )
allAccounts = set()

for blk in range( latestBlockNumber+1 ):  # latestBlockNumber+1
    block = eth.get_block( blk, True )
    timeStamp = block.timestamp
    iso = datetime.datetime.fromtimestamp( timeStamp )
    if (block.number % 1000) == 0:
        print( 'checking', block.number, 'of', latestBlockNumber,
            iso, 'gasLimit', block.gasLimit  )
    for element in block.transactions:
        #print( element )
        allAccounts.add(  element['from'] )
        allAccounts.add(  element['to'] )
        src = element['from'][0:abbrevLen] if element['from'] else None
        to = element['to'][0:abbrevLen] if element['to'] else None
        if to:
            print( iso, element.hash.hex()[0:abbrevLen],
                        'from', src, 'to', to,
                        'value', element.value
                        )

#print( 'allAccounts', allAccounts )
for account in sorted( list( allAccounts ) ):
    bal = eth.get_balance( account )
    print( 'acct', account, 'bal', bal )
'''
