#!/usr/bin/env python3
import datetime
from web3 import Web3
from web3.middleware import geth_poa_middleware

w3 = Web3(Web3.IPCProvider('ether/priv_3/geth.ipc'))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

eth = w3.eth

abbrevLen = 16
latestBlockNumber = eth.block_number
print( 'latestBlockNumber', latestBlockNumber )
allAccounts = set()
allContracts = set()

for blk in range( latestBlockNumber+1 ):  # latestBlockNumber+1
    block = eth.get_block( blk, True )
    timeStamp = block.timestamp
    iso = datetime.datetime.fromtimestamp( timeStamp )
    if (block.number % 1000) == 0:
        print( 'checking', block.number, 'of', latestBlockNumber,
            iso, 'gasLimit', block.gasLimit  )
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
        bal = eth.get_balance( account )
    else:
        bal = 0
    print( 'acct', account or 'null', 'bal', bal )
print( 'all Contracts found' )
print( sorted( list( allContracts ) ) )
