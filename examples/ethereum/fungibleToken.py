#!/usr/bin/env python3
"""
deploys or uses an ERC-20 fungible token contract
"""
import argparse
import datetime
import json
import logging
import sys

from web3 import Web3
from web3.middleware import geth_poa_middleware


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parseLogLevel( arg ):
    '''return a logging level (int) for the given case-insensitive level name'''
    arg = arg.lower()
    map = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
        }
    if arg not in map:
        logger.warning( 'the given logLevel "%s" is not recognized (using "info" level, instead)', arg )
    setting = map.get( arg, logging.INFO )

    return setting

def getName( contractAddress, metacontract ):
    result = None
    if contractAddress and metacontract and 'abi' in metacontract:
        try:
            getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
            result = getter.functions.name().call()
        except Exception as exc:
            logger.warning( 'got exception %s (%s)', type(exc), exc )
    return result

def getSymbol( contractAddress, metacontract ):
    result = None
    if contractAddress and metacontract and 'abi' in metacontract:
        try:
            getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
            result = getter.functions.symbol().call()
        except Exception as exc:
            logger.warning( 'got exception %s (%s)', type(exc), exc )
    return result

def getTotalSupply( contractAddress, metacontract ):
    result = None
    if contractAddress and metacontract and 'abi' in metacontract:
        try:
            getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
            supply = getter.functions.totalSupply().call()
            decimalShift = getter.functions.decimals().call()
            logger.debug( 'decimalShift: %d', decimalShift )
            return (supply, decimalShift )
        except Exception as exc:
            logger.warning( 'got exception %s (%s)', type(exc), exc )
    return result


if __name__ == "__main__":
    # configure logger formatting
    logFmt = '%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s'
    logDateFmt = '%Y/%m/%d %H:%M:%S'
    formatter = logging.Formatter(fmt=logFmt, datefmt=logDateFmt )
    logging.basicConfig(format=logFmt, datefmt=logDateFmt)
    logger.setLevel(logging.WARNING)

    ap = argparse.ArgumentParser( description=__doc__,
        fromfile_prefix_chars='@', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    ap.add_argument( 'action', help='the action to perform', 
        choices=['deploy', 'mint', 'allowance', 'approve', 'balance', 'info', 'name',
            'symbol', 'totalSupply', 'transfer', 'transferFrom']
        )
    ap.add_argument( 'configName', help='the network configuration to use' )
    ap.add_argument( '--addr', help='the contract address for a transaction or query' )
    ap.add_argument( '--name', help='the name of the token for deploy' )
    ap.add_argument( '--symbol', help='the symbol (short name) of the token for deploy' )
    ap.add_argument( '--from', help='the source account addr for a transaction or query' )
    ap.add_argument( '--to', help='the destination account addr for a transaction' )
    ap.add_argument( '--amount', type=int, help='the amount for a transaction' )
    ap.add_argument( '--logLevel', default ='info', help='verbosity of log (e.g. debug, info, warning, error)' )
    args = ap.parse_args()

    logLevel = parseLogLevel( args.logLevel )
    logger.setLevel(logLevel)
    logger.debug('the logger is configured')

    fromArg = vars(args)['from']  # workaround for 'from' keyword problem


    w3 = Web3(Web3.IPCProvider( 'ether/%s/geth.ipc' % args.configName ))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    eth = w3.eth

    contractFilePath = 'contracts/ERC20PresetMinterPauser.json'
    metacontract = None
    try:
        with open( contractFilePath, 'r') as jsonInFile:
            try:
                metacontract = json.load(jsonInFile)  # a dict
            except Exception as exc:
                logger.error( 'could not load json (%s) %s', type(exc), exc )
                sys.exit( 1 )
    except Exception as exc:
            logger.error( 'could not open %s (%s)', contractFilePath, type(exc) )
            sys.exit( 1 )


    contractAddress = args.addr
    if args.action != 'deploy' and not contractAddress:
        sys.exit( 'error: no contract --addr passed for ' + args.action )

    if args.action == 'deploy':
        tokenName = args.name
        symbol = args.symbol
        if not tokenName:
            sys.exit( 'no --name given')
        if not symbol:
            sys.exit( 'no --symbol given')
        eth.default_account = eth.accounts[0]
        bytecode = metacontract['bytecode']
        contractor = eth.contract(abi=metacontract['abi'], bytecode=bytecode)
        try:
            tx = contractor.constructor( tokenName, symbol ).transact( {'gas': 3000000,'gasPrice': 1} )
        except Exception as exc:
            logger.error( 'deploy got exception (%s) %s', type(exc), exc )
            sys.exit( 1 )
        logger.info( 'transaction hash: %s', tx.hex() )
        logger.info( 'waiting for receipt')
        try:
            receipt = eth.waitForTransactionReceipt( tx )
        except Exception as exc:
            logger.warning( 'waiting for receipt for transaction %s got exception (%s) %s',
                tx.hex(), type(exc), exc )
            logger.info( 'the deploy may or may not have gone through')
            sys.exit( 1 )
        print( 'contract address:', receipt.contractAddress )
    elif args.action == 'balance':
        if not contractAddress:
            sys.exit( 'error: no contract --addr passed for getting balance')
        srcAddr = fromArg
        if not srcAddr:
            srcAddr = eth.accounts[0]
            logger.info( 'using default account to get balance of' )
        checkedAddr = Web3.toChecksumAddress( srcAddr )
        logger.info( 'from: %s', checkedAddr )
        getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        result = getter.functions.balanceOf( checkedAddr ).call()
        decimalShift = getter.functions.decimals().call()
        logger.debug( 'decimalShift: %d', decimalShift )
        print( result / (10**decimalShift) )
        logger.info( 'unshifted bal: %s', result )
    elif args.action == 'name':
        if not contractAddress:
            sys.exit( 'error: no contract --addr passed for getting name')
        result = getName( contractAddress, metacontract )
        print( result )
    elif args.action == 'symbol':
        if not contractAddress:
            sys.exit( 'error: no contract --addr passed for getting symbol')
        result = getSymbol( contractAddress, metacontract )
        print( result )
    elif args.action == 'totalSupply':
        tup = getTotalSupply( contractAddress, metacontract )
        if tup and len(tup) >= 2:
            unshifted = tup[0]
            decimalShift = tup[1]
            logger.info( 'unshifted supply: %s', unshifted )
            print( unshifted / (10**decimalShift) )
    elif args.action == 'info':
        result = {'name': None, 'symbol': None, 'totalSupply': None }
        result['name'] = getName( contractAddress, metacontract )
        result['symbol'] = getSymbol( contractAddress, metacontract )
        tup = getTotalSupply( contractAddress, metacontract )
        if tup and len(tup) >= 2:
            unshifted = tup[0]
            decimalShift = tup[1]
            logger.info( 'unshifted supply: %s', unshifted )
            result['totalSupply'] = unshifted / (10**decimalShift)
            result['totalSupplyUnshifted'] = unshifted
            print( json.dumps( result ) )
    elif args.action == 'mint':
        if not args.amount:  # should test for  None instead
            sys.exit( 'error: no --amount passed for mint')
        amount = args.amount
        destAddr = args.to
        if not destAddr:
            destAddr = eth.accounts[0]
            logger.info( 'using default account to mint to' )
        eth.default_account = eth.accounts[0]
        setter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        decimalShift = setter.functions.decimals().call()
        shiftedAmount = amount / (10**decimalShift)
        logger.info( 'decimalShift: %d, shiftedAmount %.18g', decimalShift, shiftedAmount )
        try:
            tx = setter.functions.mint( destAddr, amount ).transact({ 'gas': 100000, 'gasPrice': 1 })
        except Exception as exc:
            logger.error( 'mint got exception (%s) %s', type(exc), exc )
            sys.exit( 1 )
        logger.info( 'waiting for receipt')
        try:
            receipt = eth.waitForTransactionReceipt( tx )
        except Exception as exc:
            logger.warning( 'waiting for receipt for mint transaction %s got exception (%s) %s',
                tx.hex(), type(exc), exc )
            logger.info( 'the mint may or may not have gone through')
            sys.exit( 1 )
        print( 'transaction hash:', receipt.transactionHash.hex() )
    elif args.action == 'approve':
        if args.amount == None:
            sys.exit( 'error: no --amount passed for approve')
        if args.amount <= 0:
            sys.exit( 'error: non-positive --amount passed for approve')
        amount = args.amount
        spender = args.to
        if not spender:
            spender = eth.accounts[0]
            print( 'approving the primary account (which may not make sense)', file=sys.stderr )
        eth.default_account = eth.accounts[0]
        setter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        decimalShift = setter.functions.decimals().call()
        shiftedAmount = amount / (10**decimalShift)
        print( 'decimalShift', decimalShift, 'shiftedAmount', shiftedAmount )

        tx = setter.functions.approve( spender, amount ).transact({ 'gas': 100000, 'gasPrice': 1 })
        print( 'waiting for receipt')
        receipt = eth.waitForTransactionReceipt( tx )
        #print( 'receipt', receipt )
        print( 'transaction hash:', receipt.transactionHash.hex() )
    elif args.action == 'allowance':
        srcAddr = fromArg
        if not srcAddr:
            srcAddr = eth.accounts[0]
            print( 'using default account as allowance approver', file=sys.stderr )
        checkedApprover = Web3.toChecksumAddress( srcAddr )
        spender = args.to
        if not spender:
            spender = eth.accounts[0]
            print( 'using default account as allowance spender', file=sys.stderr )
        checkedSpender = Web3.toChecksumAddress( spender )
        print( 'approver:', checkedApprover, 'spender:', checkedSpender, file=sys.stderr )
        getter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        result = getter.functions.allowance( checkedApprover, checkedSpender ).call()
        decimalShift = getter.functions.decimals().call()
        #print( 'decimalShift', decimalShift )
        print( result / (10**decimalShift) )
        print( 'unshifted allowance', result )
    elif args.action == 'transfer':
        if not args.amount:  # maybe could test for  None instead
            sys.exit( 'error: no --amount passed for transfer')
        if args.amount <= 0:
            sys.exit( 'error: non-positive --amount passed for transfer')
        amount = args.amount
        logger.info( 'unshifted amount: %d', amount)
        srcAddr = fromArg
        if not srcAddr:
            srcAddr = eth.accounts[0]
            logger.info( 'using default account to transfer from' )
        checkedAddr = Web3.toChecksumAddress( srcAddr )
        destAddr = args.to
        if not destAddr:
            destAddr = eth.accounts[0]
            logger.info( 'using default account to transfer to' )
        eth.default_account = srcAddr
        setter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        try:
            tx = setter.functions.transfer( destAddr, amount ).transact({ 'gas': 100000, 'gasPrice': 1 })
        except Exception as exc:
            logger.error( 'transfer got exception (%s) %s', type(exc), exc )
            logger.info( 'you may want to try again')
            sys.exit( 1 )
        logger.info( 'waiting for receipt for transaction %s', tx.hex() )
        try:
            receipt = eth.waitForTransactionReceipt( tx )
        except Exception as exc:
            logger.warning( 'waiting for receipt for transaction %s got exception (%s) %s',
                tx.hex(), type(exc), exc )
            logger.info( 'the transaction may or may not have gone through')
            sys.exit( 1 )
        print( 'transaction hash:', receipt.transactionHash.hex() )
    elif args.action == 'transferFrom':
        if args.amount == None:
            sys.exit( 'error: no --amount passed for transferFrom')
        if args.amount <= 0:
            sys.exit( 'error: non-positive --amount passed for transferFrom')
        amount = args.amount
        srcAddr = fromArg
        if not srcAddr:
            srcAddr = eth.accounts[0]
            print( 'using default account to transfer from', file=sys.stderr )
        checkedSrc = Web3.toChecksumAddress( srcAddr )
        destAddr = args.to
        if not destAddr:
            destAddr = eth.accounts[0]
            print( 'using default account to transfer to', file=sys.stderr )
        eth.default_account = eth.accounts[0]
        print( 'from:', checkedSrc, 'to:', destAddr, 'by:', eth.default_account,
            'amount:', amount,
            file=sys.stderr )
        setter = eth.contract( address=contractAddress, abi=metacontract['abi'] )
        tx = setter.functions.transferFrom( checkedSrc, destAddr, amount ).transact(
            { 'gas': 100000, 'gasPrice': 1 }
            )
        print( 'waiting for receipt')
        receipt = eth.waitForTransactionReceipt( tx )
        #print( 'receipt', receipt )
        print( 'transaction hash:', receipt.transactionHash.hex() )
    else:
        print( args.action, 'not implemented', file=sys.stderr )
