# coding=UTF-8
import timeit
import urllib.request
import web3.eth
from web3 import Web3, HTTPProvider
from web3.eth import Eth
from tqdm import tqdm
from collections import defaultdict
import numpy as np
import os
import datetime
import sys
import time
import multiprocessing as mp
from multiprocessing import Lock
import json
import threading
import argparse
from kafka import KafkaProducer
from time import sleep
from numpy import double
import cls_pb2 as pb2
from apscheduler.schedulers.blocking import BlockingScheduler

# coin_address={'0xdac17f958d2ee523a2206206994597c13d831ec7':'USDT',
#                 '0xb8c77482e45f1f44de1745f52c74426c631bdd52':'BNB',
#                 '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984':'UNI',
#                 '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48':'USDC',
#                 '0x514910771af9ca656af840dff83e8264ecf986ca':'LINK',
#                 '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0':'MATIC',
#                 '0x2b591e99afe9f32eaa6214f7b7629768c40eeb39':'HEX',
#                 '0x4fabb145d64652a948d72533023f6e7a623c7c53':'BUSD',
#                 '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599':'WBTC',
#                 '0x63d958d765f5bd88efdbd8afd32445393b24907f':'ACA',
#                 '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9':'AAVE',
#                 '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359':'SAI',
#                 '0x6b175474e89094c44da98b954eedeac495271d0f':'DAI',
#                 '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce':'SHIB',
#                 '0x6f259637dcd74c767781e37bc6133cd6a68aa161':'HT',
#                 '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2':'MKR',
#                 '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b':'CRO',
#                 '0xc00e94cb662c3520282e6f5717214004a7f26888':'COMP',
#                 '0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3':'LEO',
#                 '0x956f47f50a910163d8bf957cf5846d573e7f87ca':'FEI'}

#获取账户类型，参数地址，连接，块号
def AccountType(address, w3, block_num):
    # w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8555'))
    # 多次尝试连接，get_code
    if address is None:
        return 'none'
    if address == '0x0000000000000000000000000000000000000000':
        return 'blackhole'
    for c3 in range(3):
        try:
            low_case_address = w3.to_checksum_address(address)
            # print(low_case_address)
            code = w3.eth.get_code(low_case_address).hex()
            # print(code)
        except Exception as e:
            if c3 == 2:  # 判断缺失
                print(block_num, '多次尝试无效，判断缺失')
            else:
                print('get_code重新尝试第', c3 + 1, '次')
        else:
            break
    if c3 == 2:
        return 'failDecode'

    if code == '0x':
        return 'normal'
    else:
        return 'contract'

# def getRate():
#     global rates
#     print("获取汇率！")
#     dic = {}
#     api_key = "JPP8TF0RINYSGJXWX738FVMHSJPL5WKC6QIMCYKD"
#     symbols = 'ETH,BNB,USDT,UNI,USDC,LINK,MATIC,HEX,BUSD,WBTC,ACA,AAVE,SAI,DAI,SHIB,HT,MKR,CRO,COMP,LEO,FEI'
#     url = 'https://data.mifengcha.com/api/v3/price?api_key=' + api_key +'&symbol='+ symbols
#     result = json.loads(urllib.request.urlopen(url).read())
#     for item in result:
#         dic[item['S']] = item['u']
#     rates = dic
#     print(dic)
#     return dic

def sendMsg(edge, producer, from_, to, timestamp):
    dt = datetime.date.today()
    topic = "ETH_"+str(dt.year)+'_'+str(dt.month).zfill(2)+'_'+str(dt.day).zfill(2)
    producer.send(topic, pb2.StreamField(timeStamp=int(timestamp),
                                  from_=from_,
                                  to=to,
                                  specialField=edge.SerializeToString(),
                                  spaceName = "ETH").SerializeToString())

def serializeAndSendEdge(dict, temp, content, timestamp, producer):
    edge = pb2.Edge(timestamp=int(timestamp),
                            _from=dict['from'],
                            to=dict['to'].lower(),
                            coin=dict['coin'],
                            value=float(dict['value']),
                            transHash=temp['hash'].hex(),
                            gasUsed=float(content['gasUsed']),
                            gasLimit=float(temp['gas']),
                            fee=float(content['gasUsed'] * temp['gasPrice']),
                            fromType=dict['fromType'],
                            toType=dict['toType'],
                            transType=dict['transType'],
                            isLoop=int('1' if dict['from'] == dict['to'] else '0'),
                            status=int(content['status']),
                            id=int(dict['id']),
                            rank=int(dict['id'] * 10e9 + timestamp))
    sendMsg(edge, producer,dict['from'],dict['to'].lower(), timestamp)

def serializeAndSendEdgeWithRate(dict, temp, content, timestamp, producer):
    edge = pb2.Edge(timestamp=int(timestamp),
                            _from=dict['from'],
                            to=dict['to'].lower(),
                            coin=dict['coin'],
                            value=float(dict['value']),
                            transHash=temp['hash'].hex(),
                            gasUsed=float(content['gasUsed']),
                            gasLimit=float(temp['gas']),
                            fee=float(content['gasUsed'] * temp['gasPrice']),
                            fromType=dict['fromType'],
                            toType=dict['toType'],
                            transType=dict['transType'],
                            isLoop=int('1' if dict['from'] == dict['to'] else '0'),
                            status=int(content['status']),
                            id=int(dict['id']),
                            rank=int(dict['id'] * 10e9 + timestamp),
                            rate=double(dict['rate']),
                            usdPrice=double(dict['usd_price']))
    sendMsg(edge, producer,dict['from'],dict['to'], timestamp)

def getEthTrans(temp, block_num, content, t,w3, timestamp, producer, id):
    dict = {}
    transType = 'ETH'
    try:
        # dict['timestamp'] = t
        dict['from'] = temp['from'].lower()
        dict['to'] = temp['to']
        fromtype = AccountType(dict['from'], w3,block_num)
        totype = AccountType(dict['to'], w3,block_num)
        if dict['to'] is None:  # 代表创建合约
            dict['to'] = content['contractAddress']
            if totype == 'normal':
                with open('./errorGetcodeAddress1.txt', 'a') as f:
                    f.write(dict['to'] + '\n')
            totype = 'contract'
            transType = 'createContract'
        elif totype == 'contract':  # 代表直接合约转账
            transType = 'toContract'
        # dict['to'] = dict['to'].lower()
        dict['coin'] = 'ETH'
        dict['value'] = temp['value']
        # dict['transHash'] = temp['hash'].hex()
        # dict['gasUsed'] = content['gasUsed']
        # dict['gaslimit'] = temp['gas']
        # dict['fee'] = content['gasUsed'] * temp['gasPrice']
        dict['fromType'] = fromtype
        dict['toType'] = totype
        dict['transType'] = transType
        # dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
        # dict['status'] = str(content['status'])
        dict['id'] = id
        # dict['rank'] = 10e9 + timestamp
        # dict['rate'] = res['ETH']
        # dict['usd_price'] = float(temp['value'])*float(res['ETH'])
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
        
    except Exception as e:
        print(e)
        print("普通交易方法异常！异常交易Hash:"+str(temp['hash'].hex()))

def getCallContract(temp, block_num, content, t, w3, timestamp, producer, id):
    dict = {}
    try:
        dict['timestamp'] = t
        dict['from'] = temp['from'].lower()
        dict['to'] = temp['to']
        fromtype = AccountType(dict['from'], w3, block_num)
        totype = AccountType(dict['to'], w3, block_num)
        if totype == 'normal':
            with open('./errorGetcodeAddress1.txt', 'a') as f:
                f.write(dict['to'] + '\n')
        totype = 'contract'
        dict['to'] = dict['to']
        dict['coin'] = 'ETH'
        dict['value'] = temp['value']
        # dict['transHash'] = temp['hash'].hex()
        # dict['gasUsed'] = content['gasUsed']
        # dict['gaslimit'] = temp['gas']
        # dict['fee'] = content['gasUsed'] * temp['gasPrice']
        if dict['to'] is None:  # 代表创建合约
            dict['to'] = content['contractAddress'].lower()
            totype = AccountType(dict['to'], w3, block_num)
            if totype == 'normal':
                with open('./errorGetcodeAddress1.txt', 'a') as f:
                    f.write(dict['to'] + '\n')
            totype = 'contract'
            transType = 'createContract'
        else:
            transType = 'callContract'
        dict['fromType'] = fromtype
        dict['toType'] = totype
        dict['transType'] = transType
        # dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
        # dict['status'] = str(content['status'])
        dict['id'] = id
        # dict['rank'] = 10e10 + timestamp
        # dict['rate'] = res['ETH']
        # dict['usd_price'] = float(temp['value'])*float(res['ETH'])
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print(e)
        print("合约方法异常！异常交易Hash:"+str(temp['hash'].hex()))

def getERC20(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        dict['timestamp'] = t
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:]).lower()
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:]).lower()
        dict['coin'] = item['address'].lower()
        if item['data'] == '0x':
            dict['value'] = '0'
        else:
            # dict['value'] = str(int(item['data'], 16))  # 16进制转换，value
            dict['value'] = str(int.from_bytes(item['data'], byteorder='big', signed=False))
        # dict['transHash'] = item['transactionHash'].hex()
        # dict['gasUsed'] = content['gasUsed']
        # dict['gaslimit'] = temp['gas']
        # dict['fee'] = content['gasUsed'] * temp['gasPrice']
        dict['fromType'] = AccountType(dict['from'], w3, block_num)
        dict['toType'] = AccountType(dict['to'], w3, block_num)
        dict['transType'] = 'ERC20'
        # dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
        # dict['status'] = str(content['status'])
        dict['id'] = id
        # dict['rank'] = id * 10e10 + timestamp
        # if dict['coin'] in coin_address:
        #     if coin_address[dict['coin']] in res:
        #         dict['rate'] = res[coin_address[dict['coin']]]
        #         dict['usd_price'] = float(temp['value'])*float(res[coin_address[dict['coin']]])
        #         serializeAndSendEdgeWithRate(dict, temp, content, timestamp, producer)
        #     else:
        #         serializeAndSendEdge(dict, temp, content, timestamp, producer)
        # else:
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print("ERC20方法异常！异常交易Hash:"+str(item['transactionHash'].hex()))

def getERC721(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        dict['timestamp'] = t
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:]).lower()
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:]).lower()
        dict['coin'] = item['address'].lower()
        dict['value'] = str(int(topics[3].hex(), 16))  # 16进制转换，value topics[3]是tokenID
        # dict['transHash'] = item['transactionHash'].hex()
        # dict['gasUsed'] = content['gasUsed']
        # dict['gaslimit'] = temp['gas']
        # dict['fee'] = content['gasUsed'] * temp['gasPrice']
        dict['fromType'] = AccountType(dict['from'], w3, block_num)
        dict['toType'] = AccountType(dict['to'], w3, block_num)
        dict['transType'] = 'ERC721'
        # dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
        # dict['status'] = str(content['status'])
        dict['id'] = id
        # dict['rank'] = id * 10e10 + timestamp
        # if dict['coin'] in coin_address:
        #     if coin_address[dict['coin']] in res:
        #         dict['rate'] = res[coin_address[dict['coin']]]
        #         dict['usd_price'] = float(temp['value'])*float(res[coin_address[dict['coin']]])
        #         serializeAndSendEdgeWithRate(dict, temp, content, timestamp, producer)
        #     else:
        #         serializeAndSendEdge(dict, temp, content, timestamp, producer)
        # else:
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print(e)
        print("ERC721方法异常！异常交易Hash:"+str(item['transactionHash'].hex()))

def parse(data, block_num, w3, producer):
    trans = data['transactions']  # 交易hash列表
    t = datetime.datetime.fromtimestamp(data['timestamp']).strftime("%Y/%m/%d, %H:%M:%S")  # 块的时间
    id = 1
    for tran in trans:
        content = None
        temp = None
        # 多次尝试连接，getTransactionReceipt，get_transaction
        for c2 in range(3):
            try:
                content = w3.eth.get_transaction_receipt(tran)
                temp = w3.eth.get_transaction(tran)
                # temp = w3.eth.get_transaction_by_block(block_num, index)
                # content = w3.eth.get_transaction_receipt(temp['hash'].hex())
            except Exception as e:
                print(e)
                if c2 != 2:  # 判断缺失
                    print('get_tran_info重新尝试第', c2 + 1, '次')
            else:
                break

        if c2 == 2:
                continue
        # ETH交易、创建合约、合约转账、（异常交易）
        if len(content['logs']) == 0:  # 无log，只取出外部交易
            getEthTrans(temp, block_num, content, t, w3, data['timestamp'], producer, id)
            id += 1
        else:
            # 加入调用合约callContract
            getCallContract(temp, block_num, content, t, w3, data['timestamp'], producer, id)
            # 内部交易
            id += 1
            for item in content['logs']:
                topics = item['topics']
                if len(topics) != 0 and \
                        topics[0].hex() == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                    if len(topics) == 3:  # 这个topics[0].hex()表示交易行为类型，topics长度为3的是ERC20合约的
                        getERC20(temp, block_num, content, t, topics, item, w3, id, data['timestamp'], producer)
                    elif len(topics) == 4:  # 这个topics[0].hex()表示交易行为类型，topics长度为4的是ERC721合约的
                        getERC721(temp, block_num, content, t, topics, item, w3, id, data['timestamp'], producer)
                    id += 1
    return id

lock = threading.Lock()

def process():
    global now
    # global rates
    w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
    while True:
        # if w3.isConnected() == False:
        #     w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
        try:
            lock.acquire()
            block_num = now
            now += 1
        finally:
            lock.release()  
        # res = rates
        producer = KafkaProducer(bootstrap_servers='10.160.196.2:30002')
        while True:
            try:
                data = w3.eth.get_block(block_num)
                break
            except Exception as e:
                time.sleep(3)
        count = parse(data, block_num, w3, producer)
        print("%s adds block_num to 1:%d,交易量:%d,时间是%d"%(threading.current_thread().getName(), data['number'], count, time.time() - int(data['timestamp'])))
        
# rates = getRate()
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--processes', type=int, default=2)
parser.add_argument('-s', '--start', type=int, default=0)
args = parser.parse_args()
if args.start == 0:
    now = w3.eth.get_block_number()
else:
    now = args.start

with open('./startNum.txt', 'a') as f:
    f.write(str(now) + '\n')
threads = [threading.Thread(name = 'process%d' %(i,), target=process) for i in range(args.processes)]
[t.start() for t in threads]
# sched = BlockingScheduler()
# sched.add_job(getRate, 'interval', hours = 1, id='getRate')
# print("-----开启定时任务-----")
# sched.start()