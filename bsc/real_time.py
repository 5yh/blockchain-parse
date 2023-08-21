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
from web3.middleware import geth_poa_middleware

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
            low_case_address = w3.toChecksumAddress(address)
            code = w3.eth.get_code(low_case_address).hex()
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

def sendMsg(edge, producer, from_, to, timestamp):
    dt = datetime.date.today()
    topic = "Bnb_" + str(dt.year)+'_'+str(dt.month).zfill(2)+'_'+str(dt.day).zfill(2)
    producer.send(topic, pb2.StreamField(timeStamp=int(timestamp),
                                  from_=from_,
                                  to=to,
                                  specialField=edge.SerializeToString(),
                                  spaceName = "BNB").SerializeToString())

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


def getBNBTrans(temp, block_num, content, t,w3, timestamp, producer, id):
    dict = {}
    transType = 'bnb'
    try:
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
        dict['coin'] = 'bnb'
        dict['value'] = temp['value']
        dict['fromType'] = fromtype
        dict['toType'] = totype
        dict['transType'] = transType
        dict['id'] = id
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
        dict['coin'] = 'bnb'
        dict['value'] = temp['value']
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
        dict['id'] = id
        # dict['transHash'] = temp['hash'].hex()
        # dict['gasUsed']=float(content['gasUsed'])
        # dict['gasLimit']=float(temp['gas'])
        # dict['fee'] = float(content['gasUsed'] * temp['gasPrice'])
        # print(dict)
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print(e)
        print("合约方法异常！异常交易Hash:"+str(temp['hash'].hex()))

def getBEP20(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        dict['timestamp'] = t
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:]).lower()
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:]).lower()
        dict['coin'] = item['address'].lower()
        if item['data'] == '0x':
            dict['value'] = '0'
        else:
            dict['value'] = str(int(item['data'], 16))  # 16进制转换，value
        dict['fromType'] = AccountType(dict['from'], w3, block_num)
        dict['toType'] = AccountType(dict['to'], w3, block_num)
        dict['transType'] = 'BEP20'
        dict['id'] = id
        dict['transHash'] = temp['hash'].hex()
        dict['gasUsed']=float(content['gasUsed'])
        dict['gasLimit']=float(temp['gas'])
        dict['fee'] = float(content['gasUsed'] * temp['gasPrice'])
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print("BEP20方法异常！异常交易Hash:"+str(item['transactionHash'].hex()))

def getERC721(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        dict['timestamp'] = t
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:]).lower()
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:]).lower()
        dict['coin'] = item['address'].lower()
        dict['value'] = str(int(topics[3].hex(), 16))  # 16进制转换，value topics[3]是tokenID
        dict['fromType'] = AccountType(dict['from'], w3, block_num)
        dict['toType'] = AccountType(dict['to'], w3, block_num)
        dict['transType'] = 'ERC721'
        dict['id'] = id
        serializeAndSendEdge(dict, temp, content, timestamp, producer)
    except Exception as e:
        print(e)
        print("ERC721方法异常！异常交易Hash:"+str(item['transactionHash'].hex()))

def parse(data, block_num, w3, producer):
    trans = data['transactions']  # 交易hash列表
    t = datetime.datetime.fromtimestamp(data['timestamp']).strftime("%Y/%m/%d, %H:%M:%S")  # 块的时间
    id = 0
    for tran in trans:
        content = None
        temp = None
        # 多次尝试连接，getTransactionReceipt，get_transaction
        for c2 in range(3):
            try:
                content = w3.eth.get_transaction_receipt(tran.hex())
                temp = w3.eth.get_transaction(tran.hex())
            except Exception as e:
                if c2 != 2:  # 判断缺失
                    print('get_tran_info重新尝试第', c2 + 1, '次')
                else:
                    with open('./errorGetcodeAddress.txt', 'a') as f:
                                f.write(tran.hex() + '\n')
            else:
                break

        if c2 == 2:
                continue
        # BNB交易、创建合约、合约转账、（异常交易）
        if len(content['logs']) == 0:  # 无log，只取出外部交易
            id += 1
            getBNBTrans(temp, block_num, content, t, w3, data['timestamp'], producer, id)
        else:
            # 加入调用合约callContract
            id += 1
            getCallContract(temp, block_num, content, t, w3, data['timestamp'], producer, id)
            # 内部交易
            for item in content['logs']:
                topics = item['topics']
                if len(topics) != 0 and \
                        topics[0].hex() == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                    if len(topics) == 3:  # 这个topics[0].hex()表示交易行为类型，topics长度为3的是TRC20/21合约的
                        id += 1
                        getBEP20(temp, block_num, content, t, topics, item, w3, id, data['timestamp'], producer)
                    elif len(topics) == 4:  # 这个topics[0].hex()表示交易行为类型，topics长度为4的是TRC721合约的
                        id += 1
                    
                        getERC721(temp, block_num, content, t, topics, item, w3, id, data['timestamp'], producer)
    return id

lock = threading.Lock()

def process():
    global now
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8595'))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # 注入poa中间件
    producer = KafkaProducer(bootstrap_servers='hpc02:9092')
    while True:
        while w3.is_connected() == False:
            # w3 = Web3(Web3.IPCProvider('/pub/p5/bsc/data/geth.ipc'))
            w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8595'))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # 注入poa中间件
            print('connect failed')
            time.sleep(3)
        try:
            lock.acquire()
            block_num = now
            now += 1
        finally:
            lock.release()  
        # producer = KafkaProducer(bootstrap_servers='192.168.1.11:9092')
        while True:
            try:
                data = w3.eth.get_block(block_num)
                break
            except Exception as e:
                time.sleep(3)
        txns_num = parse(data, block_num, w3, producer)
        print("%s 解析块号block_num:%d, 交易量：%d"%(threading.current_thread().getName(), block_num, txns_num))
        

w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8595'))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # 注入poa中间件
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--processes', type=int, default=2)
parser.add_argument('-s', '--start', type=int, default=-1)
args = parser.parse_args()
if args.start == -1:
    now = w3.eth.get_block_number()
else:
    now = args.start
threads = [threading.Thread(name = 'process%d' %(i,), target=process) for i in range(args.processes)]
[t.start() for t in threads]