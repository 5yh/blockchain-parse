# coding=UTF-8
import pandas as pd
from web3 import Web3, HTTPProvider
import datetime
import sys
import time
import threading
import argparse
from kafka import KafkaProducer
from numpy import double
import tron_pb2 as pb2
import requests
import tronpy
import os

def init_coin():
    dict = {}
    f = open("./address_to_coin.txt", "r")
    line = f.readline()
    while (line):
        line = ''.join(line).strip('\n')
        list = line.split(',')
        if len(list) < 2: continue  # 目的处理问题行
        dict[list[0]] = list[1]
        line = f.readline()
    f.close()
    return dict


def get_tron_address(data):
    return tronpy.keys.to_base58check_address(data)


def get_coin(addr):
    global dict_coin
    if addr in dict_coin:
        return dict_coin[addr]
    else:
        url = "https://apilist.tronscanapi.com/api/token_trc20?contract=" + str(addr) + "&showAll=1&start=&limit="
        response = requests.get(url, timeout=30)
        data = response.json()
        tmp = data["trc20_tokens"]
        info = tmp[0]
        symbol = info['symbol']
        dict_coin[addr] = symbol
        with open("./address_to_coin.txt", "a") as f:
            f.write(addr + "," + symbol + '\n')
    return dict_coin[addr]


# 获取账户类型，参数地址，连接，块号
def AccountType(address, w3, block_num):
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:50545/jsonrpc'))
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

def sendMsg(edge, producer, from_, to, timestamp):
    dt = datetime.date.today()
    topic = "TRON_retest"
    producer.send(topic, pb2.StreamField(timeStamp=int(timestamp),
                                         from_=from_,
                                         to=to,
                                         specialField=edge.SerializeToString(),
                                         spaceName="TRX").SerializeToString())


def serializeAndSendEdge(dict, timestamp, producer):
    if dict['status'] == 1:
        dict['status'] = "SUCCESS"
    else:
        dict['status'] = "FAILED"
    # print(dict)
    edge = pb2.Edge(transHash=str(dict['transHash']),
                            block=int(dict['block']),
                            coin=str(dict['coin']),
                            value=float(dict['value']),
                            to=str(dict['to']),
                            _from=str(dict['from']),
                            timestamp=int(dict['timestamp']),
                            id=int(dict['id']),
                            status=str(dict['status']),
                            )
    # print(edge)
    sendMsg(edge, producer,dict['from'],dict['to'], timestamp)


def getEthTrans(temp, block_num, content, t, w3, timestamp, producer, id):
    dict = {}
    try:
        dict['to'] = temp['to']
        if dict['to'] is None:  # 代表创建合约
            return
        dict['transHash'] = temp['hash'].hex()
        dict['transHash'] = dict['transHash'][2:]
        dict['block'] = block_num
        dict['coin'] = 'TRX'
        dict['value'] = str(temp['value'])
        dict['from'] = temp['from'].lower()
        dict['from'] = get_tron_address(dict['from'])
        totype = AccountType(dict['to'], w3, block_num)
        if totype == 'contract':  # 代表直接合约转账
            dict['to'] = dict['to']
            transType = 'toContract'
        dict['to'] = get_tron_address(dict['to'])
        dict['timestamp'] = t
        dict['id'] = id + 1e3 * t
        dict['status'] = content['status']
        serializeAndSendEdge(dict, timestamp, producer)

    except Exception as e:
        print(e)
        print("普通交易方法异常！异常交易Hash:" + str(temp['hash'].hex()))


def getCallContract(temp, block_num, content, t, w3, timestamp, producer, id):
    dict = {}
    try:
        dict['from'] = temp['from'].lower()
        dict['to'] = temp['to']
        totype = AccountType(dict['to'], w3, block_num)
        if totype == 'normal':
            with open('./errorGetcodeAddress1.txt', 'a') as f:
                f.write(dict['to'] + '\n')
        dict['value'] = temp['value']
        if dict['to'] is None:  # 代表创建合约
            dict['to'] = content['contractAddress'].lower()
            totype = AccountType(dict['to'], w3, block_num)
            if totype == 'normal':
                with open('./errorGetcodeAddress1.txt', 'a') as f:
                    f.write(dict['to'] + '\n')
        else:
            if int(dict['value']) == 0: return id
            dict['to'] = get_tron_address(dict['to'])
        dict['block'] = block_num
        dict['from'] = get_tron_address(dict['from'])
        dict['coin'] = 'TRX'
        dict['transHash'] = temp['hash'].hex()
        dict['transHash'] = dict['transHash'][2:]
        dict['timestamp'] = t
        dict['id'] = id + 1e3 * t
        dict['status'] = content['status']
        serializeAndSendEdge(dict, timestamp, producer)
        return int(id) + 1
    except Exception as e:
        print(e)
        print("合约方法异常！异常交易Hash:" + str(temp['hash'].hex()))


def getERC20(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        if item['data'] == '0x':
            return
        else:
            dict['value'] = str(int.from_bytes(item['data'], byteorder='big', signed=False))
        if dict['value']==0:
            return
        dict['transHash'] = item['transactionHash'].hex()
        dict['transHash'] = dict['transHash'][2:]
        dict['block'] = block_num
        dict['coin'] = item['address'].lower()
        dict['coin'] = get_tron_address(dict['coin'])
        dict['coin'] = get_coin(dict['coin'])
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:])
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:])
        dict['to'] = get_tron_address(dict['to'])
        dict['from'] = get_tron_address(dict['from'])
        dict['timestamp'] = t
        dict['id'] = id + 1e3 * t
        dict['status'] = content['status']

        serializeAndSendEdge(dict, timestamp, producer)
    except Exception as e:
        print("ERC20方法异常！异常交易Hash:" + str(item['transactionHash'].hex()))


def getERC721(temp, block_num, content, t, topics, item, w3, id, timestamp, producer):
    dict = {}
    try:
        dict['value'] = str(int(topics[3].hex(), 16))  # 16进制转换，value topics[3]是tokenID
        dict['transHash'] = item['transactionHash'].hex()
        dict['transHash'] = dict['transHash'][2:]
        dict['block'] = block_num
        dict['coin'] = item['address'].lower()
        dict['coin'] = get_tron_address(dict['coin'])
        dict['coin'] = get_coin(dict['coin'])
        dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:])
        dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:])
        dict['to'] = get_tron_address(dict['to'])
        dict['from'] = get_tron_address(dict['from'])
        dict['timestamp'] = t
        dict['id'] = id + 1e3 * t
        dict['status'] = content['status']

        serializeAndSendEdge(dict, timestamp, producer)
    except Exception as e:
        print(e)
        print("ERC721方法异常！异常交易Hash:" + str(item['transactionHash'].hex()))


def parse(data, block_num, w3, producer):
    trans = data['transactions']  # 交易hash列表
    t = data['timestamp']
    # t = datetime.datetime.fromtimestamp(data['timestamp']).strftime("%Y/%m/%d, %H:%M:%S")  # 块的时间
    id = 1
    for tran in trans:
        content = None
        temp = None
        # 多次尝试连接，getTransactionReceipt，get_transaction
        for c2 in range(3):
            try:
                content = w3.eth.get_transaction_receipt(tran)
                temp = w3.eth.get_transaction(tran)
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
            if temp['value'] != 0:
                getEthTrans(temp, block_num, content, t, w3, data['timestamp'], producer, id)
            id += 1
        else:
            # 加入调用合约callContract
            if temp['value'] != 0:
                id = getCallContract(temp, block_num, content, t, w3, data['timestamp'], producer, id)
            # 内部交易
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
    global blockLimit
    global transCount
    global beginTime
    # global rates
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:50545/jsonrpc'))
    while True:
        try:
            lock.acquire()
            # if now >= blockLimit:
            #     with open('./lastNum.txt', 'w') as f:
            #         f.write(str(blockLimit))
            #     sys.exit()
            block_num = now
            now += 1
            time_now = time.time()
        finally:
            lock.release()
        producer = KafkaProducer(
            bootstrap_servers='10.160.196.23:30812',
                         security_protocol='SASL_PLAINTEXT',
                         sasl_mechanism='SCRAM-SHA-512',
                         sasl_plain_username='admin',
                         sasl_plain_password='starryJff6%2ND',
                         api_version=(2,8,2),
        )
        while True:
            try:
                data = w3.eth.get_block(block_num)
                break
            except Exception as e:
                time.sleep(3)
        count = parse(data, block_num, w3, producer) - 1
        transCount = transCount + count
        tmp = time.time() - beginTime
        print("%s adds block_num to 1:%d,累计交易量:%d,当前区块解析发送用时%d秒,程序累计运行时间%d时%d分%d秒" % (
        threading.current_thread().getName(), data['number'], transCount, time.time() - time_now, tmp / 3600,
        (tmp % 3600) / 60, tmp % 60))
#        if transCount >10000:
#            sys.exit()

w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:50545/jsonrpc'))
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--processes', type=int, default=2)
parser.add_argument('-s', '--start', type=int, default=0)
args = parser.parse_args()

if args.start == 0:
    now = w3.eth.get_block_number()
else:
    now = args.start
# blockLimit = int(input("输入需要推的区块数量:")) + now
transCount = 0
beginTime = time.time()
dict_coin = init_coin()

with open('./startNum.txt', 'a') as f:
    f.write(str(now) + '\n')
threads = [threading.Thread(name='process%d' % (i,), target=process) for i in range(args.processes)]
[t.start() for t in threads]
# sched = BlockingScheduler()
# sched.add_job(getRate, 'interval', hours = 1, id='getRate')
# print("-----开启定时任务-----")
# sched.start()
