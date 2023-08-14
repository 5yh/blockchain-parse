# coding=UTF-8
from web3 import Web3, HTTPProvider
from web3.eth import Eth
from tqdm import tqdm
from collections import defaultdict
import pickle
import numpy as np
import os
import datetime
import sys
import pandas as pd
import time
import multiprocessing as mp
from multiprocessing import Lock
import argparse

if 'http_proxy' in os.environ.keys():
    os.environ.pop('http_proxy')  # 关闭服务器上的代理环境变量
    # del os.environ['http_proxy']


# 'normal' 普通账户
# 'contract' 合约账户
# 'none' 为空，一般不会出现
# 'blackhole' '0x0000000000000000000000000000000000000000'
# failDecode 解析失败，一般不会出现
def AccountType(address, w3, p_num, block_num, total_p):
    # 多次尝试连接，get_code
    if address is None:
        return 'none'
    if address == '0x0000000000000000000000000000000000000000':
        return 'blackhole'
    for c3 in range(3):
        try:
            low_case_address = w3.to_checksum_address(address)
            code = w3.eth.get_code(low_case_address).hex()
        except Exception as e:
            if c3 == 2:  # 判断缺失
                print(total_p * '\n', '>>进程' + str(p_num), block_num, '多次尝试无效，判断缺失')
                # while True:
                #     try:
                #         # w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8555'))
                #         w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8555'))
                #         low_case_address = w3.toChecksumAddress(address)
                #         code = w3.eth.get_code(low_case_address).hex()
                #     except Exception as e:
                #         print('>>进程' + str(p_num), block_num, '重新连接中')
                #         time.sleep(60)
                #     else:
                #         print('>>进程' + str(p_num), block_num, '重新连接成功')
                #         break
                q.put(address + ' ' + 'get_code_error')
            else:
                print(total_p * '\n', '>>进程' + str(p_num), e, e.args, '\n', 'get_code重新尝试第', c3 + 1, '次')
        else:
            break
    if c3 == 2:
        return 'failDecode'

    if code == '0x':
        return 'normal'
    else:
        return 'contract'


# 交易分类
# 以太币交易
# ERC20代币交易
# ERC721交易
# 直接合约转账
# 创建合约（非交易）
# 调用合约（非交易）
# 异常交易
# 额外判断是否是自环交易


# def TransType(flag):
#     flagType = ['ETH', 'ERC20', 'ERC721', 'createContract', 'toContract', 'callContract']
#     res = flagType[flag]
#
#     return res


def parse(p_num, start, end, file, total_p, q, q_count,port):
    # [start,end]
    # w3 = Web3(Web3.IPCProvider(ipc_location))
    w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
    # w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8555'))
    # w3 = Web3(Web3.HTTPProvider('ws://127.0.0.1:'+port))
    aaaaaaa=w3.eth.get_block_number()
    print(aaaaaaa)#26308866
    # print('进程' + str(p_num), '连接状态:', w3.isConnected(), '保存位置文件位置:', file)
    if os.path.exists(file):
        os.remove(file)
        print('进程' + str(p_num), '已删除原有文件' + file)

    index = list(np.linspace(start, end, 100 + 1, dtype=int))  # 分成100份,1%增量保存一次
    i = 1
    count = 0
    trans_count = 0
    trans_count_all = 0
    st = []
    t0 = time.time()
    time.sleep(10 + p_num)
    for block_num in tqdm(range(start, end + 1), position=p_num, desc='进程' + str(p_num) + ' ' + str([start, end]),
                          colour='green'):
        data = None
        # 多次尝试连接，get_block
        for c1 in range(3):
            try:
                data = w3.eth.get_block(block_num)
            except Exception as e:
                if c1 == 2:  # 判断缺失
                    print(total_p * '\n', '>>进程' + str(p_num), block_num, '多次尝试无效，判断缺失')
                    # while True:
                    #     try:
                    #         # w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8555'))
                    #         w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8555'))
                    #         data = w3.eth.get_block(block_num)
                    #     except Exception as e:
                    #         print('>>进程' + str(p_num), block_num, e,'重新连接中')
                    #         time.sleep(60)
                    #     else:
                    #         print('>>进程' + str(p_num), block_num, '重新连接成功')
                    #         break
                    q.put(str(block_num) + ' ' + 'get_block_error')
                else:
                    print(total_p * '\n', '>>进程' + str(p_num), e, e.args, '\n', 'get_block重新尝试第', c1 + 1, '次')
            else:
                break

        if c1 == 2:
            continue

        # 获取交易和时间
        trans = data['transactions']  # 交易hash列表
        q_count.put(len(trans) + q_count.get())
        trans_count += len(trans)
        trans_count_all += len(trans)
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
                except Exception as e:
                    if c2 == 2:  # 判断缺失
                        print(total_p * '\n', '>>进程' + str(p_num), block_num, '多次尝试无效，判断缺失')
                        # while True:
                        #     try:
                        #         # w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8555'))
                        #         w3 = Web3(Web3.WebsocketProvider('ws://127.0.0.1:8555'))
                        #         content = w3.eth.getTransactionReceipt(tran)
                        #         temp = w3.eth.get_transaction(tran)
                        #     except Exception as e:
                        #         print('>>进程' + str(p_num), block_num, e, '重新连接中')
                        #         time.sleep(60)
                        #     else:
                        #         print('>>进程' + str(p_num), block_num, '重新连接成功')
                        #         break
                        q.put(str(block_num) + ' ' + tran.hex() + ' ' + 'get_tran_info_error')
                    else:
                        print(total_p * '\n', '>>进程' + str(p_num), e, e.args, '\n', 'get_tran_info重新尝试第', c2 + 1, '次')
                else:
                    break

            if c2 == 2:
                continue

            # ETH交易、创建合约、合约转账、（异常交易）
            if len(content['logs']) == 0:  # 无log，只取出外部交易
                dict = {}
                count += 1
                transType = 'ETH'
                try:
                    dict['timestamp'] = t
                    dict['from'] = temp['from'].lower()
                    dict['to'] = temp['to']
                    fromtype = AccountType(dict['from'], w3, p_num, block_num, total_p)
                    totype = AccountType(dict['to'], w3, p_num, block_num, total_p)
                    if dict['to'] is None:  # 代表创建合约
                        dict['to'] = content['contractAddress'].lower()
                        if totype == 'normal':
                            with open('./errorGetcodeAddress3.txt', 'a') as f:
                                f.write(dict['to'] + '\n')
                        totype = 'contract'
                        transType = 'createContract'
                        count -= 1
                    elif totype == 'contract':  # 代表直接合约转账
                        dict['to'] = dict['to'].lower()
                        transType = 'toContract'
                    dict['coin'] = 'eth'
                    dict['value'] = str(temp['value'])
                    dict['transHash'] = temp['hash'].hex()
                    dict['gasUsed'] = content['gasUsed']
                    dict['gaslimit'] = temp['gas']
                    dict['fee'] = content['gasUsed'] * temp['gasPrice']
                    dict['fromType'] = fromtype
                    dict['toType'] = totype
                    dict['transType'] = transType
                    dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
                    dict['status'] = str(content['status'])
                    dict['id'] = id
                    dict['rank']=int(id * 10e9 + data['timestamp'])
                    id += 1
                    st.append(dict)
                except Exception as e:
                    print(e)
                    print(temp['from'], temp['to'], temp['hash'].hex())
                    q.put(str(block_num) + ' ' + tran.hex() + ' ' + '001_trans_parse_error')
                    sys.exit()
            else:
                # 加入调用合约callContract
                dict = {}
                try:
                    dict['timestamp'] = t
                    dict['from'] = temp['from'].lower()
                    dict['to'] = temp['to']
                    fromtype = AccountType(dict['from'], w3, p_num, block_num, total_p)
                    totype = AccountType(dict['to'], w3, p_num, block_num, total_p)
                    if totype == 'normal':
                        with open('./errorGetcodeAddress3.txt', 'a') as f:
                            f.write(dict['to'] + '\n')
                    totype = 'contract'
                    dict['coin'] = 'eth'
                    dict['value'] = str(temp['value'])
                    dict['transHash'] = temp['hash'].hex()
                    dict['gasUsed'] = content['gasUsed']
                    dict['gaslimit'] = temp['gas']
                    dict['fee'] = content['gasUsed'] * temp['gasPrice']
                    if dict['to'] is None:  # 代表创建合约
                        dict['to'] = content['contractAddress'].lower()
                        totype = AccountType(dict['to'], w3, p_num, block_num, total_p)
                        if totype == 'normal':
                            with open('./errorGetcodeAddress3.txt', 'a') as f:
                                f.write(dict['to'] + '\n')
                        totype = 'contract'
                        transType = 'createContract'
                    else:
                        dict['to'] =dict['to'].lower()
                        transType = 'callContract'
                    dict['fromType'] = fromtype
                    dict['toType'] = totype
                    dict['transType'] = transType
                    dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
                    dict['status'] = str(content['status'])
                    dict['id'] = id
                    dict['rank']=int(id * 10e9 + data['timestamp'])
                    id += 1
                    st.append(dict)
                except Exception as e:
                    print(e)
                    print(temp['from'], temp['to'], temp['hash'].hex())
                    q.put(str(block_num) + ' ' + tran.hex() + ' ' + '002_trans_parse_error')
                    sys.exit()

                # 内部交易
                for item in content['logs']:
                    topics = item['topics']
                    if len(topics) != 0 and \
                            topics[0].hex() == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                        if len(topics) == 3:  # 这个topics[0].hex()表示交易行为类型，topics长度为3的是ERC20合约的
                            count += 1
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
                                    dict['value'] = str(int.from_bytes(item['data'], byteorder='big', signed=False))  # 16进制转换，value
                                dict['transHash'] = item['transactionHash'].hex()
                                dict['gasUsed'] = content['gasUsed']
                                dict['gaslimit'] = temp['gas']
                                dict['fee'] = content['gasUsed'] * temp['gasPrice']
                                dict['fromType'] = AccountType(dict['from'], w3, p_num, block_num, total_p)
                                dict['toType'] = AccountType(dict['to'], w3, p_num, block_num, total_p)
                                dict['transType'] = 'ERC20'
                                dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
                                dict['status'] = str(content['status'])
                                dict['id'] = id
                                dict['rank']=int(id * 10e9 + data['timestamp'])
                                id += 1
                                st.append(dict)

                            except Exception as e:
                                print(e)
                                print(item['data'], item['address'], item['topics'], item['transactionHash'].hex())
                                q.put(str(block_num) + ' ' + tran.hex() + ' ' + '003_trans_parse_error')
                                sys.exit()

                        elif len(topics) == 4:  # 这个topics[0].hex()表示交易行为类型，topics长度为4的是ERC721合约的
                            count += 1
                            dict = {}
                            try:
                                dict['timestamp'] = t
                                dict['from'] = (topics[1].hex()[:2] + topics[1].hex()[26:]).lower()
                                dict['to'] = (topics[2].hex()[:2] + topics[2].hex()[26:]).lower()
                                dict['coin'] = item['address'].lower()
                                dict['value'] = str(int(topics[3].hex(), 16))  # 16进制转换，value topics[3]是tokenID
                                dict['transHash'] = item['transactionHash'].hex()
                                dict['gasUsed'] = content['gasUsed']
                                dict['gaslimit'] = temp['gas']
                                dict['fee'] = content['gasUsed'] * temp['gasPrice']
                                dict['fromType'] = AccountType(dict['from'], w3, p_num, block_num, total_p)
                                dict['toType'] = AccountType(dict['to'], w3, p_num, block_num, total_p)
                                dict['transType'] = 'ERC721'
                                dict['isLoop'] = '1' if dict['from'] == dict['to'] else '0'
                                dict['status'] = str(content['status'])
                                dict['id'] = id
                                dict['rank']=int(id * 10e9 + data['timestamp'])
                                id += 1
                                st.append(dict)

                            except Exception as e:
                                print(e)
                                print(item['data'], item['address'], item['topics'], item['transactionHash'].hex())
                                q.put(str(block_num) + ' ' + tran.hex() + ' ' + '004_trans_parse_error')
                                sys.exit()

        if block_num == index[i]:
            df = pd.DataFrame(st)
            if os.path.exists(file):
                df.to_csv(file, mode='a', index=False, header=False)
                print(total_p * '\n',
                      '>>进程' + str(p_num), '已保存入:' + file, index[i - 1] + 1, '--', index[i], '\n',
                      '>>交易数量:', trans_count, '\t内外交易数量:', count, '\t耗时:', str(round(time.time() - t0, 2)) + 's')

            else:
                df.to_csv(file, index=False)
                print(total_p * '\n',
                      '>>进程' + str(p_num), '已保存入:' + file, index[0], '--', index[i], '\n',
                      '>>交易数量:', trans_count, '\t内外交易数量:', count, '\t耗时:', str(round(time.time() - t0, 2)) + 's')

            count = 0
            trans_count = 0
            st = []
            i += 1
            t0 = time.time()

    print(total_p * '\n',
          '>>进程' + str(p_num), '已完成', '\t总交易数量:', trans_count_all)


if __name__ == '__main__':
    t1 = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', type=int, default=2)
    parser.add_argument('-f', '--fromBlock', type=int, default=0)  # 13594000
    parser.add_argument('-t', '--toBlock', type=int, default=19999)  # 13598000
    # parser.add_argument('-i', '--ipc', type=str, default="/home/bangsun/research/linyj/BlockChain/chaindata/geth.ipc")
    parser.add_argument('-d', '--data', type=str, default="/mnt/blockchain03/ethParse/")  # /pub/p3/test/
    parser.add_argument('-p', '--port', type=str, default="8545")  # /pub/p3/test/
    args = parser.parse_args()
    print('设置进程数:', args.num)
    # 进程数
    p_num = args.num
    f1 = args.fromBlock
    f2 = args.toBlock
    database = args.data
    index = list(np.linspace(f1, f2, p_num + 1, dtype=int))
    p_list = []
    # lock = Lock()
    q = mp.Queue()
    q_count = mp.Queue()
    q_count.put(0)
    for i in range(p_num):
        if i == 0:
            p_list.append(mp.Process(target=parse, args=(i, index[i], index[i + 1], database + 'erigon_parse_' +
                                                         str(index[i]) + '_' + str(index[i + 1]) + '.csv',
                                                         p_num, q, q_count,args.port)))
        else:
            p_list.append(
                mp.Process(target=parse, args=(i, index[i] + 1, index[i + 1], database + 'erigon_parse_' +
                                               str(index[i] + 1) + '_' + str(index[i + 1]) + '.csv',
                                               p_num, q, q_count,args.port)))

    for i in range(p_num):
        p_list[i].start()

    for i in range(p_num):
        p_list[i].join()

    t2 = time.time()
    t_all = round(t2 - t1, 2)
    print('已结束', '耗时:', t_all)

    while not q.empty():
        error = q.get()
        print('error:', error, '已保存入./error1.txt')
        with open('./error1.txt', 'a') as f:
            f.write(error + '\n')
    count_all = 0
    while not q_count.empty():
        count_all += q_count.get()
        print('1')
    print("speed:", count_all / t_all)
    with open('./speed.txt', 'a') as f:
        f.write(str(f1) + ' ' + str(f2) + ' : ' + str(count_all / t_all) + '\n')
