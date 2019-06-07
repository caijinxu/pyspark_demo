#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import division
from collections import Counter
from pyspark import SparkContext,SparkConf,StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from urlconduct import *
from datacmpute import datacomputclass
from stockidtoname import stockidtoname
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def split_k_v(data):
    data1 = json.loads(data[1].encode('utf8'))
    if data1['database'] == 'alog' and data1['table'] == 'userlog' and data1['type'] == 'insert':
        vdict = dict()
        key = ''
        if data1['data']["username"] != '0':
            key = data1['data']["username"]
        elif data1['data']['global_unique'] != '':
            key = data1['data']['global_unique']

        vdict['global_uniquelist'] = dict()
        vdict['global_uniquelist'][data1['data']["global_unique"]] = str(int(time.mktime(time.strptime(data1['data']['starttime'],'%Y-%m-%d %H:%M:%S'))))
        vdict['url'] = []
        vdict['url'].append(data1['data']['url'][::-1])
        vdict['time'] = str(int(time.mktime(time.strptime(data1['data']['starttime'],'%Y-%m-%d %H:%M:%S'))))
        if key != '':
            return (key, vdict)


def fildat(data):
    if data != 'None':
        return data


def convale(x,y):
    result = {}
    for key, val in y['global_uniquelist'].items():
        if x['global_uniquelist'].has_key(key):
            if val > x['global_uniquelist'][key]:
                x['global_uniquelist'][key] = val
        else:
            x['global_uniquelist'][key] = val
    result['global_uniquelist'] =  x['global_uniquelist']
    result['url'] = []
    for url in x["url"]:
        result['url'].append(url)
    for url in y["url"]:
        result['url'].append(url)
    result['time'] = x['time'] + ',' + y['time']
    return result


def data_integration(data):
    result = {}
    result['stockid'] = []
    result['source'] = []
    result['channelnamelist'] = []
    result['keywordlist'] = []
    result['autheorlist'] = []
    if data[1]['urlinfo']:
        for key in data[1]['urlinfo'].keys():
            if key == 'stockids':
                tmp = data[1]['urlinfo'][key].split(',')
                stocknumlist = [x for x in tmp if x != '']
                # 将股票代码转为股票名称
                for stid in stocknumlist:
                    try:
                        stockname = stockidtoname[stid].decode("unicode_escape")
                    except:
                        stockname = stid
                    result['stockid'].append(stockname)
            else:
                tmp = data[1]['urlinfo'][key].split(',')
                if key == 'keyword':
                    result['keywordlist'] = [x for x in tmp if x != '']
                if key == 'author':
                    result['autheorlist'] = [x for x in tmp if x != '']
                if key == 'channelname':
                    result['channelnamelist'] = [x for x in tmp if x != '']
                if key == ['source']:
                    result['source'] = [x for x in tmp if x != '']

    strtimelist = data[1]['time'].split(',')
    result['time'] = []
    for t in strtimelist:
        t = int(t)
        result['time'].append(t)
    result['time'].sort()
    result['global_uniquelist'] = data[1]['global_uniquelist']
    return (data[0], result)


conf = SparkConf().setAppName("newPythonStreamingKafkatest").setMaster('yarn')
conf.set("spark.dynamicAllocation.enabled", "true")
conf.set("spark.dynamicAllocation.maxExecutors", "4")
conf.set("spark.executor.cores", "6")
conf.set("spark.executor.memory", "1500m")
conf.set("spark.default.parallelism", "192")


sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,60)
topic = "maxwell"
kvs = KafkaUtils.createStream(ssc, "172.30.2.145:2181", 'rta', {topic:1})


lines_rdd2 = kvs.map(split_k_v).filter(fildat)\
    .combineByKey(lambda x:x,convale,convale)\
    .map(datacomputclass.getuid)\
    .map(urlconduct.urltoinfo).map(datacomputclass.hbasecompute)\
    .foreachRDD(lambda x:x.foreach(datacomputclass.upresultdb))

ssc.start()
ssc.awaitTermination()




