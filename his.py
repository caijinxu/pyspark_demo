#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
from pyspark import SparkContext,SparkConf,StorageLevel
from urlconduct import *
from datacmpute import datacomputclass
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def split_k_v(data):
    vdict = dict()
    key = ''
    if data["username"] != '0':
        key = data["username"]
    elif data['global_unique'] != '':
        key = data['global_unique']

    vdict['global_uniquelist'] = dict()
    vdict['global_uniquelist'][data["global_unique"]]= str(data["UNIX_TIMESTAMP(starttime)"])
    vdict['url'] = data['url'][::-1]
    vdict['time'] = str(data["UNIX_TIMESTAMP(starttime)"])
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
    result['global_uniquelist'] = x['global_uniquelist']
    result['url'] = []
    for url in x["url"]:
        result['url'].append(url)
    for url in y["url"]:
        result['url'].append(url)
    result['time'] = x['time'] + ',' + y['time']
    return result


conf = SparkConf().setAppName("newPythonStreamingKafkatest").setMaster('yarn')
conf.set("spark.dynamicAllocation.enabled", "true")
conf.set("spark.dynamicAllocation.maxExecutors", "10")
conf.set("spark.executor.cores", "8")
conf.set("spark.executor.memory", "2g")
conf.set("spark.default.parallelism", "60000")


sc = SparkContext(conf=conf)
log_path = '/alog/uptest/history.txt'
sc.setLogLevel("WARN")

lines_rdd = sc.textFile(log_path).repartition(6000).map(json.loads)
lines_rdd2 = lines_rdd.map(split_k_v).filter(fildat).combineByKey(lambda x:x,convale,convale)
lines_rdd3 = lines_rdd2.map(datacomputclass.getuid).map(urlconduct.urltoinfo)
lines_rdd4 = lines_rdd3.map(datacomputclass.hbasecompute).foreach(datacomputclass.upresultdb)




