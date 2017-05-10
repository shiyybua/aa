# -*- coding: utf-8 -*-
from pyspark import SparkContext

path = "hdfs:localhost:8020/test/data.txt"
# path = "/Users/mac/Desktop/data.txt"
#创建scZZ
sc = SparkContext('local', 'app name')

def grouping():
    file = sc.textFile(path)
    # 把原文件转换成RDD键值对,key是第一个topicID，value是整个line。
    content = file.flatMap(lambda line: line.split('\n')).map(lambda content: (content.split('\t')[0], content))

    for topic, c in content.groupByKey().collect():
        print topic
        for i, result in enumerate(c):
            print result
            if i > 20:
                break

        print '-'*100




grouping()