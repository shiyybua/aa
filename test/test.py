# -*- coding: utf-8 -*-
"""SimpleApp.py"""
from pyspark import SparkContext
from operator import add

def simplest():
    logFile = "/Users/mac/software/spark-2.1/README.md"  # Should be some file on your system
    sc = SparkContext("local", "Simple App")
    logData = sc.textFile(logFile).cache()

    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


def word_count():
    def tokenize(text):
        return text.split(' ')
    path = "/Users/mac/software/spark-2.1/README.md"
    sc = SparkContext()
    text = sc.textFile(path)
    # collect 返回文本的所以内容，以数组的形式，每行一个元素。
    print text.collect()
    words = text.flatMap(tokenize)
    wc = words.map(lambda x: (x, 1))
    counts = wc.reduceByKey(add)
    print counts.collect()

def parallelize_show():
    sc = SparkContext()
    x = sc.parallelize([('a',1), ('b',2), ('a',3)])
    def add(a,b):
        return a + str(b)

    print x.combineByKey(str, add, add).collect()
parallelize_show()