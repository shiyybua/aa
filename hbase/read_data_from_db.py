# coding:utf-8
from pyspark import SparkContext, SparkConf
import happybase

conf = SparkConf().setMaster("spark://cdh-master-slave1:7077").set("spark.executor.memory", "5G").set(
        "spark.driver.memory", "3G").set("spark.executor.cores", "2").set("spark.cores.max", "6")
sc = SparkContext(conf=conf)
conn = happybase.Connection('192.168.10.23')

def read_source_id():
    conn.open()
    table = conn.table('source_id')
    # get source id from hbase
    source = {}
    for data in table.scan():
        print data[0], data[1].get('abstract:id')
        source[data[0].decode('utf-8')] = data[1].get('abstract:id')

def read_test():
    conn.open()
    table = conn.table('test')
    for data in table.scan():
        print data[0]
        for k,v in data[1].items():
            print k,v
        print '-'*50

def read_specific_lines():
    conn.open()
    table = conn.table('test')
    #TODO more operations see: https://happybase.readthedocs.io/en/latest/

if __name__ == '__main__':
    read_test()
    sc.stop()





