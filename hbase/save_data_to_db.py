# coding:utf-8
from pyspark import SparkContext, SparkConf
import happybase

conf = SparkConf().setMaster("spark://cdh-master-slave1:7077").set("spark.executor.memory", "5G").set(
        "spark.driver.memory", "3G").set("spark.executor.cores", "2").set("spark.cores.max", "6")
sc = SparkContext(conf=conf)
host = '192.168.10.23'
hbase_table = 'tmp'
conf = {"hbase.zookeeper.quorum": host,
        "hbase.mapred.outputtable": hbase_table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
        }
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
dl= [['sparkrow1', 'cf', 'a', 'value1']]
sc.parallelize(dl).map(lambda x: (x[0], x))\
  .saveAsNewAPIHadoopDataset(conf, keyConv, valueConv)