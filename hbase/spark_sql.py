from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("spark://cdh-master-slave1:7077").set("spark.executor.memory", "5G").set(
        "spark.driver.memory", "3G").set("spark.executor.cores", "2").set("spark.cores.max", "6")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)

data_source_format = 'org.apache.hadoop.hbase.spark'

df = sc.parallelize([('a', '1.0'), ('b', '2.0')]).toDF(schema=['col0', 'col1'])

# ''.join(string.split()) in order to write a multi-line JSON string here.
catalog = ''.join("""{
    "table":{"namespace":"default", "name":"test"},
    "rowkey":"key",
    "columns":{
        "col0":{"cf":"rowkey", "col":"key", "type":"string"},
        "col1":{"cf":"cf", "col":"col1", "type":"string"}
    }
}""".split())


# Writing
df.write.options(catalog=catalog).format(data_source_format).save()

# Reading
df = sqlc.read.options(catalog=catalog).format(data_source_format).load()