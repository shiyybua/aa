from pyspark import SparkContext, SparkConf
import json
import happybase

def transform_from_hbase(data):
    if not data:
        return {}
    data = data.split('\n')
    tmp = {}
    for x in data:
        td = json.loads(x)
        key = '%s:%s' % (td['columnFamily'], td['qualifier'])
        tmp[key] = td['value']
    return tmp

if __name__ == '__main__':
    conf = SparkConf().setMaster("spark://cdh-master-slave1:7077").set("spark.executor.memory", "5G").set(
        "spark.driver.memory", "3G").set("spark.executor.cores", "2").set("spark.cores.max", "6")
    sc = SparkContext(conf=conf)

    host = '192.168.10.23'
    hbase_table = 'test'
    rkeyConv = "examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    rvalueConv = "examples.pythonconverters.HBaseResultToStringConverter"
    rconf = {"hbase.zookeeper.quorum": host,
             "hbase.mapreduce.inputtable": hbase_table}
    ps_data = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=rkeyConv,
        valueConverter=rvalueConv,
        conf=rconf
    )

    real_data = ps_data.map(lambda x: {x[0]: transform_from_hbase(x[1])})
    for v in real_data.collect():
        print (v)

    sc.stop()