import py4j
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from pyspark.sql import SQLContext
from py4j.java_gateway import java_import

def register(sc):
    java_import(sc._gateway.jvm, "org.apache.spark.sql.hbase.HBaseSQLContext")

__all__ = ["HBaseSQLContext"]

class HBaseSQLContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in HBase.
    """

    def __init__(self, sparkContext):
        """Create a new HbaseContext.
    @param sparkContext: The SparkContext to wrap.
    """
        SQLContext.__init__(self, sparkContext)
        self._scala_HBaseSQLContext = self._get_hbase_ctx()

    @property
    def _ssql_ctx(self):
        if self._scala_HBaseSQLContext is None:
            print ("loading hbase context ..")
            self._scala_HBaseSQLContext = self._get_hbase_ctx()
        if self._scala_SQLContext is None:
            self._scala_SQLContext = self._scala_HBaseSQLContext
        return self._scala_HBaseSQLContext

    def _get_hbase_ctx(self):
        return self._jvm.HBaseSQLContext(self._jsc.sc())

conf = SparkConf().setMaster("spark://cdh-master-slave1:7077").set("spark.executor.memory", "5G").set(
        "spark.driver.memory", "3G").set("spark.executor.cores", "2").set("spark.cores.max", "6")
sc = SparkContext(conf=conf)

print("You are using Spark SQL on HBase!!!")
try:
    register(sc)
    hsqlContext = HBaseSQLContext(sc)
except py4j.protocol.Py4JError:
    print("HBaseSQLContext can not be instantiated, falling back to SQLContext now")
    hsqlContext = SQLContext(sc)
except TypeError:
    print("HBaseSQLContext can not be instantiated, falling back to SQLContext now")
    hsqlContext = SQLContext(sc)

print("%s available as hsqlContext." % hsqlContext.__class__.__name__)