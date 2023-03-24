from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext

from .. import settings

def get_spark_session():
    if not settings.SPARK_CONF:
        raise Exception("Please configure the spark configuration via env 'SPARK_CONF'")

    conf = SparkConf()
    for k,v in settings.SPARK_CONF.items():
        conf.set(k,v)

    context = SparkContext(conf=conf)

    return SparkSession(context)

