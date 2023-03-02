from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext

from .. import settings

def get_spark_session():

    conf = SparkConf()
    for k,v in settings.SPARK_CONF.items():
        conf.set(k,v)

    context = SparkContext(conf=conf)

    return SparkSession(context)

