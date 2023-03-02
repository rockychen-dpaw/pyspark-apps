import os
import logging.config
import pytz


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.config.fileConfig('{}/logging.conf'.format(BASE_DIR))

SPARK_MASTER = "spark://master.spark.dbca.wa.gov.au:7077"
DEPLOY_MODE =  "client"
SPARK_CONF = {
    "spark.master":"spark://master.spark.dbca.wa.gov.au:7077",
    "spark.submit.deployMode":"client",
    "spark.driver.cores":1,
    "spark.driver.memory":"100m",
    "spark.executor.memory":"450m",
    "spark.executor.pyspark.memory":"800m"
}

TIMEZONE = pytz.timezone("Australia/Perth")

