import os
import logging.config
import pytz
import logging


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOGGING_CONF = os.environ.get("SPARK_LOGGING_CONF")
if LOGGING_CONF:
    logging.config.fileConfig(LOGGING_CONF)


logger = logging.getLogger("pyspark_app.app.nginxaccesslog")

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
GEOIP_DATABASE_HOME=os.environ.get("GEOIP_DATABASE_HOME")

