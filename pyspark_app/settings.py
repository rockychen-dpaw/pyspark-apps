import os
import logging.config
import pytz
import logging
import json


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOGGING_CONF = os.environ.get("LOGGING_CONF")
if LOGGING_CONF:
    logging.config.fileConfig(LOGGING_CONF)


logger = logging.getLogger("pyspark_app.app.nginxaccesslog")

if os.environ.get("SPARK_CONF"):
    try:
        with open(os.environ.get("SPARK_CONF")) as f:
            SPARK_CONF = json.loads(f.read())
    except Exception as ex:
        raise Exception("Failed to load spark configuration.{}".format(str(ex)))
else:
    SPARK_CONF = None
 

TIMEZONE = pytz.timezone("Australia/Perth")
GEOIP_DATABASE_HOME=os.environ.get("GEOIP_DATABASE_HOME")

