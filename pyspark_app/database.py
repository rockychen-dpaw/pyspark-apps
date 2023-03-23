import logging
import traceback
import atexit
import re

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from . import utils

logger = logging.getLogger(__name__)

class Database(object):
    inited = False
    _DATABASES = {}
    DATABASE_URL_RE = re.compile("^(?P<protocol>[a-zA-Z0-9\-_]+)://(?P<user>[a-zA-Z0-9\-_]+):(?P<password>[^@]+)@(?P<host>[^/:]+)(:(?P<port>[0-9]+))?/(?P<db>[a-zA-Z0-9\-_]+)$")
    def __new__(cls, databaseurl):
        if utils.get_processid() not in cls._DATABASES:
            cls._DATABASES[utils.get_processid()] = {}
        if databaseurl in cls._DATABASES[utils.get_processid()]:
            return cls._DATABASES[utils.get_processid()][databaseurl]
        else:
            o = super(Database,cls).__new__(cls)
            cls._DATABASES[utils.get_processid()][databaseurl] = o
            return o

    def __init__(self,databaseurl):
        if self.inited:
            return
        m = self.DATABASE_URL_RE.search(databaseurl)
        if not m:
            raise Exception("Incorrect database url '{}'".format(databaseurl))
        self.host = m.group("host")
        self.port = m.group("port") or 5432
        self.user = m.group("user")
        self.password = m.group("password")
        self.db = m.group("db")
        self._pool = ThreadedConnectionPool(1,2,dbname=self.db, user=self.user, password=self.password,host=self.host,port=self.port)
        self.inited = True
        logger.debug("Create the connection pool for database({})".format(self))

    def get_conn(self,autocommit=False):
        return self._Connection(self,autocommit)

    def close(self):
        if self._pool:
            try:
                self._pool.closeall()
                #logger.debug("Succeed to close connection pool({}).".format(self))
            except:
                logger.error("Failed to close connection pool({}).{}".format(self,traceback.format_exc()))
            self._pool = None

    @classmethod
    def closeall(cls):
        if utils.get_processid() in cls._DATABASES:
            for db in cls._DATABASES[utils.get_processid()].values():
                db.close()

            del cls._DATABASES[utils.get_processid()]

    def __str__(self):
        return "postgres://{2}:xxxxxx@{0}:{1}/{3}".format(self.host,self.port,self.user,self.db)

    class _Connection(object):
        def __init__(self,database,autocommit):
            self.database = database
            self.autocommit = autocommit
            self._conn = None

        def __enter__(self):
            self._conn = self.database._pool.getconn()
            self._conn.autocommit = self.autocommit
            return self

        def __exit__(self,t,value,tb):
            if self._conn:
                try:
                    if not self.autocommit:
                        self._conn.commit()
                    self._conn.autocommit = False
                    self.database._pool.putconn(self._conn)
                    #logger.debug("Return the connection to connection pool of database({})".format(self))
                except :
                    logger.error("Failed to return the connection to connection pool({}).{}".format(self,traceback.format_exc()))
                self._conn = None

        def __str__(self):
            return str(self.database)

        def cursor(self):
            return self._conn.cursor()

        def commit(self):
            return self._conn.commit()

        def rollback(self):
            return self._conn.rollback()

atexit.register(Database.closeall)
