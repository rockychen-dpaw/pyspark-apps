import logging
import urllib.parse
import json
import os
import traceback
import re
import maxminddb

import psycopg2

from .. import database
from ..utils import timezone,get_processid
from .. import settings
from .helper import transformer_factory


logger = logging.getLogger(__name__)

enum_dicts = {
}

def str2enum(key,databaseurl=None,columnid=None,columnname=None,context=None,record=None,is_valid=None,values=None,pattern=None,default=None):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")

    key = key.strip() if (key and key.strip()) else ""

    #try to get the value from cache
    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                for row in cursor.fetchall():
                    enum_dicts[columnid][row[0]] = row[1]

        if key in enum_dicts[columnid]:
            return enum_dicts[columnid][key]

    elif key in enum_dicts[columnid]:
        return enum_dicts[columnid][key]

    #check whether the key is valid or not
    valid_key = True
    if (values and key not in values) or (is_valid and not is_valid(key) or (pattern and not pattern.search(key))):
        #key is invalid
        if context["phase"] == "Download":
            #can't parse the key, raise exception
            with database.Database(databaseurl).get_conn() as conn:
                with conn.cursor() as cursor:
                    if context.get("dsfile"):
                        msg = "The value({2}) of the column({1}) is invalid for dataset file({0}).record={3}".format(context["dsfile"],columnname,key,record)
                    else:
                        msg = "The value({1}) of the column({0}) is invalid.".format(columnname,key)
                    msg = msg.replace("'","''")

                    sql = """
INSERT INTO datascience_runningissue
    ("phase","category","dstime","dsfile","message","created") 
VALUES 
    ('{0}','{1}', '{2}','{3}','{4}','{5}')
""".format(
                        context["phase"],
                        context['category'], 
                        timezone.dbtime(context.get("dstime")) if context.get("dstime") else 'null', 
                        context.get("dsfile","null"),
                        msg,
                        timezone.dbtime()
                    )
                    try:
                        cursor.execute(sql)
                        conn.commit()
                    except:
                        conn.rollback()
                        raise
                    
        if default is not None:
            #a default value is configured,use the default value
            if default in enum_dicts[columnid]:
                return enum_dicts[columnid][default]
            else:
                valid_key = False
        else:
            raise Exception("The value({2}) of the column({1}) is invalid,context={0}".format(context,columnname,key))

    sql = None
    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            if not valid_key:
                #use the default value
                sql = """
INSERT INTO datascience_datasetenum 
(column_id,key,value,info) 
VALUES 
({0},'{1}', 0,'{{}}')
ON CONFLICT (column_id,value) DO UPDATE 
SET key='{1}'
""".format(columnid,default)
                cursor.execute(sql)
                conn.commit()
                enum_dicts[columnid][default] = 0
                return 0

            dbkey = key.replace("'","''") 
            sql = "select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,dbkey)
            cursor.execute(sql)
            data = cursor.fetchone()
            if data:
                enum_dicts[columnid][key] = data[0]
                return data[0]

            if context["phase"] != "Download":
                #not in phase 'Download', the key should exist.
                raise Exception("The value({1}) of the column({0}) is not recognized.".format(columnname,key))

            sql = "update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime())
            cursor.execute(sql)
            if cursor.rowcount == 0:
                raise Exception("Dataset Column({}) does not exist".format(columnid))
            sql = "select sequence from datascience_datasetcolumn where id = {}".format(columnid)
            cursor.execute(sql)
            try:
                sequence = cursor.fetchone()[0]
                sql = "insert into datascience_datasetenum (column_id,key,value,info) values ({},'{}',{},'{{}}')".format(columnid,dbkey,sequence)
                cursor.execute(sql)
                conn.commit()
            except:
                #should already exist, if database is accessable
                conn.rollback()
                sql = "select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,dbkey)
                cursor.execute(sql)
                sequence = cursor.fetchone()[0]
    
            enum_dicts[columnid][key] = sequence
            return sequence

#domain_re = re.compile("^[a-zA-Z0-9_\-\.]+(\.[a-zA-Z0-9_\-]+)*(:[0-9]+)?$")
def domain2enum(key,databaseurl=None,columnid=None,columnname=None,record=None,context=None,return_id=True,is_valid=None,is_notfound=None,default=None,pattern=None):
    try:
        if not columnid:
            raise Exception("Missing column id")
        if not databaseurl:
            raise Exception("Missing database url")
        if not default:
            raise Exception("Please configure the parameter 'default' for column({})".format(columnname))

        key = key.strip() if (key and key.strip()) else ""
    
        if columnid not in enum_dicts:
            enum_dicts[columnid] = {}
            with database.Database(databaseurl).get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                    for row in cursor.fetchall():
                        enum_dicts[columnid][row[0]] = row[1]
    
            if key in enum_dicts[columnid]:
                return enum_dicts[columnid][key] if return_id else key
    
        elif key in enum_dicts[columnid]:
            return enum_dicts[columnid][key] if return_id else key
        
        if (pattern and not pattern.search(key)) or (is_valid and not is_valid(key)):
            #not a valid domain
            if default in enum_dicts:
                return enum_dicts[columnid][default] if return_id else default
            valid_domain = False
        else:
            valid_domain = True
    
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    if valid_domain:
                        cursor.execute("select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,key))
                        data = cursor.fetchone()
                        if data:
                            enum_dicts[columnid][key] = data[0]
                            if "reprocess" not in context:
                                context["reprocess"] = set()
                            context["reprocess"].add(columnname)
                            return data[0] if return_id else key

                        if is_notfound and is_notfound(key,record):
                            valid_domain = False
                    
                    #request a non-exist domain, maybe sent by hacker
                    if not valid_domain:
                        #the original domain is invalid
                        sql = """
INSERT INTO datascience_datasetenum 
    (column_id,key,value,info) 
VALUES 
    ({0},'{1}', 0,'{{}}')
ON CONFLICT (column_id,value) DO UPDATE 
SET key='{1}'
""".format(columnid,default)
                        cursor.execute(sql)
                        conn.commit()
                        enum_dicts[columnid][default] = 0
                        return 0 if return_id else default
                except:
                    conn.rollback()
                    raise
                #valid domain, add to the enum type
                try:
                    cursor.execute("update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime()))
                    if cursor.rowcount == 0:
                        raise Exception("Dataset Column({}) does not exist".format(columnid))
                    cursor.execute("select sequence from datascience_datasetcolumn where id = {}".format(columnid))
                    sequence = cursor.fetchone()[0]
                    cursor.execute("insert into datascience_datasetenum (column_id,key,value,info) values ({},'{}',{},'{{}}')".format(columnid,key,sequence))
                    conn.commit()
                except:
                    #should already exist, if database is accessable
                    conn.rollback()
                    cursor.execute("select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,key))
                    sequence = cursor.fetchone()[0]
        
                enum_dicts[columnid][key] = sequence
                if "reprocess" not in context:
                    context["reprocess"] = set()
                context["reprocess"].add(columnname)
                return sequence if return_id else key
    except:
        logger.error("Failed to convert domain to enum.columnname={},key={},record={}. {}".format(columnname,key,record,traceback.format_exc()))
        raise

def number2group(value,databaseurl=None,columnid=None,columnname=None,context=None,record=None,is_valid=None,values=None,pattern=None,default=None,return_id=True):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")
    if isinstance(value,str):
        try:
            value = int(value.strip()) if (value and value.strip()) else None
        except:
            with database.Database(databaseurl).get_conn() as conn:
                with conn.cursor() as cursor:
                    sql = None
                    try:
                        sql = """
INSERT INTO datascience_runningissue
    ("phase","category","dstime","dsfile","message","created") 
VALUES 
    ('{0}','{1}', '{2}','{3}','{4}','{5}')
""".format(
                        context["phase"],
                        context['category'], 
                        timezone.dbtime(context.get("dstime")) if context.get("dstime") else 'null', 
                        context.get("dsfile","null"),
                        "The value({2}) of the column({1}) is not a valid integer.record={0}".format(str(record).replace("'","''"),columnname,value),
                        timezone.dbtime()
                        )
                        cursor.execute(sql)
                        conn.commit()
                    except:
                        conn.rollback()
                        logger.error("Failed to execute sql.{1}\n{0}".format(sql,traceback.format_exc()))
            value = None

    if columnid not in enum_dicts:
        enum_dicts[columnid] = []

        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value,info from datascience_datasetenum where column_id = {} order by value asc".format(columnid))
                for row in cursor.fetchall():
                    if not row[2]:
                        raise Exception("Configure the lambda function iva 'is_in_group' for the group({1}) of column({0})".format(columnid,row[0]))
                    if row[2].get("is_group"):
                        try:
                            enum_dicts[columnid].append((row[0],row[1],eval(row[2].get("is_group"))))
                        except Exception as ex:
                            raise Exception("The lambda expression({2}) is incorrect for the group({1}) of column({0}).{3}".format(columnid,row[0],row[2]["is_group"],str(ex)))
                    else:
                        raise Exception("Configure the group pattern via 'pattern' or lambda function iva 'is_in_group' for the group({1}) of column({0})".format(columnid,row[0]))
                if not enum_dicts[columnid]:
                    raise Exception("Please declare the enum type for column({})".format(columnid))

    for k,v,f in enum_dicts[columnid]:
        if f(value):
            return v if return_id else k
    raise Exception("Can't find the range of the value({1}) for column({0})".format(columnid,value))

def _is_group_via_pattern(pattern):
    pattern_re = re.compile(pattern,re.IGNORECASE)
    def _func(val):
        return True if pattern_re.search(val) else False

    return _func

def str2group(value,databaseurl=None,columnid=None,context=None,record=None,columnname=None,return_id=True,**kwargs):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")

    value = value.strip() if (value and value.strip()) else ""

    if columnid not in enum_dicts:
        enum_dicts[columnid] = []

        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value,info from datascience_datasetenum where column_id = {} order by value asc".format(columnid))
                for row in cursor.fetchall():
                    if not row[2]:
                        raise Exception("Configure the group pattern via 'pattern' or lambda function via 'is_group' for the group({1}) of column({0})".format(columnid,row[0]))
                    if row[2].get("is_group"):
                        try:
                            func = eval(row[2].get("is_group"))
                        except Exception as ex:
                            raise Exception("The lambda expression({2}) is incorrect for the group({1}) of column({0}).{3}".format(columnid,row[0],row[2]["is_group"],str(ex)))
                    elif row[2].get("pattern"):
                        try:
                            func = _is_group_via_pattern(row[2].get("pattern"))
                        except Exception as ex:
                            raise Exception("The pattern({2}) is incorrect for the group({1}) of column({0}).{3}".format(columnid,row[0],row[2]["pattern"],str(ex)))
                    else:
                        raise Exception("Configure the group pattern via 'pattern' or lambda function via 'is_group' for the group({1}) of column({0})".format(columnid,row[0]))
                    enum_dicts[columnid].append((row[0],row[1],transformer_factory(func)))
                if not enum_dicts[columnid]:
                    raise Exception("Please declare the enum type for column({})".format(columnid))
    value = value or ""
    for k,v,f in enum_dicts[columnid]:
        if f[0](f[1],value,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs):
            return v if return_id else k

    raise Exception("Can't find the group of the value({1}) for column({0})".format(columnid,value))

_city_reader = None
def ip2city(ip,databaseurl=None,columnid=None,pattern=None,default=None,columnname=None,internal={"country":"DBCA","location":[115.861,-31.92]},unrecoginized="unknown",return_id=True):
    global _city_reader
    city = None
    if not _city_reader:
        if not settings.GEOIP_DATABASE_HOME:
            raise Exception("Please configure env var 'GEOIP_DATABASE_HOME'")
        logger.debug("{} : Open GeoLite2-City.mmdb.".format(get_processid()))
        _city_reader = maxminddb.open_database(os.path.join(settings.GEOIP_DATABASE_HOME,'GeoLite2-City.mmdb'))

    if not ip:
        #no ip address, 
        return 0 if return_id else unrecoginized
    result = _city_reader.get(ip)
    try:
        if result:
            if "city" in result:
                city = "{0} {1}".format((result.get("country") or result.get("registered_country") or result["continent"])["names"]["en"].capitalize(),result["city"]["names"]["en"].capitalize())
            else:
                city = (result.get("country") or result.get("registered_country") or result["continent"])["names"]["en"].capitalize()
    
            info = {"location":[result["location"]["longitude"],result["location"]["latitude"]]}
        elif "city" in internal:
            city = "{0} {1}".format(internal["country"].capitalize(),internal["city"].capitalize())
            info = {"location":internal.get("location",[115.861,-31.92])} 
        else:
            city = internal["country"].capitalize()
            info = {"location":internal.get("location",[115.861,-31.92])}
    except KeyError as ex:
        city = unrecoginized
        info = {}
    except :
        raise Exception("Failed to get the city from GeoLite2-City.mmdb. ip={}, ip data={},{}".format(ip,result,traceback.format_exc()))

    key = city.replace("'","\\'")

    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                for row in cursor.fetchall():
                    enum_dicts[columnid][row[0]] = row[1]
    
        if city in enum_dicts[columnid]:
            return enum_dicts[columnid][city] if return_id else city
    
    elif city in enum_dicts[columnid]:
        return enum_dicts[columnid][city] if return_id else city

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                data = cursor.fetchone()
                if data:
                    enum_dicts[columnid][city] = data[0]
                    return data[0] if return_id else city

                #valid domain, add to the enum type
                try:
                    cursor.execute("update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime()))
                    if cursor.rowcount == 0:
                        raise Exception("Dataset Column({}) does not exist".format(columnid))
                    cursor.execute("select sequence from datascience_datasetcolumn where id = {}".format(columnid))
                    sequence = cursor.fetchone()[0]
                    cursor.execute("insert into datascience_datasetenum (column_id,key,value,info) values ({},E'{}',{},'{}')".format(columnid,key,sequence,json.dumps(info)))
                    conn.commit()
                except:
                    #should already exist, if database is accessable
                    conn.rollback()
                    cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                    sequence = cursor.fetchone()[0]
        
                enum_dicts[columnid][city] = sequence
                return sequence if return_id else city
            except:
                logger.error("Failed to convert ip to enum.columnname={},ip={},city={}. {}".format(columnname,ip,city,traceback.format_exc()))
                if unrecoginized in enum_dicts[columnid]:
                    return 0 if return_id else unrecoginized
                else:
                    sql = """
            INSERT INTO datascience_datasetenum 
                (column_id,key,value,info) 
            VALUES 
                ({0},'{1}', 0,'{{}}')
            ON CONFLICT (column_id,value) DO UPDATE 
            SET key='{1}'
            """.format(columnid,unrecoginized)
                    cursor.execute(sql)
                    conn.commit()
                    enum_dicts[columnid][unrecoginized] = 0
                    return 0 if return_id else unrecoginized


_country_reader = None
def ip2country(ip,databaseurl=None,columnid=None,pattern=None,default=None,columnname=None,internal="DBCA",unrecoginized="unknown",return_id=True):
    global _country_reader
    country = None
    if not _country_reader:
        if not settings.GEOIP_DATABASE_HOME:
            raise Exception("Please configure env var 'GEOIP_DATABASE_HOME'")
        logger.debug("{} : Open GeoLite2-Country.mmdb.".format(get_processid()))
        _country_reader = maxminddb.open_database(os.path.join(settings.GEOIP_DATABASE_HOME,'GeoLite2-Country.mmdb'))
  
    if not ip:
        return 0 if return_id else unrecoginized
    result = _country_reader.get(ip)
    if result:
        try:
            country = (result.get("country") or result.get("registered_country") or result["continent"])["names"]["en"].capitalize().replace("'","\\\\'")
        except KeyError as ex:
            country = unrecoginized
        except :
            raise Exception("Failed to get the country from GeoLite2-Country.mmdb. ip={}, ip data={},{}".format(ip,result,traceback.format_exc()))
    else:
        country = internal.capitalize()

    key = country.replace("'","\\'")

    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                for row in cursor.fetchall():
                    enum_dicts[columnid][row[0]] = row[1]
    
        if country in enum_dicts[columnid]:
            return enum_dicts[columnid][country] if return_id else country
    
    elif country in enum_dicts[columnid]:
        return enum_dicts[columnid][country] if return_id else country

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                data = cursor.fetchone()
                if data:
                    enum_dicts[columnid][country] = data[0]
                    return data[0] if return_id else country

                #valid domain, add to the enum type
                try:
                    cursor.execute("update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime()))
                    if cursor.rowcount == 0:
                        raise Exception("Dataset Column({}) does not exist".format(columnid))
                    cursor.execute("select sequence from datascience_datasetcolumn where id = {}".format(columnid))
                    sequence = cursor.fetchone()[0]
                    cursor.execute("insert into datascience_datasetenum (column_id,key,value,info) values ({},E'{}',{},'{{}}')".format(columnid,key,sequence))
                    conn.commit()
                except:
                    #should already exist, if database is accessable
                    conn.rollback()
                    cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                    sequence = cursor.fetchone()[0]
        
                enum_dicts[columnid][country] = sequence
                return sequence if return_id else country
            except:
                logger.error("Failed to convert ip to enum.columnname={},ip={},country={}. {}".format(columnname,ip,country,traceback.format_exc()))
                if unrecoginized in enum_dicts[columnid]:
                    return 0 if return_id else unrecoginized
                else:
                    sql = """
            INSERT INTO datascience_datasetenum 
                (column_id,key,value,info) 
            VALUES 
                ({0},'{1}', 0,'{{}}')
            ON CONFLICT (column_id,value) DO UPDATE 
            SET key='{1}'
            """.format(columnid,unrecoginized)
                    cursor.execute(sql)
                    conn.commit()
                    enum_dicts[columnid][unrecoginized] = 0
                    return 0 if return_id else unrecoginized
 
_domain_configs = {}
NONE_FUNC = lambda domain,path,queryparameters:None
def resourcekey(domain,databaseurl=None,columnid=None,columnname=None,context=None,record=None,path_index=None,queryparameters_index=None,configs=None,default="null"):
    if not configs or not path_index or not queryparameters_index:
        raise Exception("Some of the parameters('configs','path_index','queryparameters_index') are missing")
   
    path = urllib.parse.unquote(record[path_index] or "/")
    queryparameters = urllib.parse.unquote(record[queryparameters_index] or "")
    try:
        func = _domain_configs[domain]
    except KeyError as ex:
        func = None
        for k,v in configs.items():
            if callable(k):
                if k(domain):
                    func = v
                    break
            elif k == domain:
                func = v
                break
        if func is None:
            func = NONE_FUNC

        _domain_configs[domain] = func

    key = func(domain,path,queryparameters)
    if not key:
        key = default
    elif isinstance(key,int):
        key = str(key)
    #try to get the value from cache
    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                for row in cursor.fetchall():
                    enum_dicts[columnid][row[0]] = row[1]

        if key in enum_dicts[columnid]:
            return enum_dicts[columnid][key]

    elif key in enum_dicts[columnid]:
        return enum_dicts[columnid][key]

    sql = None
    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            if key == default:
                sql = """
INSERT INTO datascience_datasetenum 
(column_id,key,value,info) 
VALUES 
({0},'{1}', 0,'{{}}')
ON CONFLICT (column_id,value) DO UPDATE 
SET key='{1}'
""".format(columnid,default)
                cursor.execute(sql)
                conn.commit()
                enum_dicts[columnid][default] = 0
                return 0
            dbkey = key.replace("'","''") 
            if len(dbkey) >= 512:
                dbkey = dbkey[:511]
            sql = "select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,dbkey)
            cursor.execute(sql)
            data = cursor.fetchone()
            if data:
                enum_dicts[columnid][key] = data[0]
                return data[0]

            sql = "update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime())
            cursor.execute(sql)
            if cursor.rowcount == 0:
                raise Exception("Dataset Column({}) does not exist".format(columnid))
            sql = "select sequence from datascience_datasetcolumn where id = {}".format(columnid)
            cursor.execute(sql)
            try:
                sequence = cursor.fetchone()[0]
                sql = "insert into datascience_datasetenum (column_id,key,value,info) values ({},'{}',{},'{{}}')".format(columnid,dbkey,sequence)
                cursor.execute(sql)
                conn.commit()
            except:
                #should already exist, if database is accessable
                conn.rollback()
                sql = "select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,dbkey)
                cursor.execute(sql)
                sequence = cursor.fetchone()[0]
    
            enum_dicts[columnid][key] = sequence
            return sequence

def get_enum(key,databaseurl=None,columnid=None):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")

    key = key.strip() if (key and key.strip()) else ""

    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
    elif key in enum_dicts[columnid]:
        return enum_dicts[columnid][key]

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,key))
            data = cursor.fetchone()
            if data:
                enum_dicts[columnid][key] = data[0]
                return data[0]
            else:
                return None

enum_val_dicts = {}
def get_enum_key(value,databaseurl=None,columnid=None,return_val_ifmissing=True):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")

    if columnid not in enum_val_dicts:
        enum_val_dicts[columnid] = {}
    elif value in enum_val_dicts[columnid]:
        return enum_val_dicts[columnid][value]

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("select key from datascience_datasetenum where column_id = {} and value = {}".format(columnid,value))
            data = cursor.fetchone()
            if data:
                enum_val_dicts[columnid][value] = data[0]
                return data[0]
            elif return_val_ifmissing:
                return value
            else:
                raise Exception("Value({1}) is not a valid data for column({0})".format(columnid,value))

def clean():
    global _country_reader
    global _city_reader
    if _country_reader:
        _country_reader.close()
        logger.debug("{} : Close GeoLite2-Country.mmdb.".format(get_processid()))
        _country_reader = None

    if _city_reader:
        _city_reader.close()
        logger.debug("{} : Close GeoLite2-City.mmdb.".format(get_processid()))
        _city_reader = None



transformers = [str2enum,domain2enum,number2group,str2group,ip2city,ip2country,resourcekey]
