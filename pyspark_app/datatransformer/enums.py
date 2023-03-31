import logging
import json
import os
import traceback
import re
import maxminddb

import psycopg2

from .. import database
from ..utils import timezone,get_processid
from .. import settings


logger = logging.getLogger(__name__)

enum_dicts = {
}

pattern_map = {}
function_map = {}

def str2enum(key,databaseurl=None,columnid=None,pattern=None,default=None,columnname=None):
    try:
        if not columnid:
            raise Exception("Missing column id")
        if not databaseurl:
            raise Exception("Missing database url")
        if pattern and not default:
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
                return enum_dicts[columnid][key]
    
        elif key in enum_dicts[columnid]:
            return enum_dicts[columnid][key]
        
        if pattern:
            if pattern in pattern_map:
                pattern_re = pattern_map[pattern]
            else:
                pattern_re = re.compile(pattern)
                pattern_map[pattern] = pattern_re
            if pattern_re.search(key):
                valid_domain = True
            else:
                #not a valid domain
                if default in enum_dicts:
                    return enum_dicts[default]
                valid_domain = False
        else:
            valid_domain = True
    
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                if not valid_domain:
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
                    enum_dicts[default] = 0
                    return 0
    
                cursor.execute("select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,key))
                data = cursor.fetchone()
                if data:
                    enum_dicts[columnid][key] = data[0]
                    return data[0]
                cursor.execute("update datascience_datasetcolumn set sequence=COALESCE(sequence,0) + 1,modified='{1}' where id = {0} ".format(columnid,timezone.dbtime()))
                if cursor.rowcount == 0:
                    raise Exception("Dataset Column({}) does not exist".format(columnid))
                cursor.execute("select sequence from datascience_datasetcolumn where id = {}".format(columnid))
                try:
                    sequence = cursor.fetchone()[0]
                    cursor.execute("insert into datascience_datasetenum (column_id,key,value,info) values ({},'{}',{},'{{}}')".format(columnid,key,sequence))
                    conn.commit()
                except:
                    #should already exist, if database is accessable
                    conn.rollback()
                    cursor.execute("select value from datascience_datasetenum where column_id = {} and key = '{}'".format(columnid,key))
                    sequence = cursor.fetchone()[0]
        
                enum_dicts[columnid][key] = sequence
                return sequence
    except:
        logger.error("Failed to convert the value({1}) of column({0}) to enum. {2}".format(columnname,key,traceback.format_exc()))
        raise


#domain_re = re.compile("^[a-zA-Z0-9_\-\.]+(\.[a-zA-Z0-9_\-]+)*(:[0-9]+)?$")
def domain2enum(key,databaseurl=None,columnid=None,columnname=None,record=None,is_invalid=None,default_domain=None,context=None,domain_pattern=None):
    try:
        if not columnid:
            raise Exception("Missing column id")
        if not databaseurl:
            raise Exception("Missing database url")
        if not default_domain:
            raise Exception("Please configure the parameter 'default_domain' for column({})".format(columnname))

        if domain_pattern:
            domain_re = pattern_map.get(domain_pattern)
            if not domain_re:
                domain_re = re.compile(domain_pattern)
                pattern_map[domain_pattern] = domain_re
        else:
            domain_re = None

        if is_invalid:
            f_invalid = function_map.get(is_invalid)
            if not f_invalid:
                f_invalid = eval(is_invalid)
                function_map[is_invalid] = f_invalid
        else:
            f_invalid = None
    
        key = key.strip() if (key and key.strip()) else ""
    
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
        
        if domain_re and not domain_re.search(key):
            #not a valid domain
            if default_domain in enum_dicts:
                return enum_dicts[columnid][default_domain]
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
                            return data[0]

                        if f_invalid and f_invalid(key,record):
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
""".format(columnid,default_domain)
                        cursor.execute(sql)
                        conn.commit()
                        enum_dicts[columnid][default_domain] = 0
                        return 0
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
                return sequence
    except:
        logger.error("Failed to convert domain to enum.columnname={},key={},record={}. {}".format(columnname,key,record,traceback.format_exc()))
        raise

def number2group(value,databaseurl=None,columnid=None):
    if not columnid:
        raise Exception("Missing column id")
    if not databaseurl:
        raise Exception("Missing database url")
    if isinstance(value,str):
        value = int(value.strip()) if (value and value.strip()) else None

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
            return v
    raise Exception("Can't find the range of the value({1}) for column({0})".format(columnid,value))

def _is_group_via_pattern(pattern):
    pattern_re = re.compile(pattern,re.IGNORECASE)
    def _func(val):
        return True if pattern_re.search(val) else False

    return _func

def str2group(value,databaseurl=None,columnid=None):
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
                            enum_dicts[columnid].append((row[0],row[1],eval(row[2].get("is_group"))))
                        except Exception as ex:
                            raise Exception("The lambda expression({2}) is incorrect for the group({1}) of column({0}).{3}".format(columnid,row[0],row[2]["is_group"],str(ex)))
                    elif row[2].get("pattern"):
                        try:
                            enum_dicts[columnid].append((row[0],row[1],_is_group_via_pattern(row[2].get("pattern"))))
                        except Exception as ex:
                            raise Exception("The pattern({2}) is incorrect for the group({1}) of column({0}).{3}".format(columnid,row[0],row[2]["pattern"],str(ex)))
                    else:
                        raise Exception("Configure the group pattern via 'pattern' or lambda function via 'is_group' for the group({1}) of column({0})".format(columnid,row[0]))
                if not enum_dicts[columnid]:
                    raise Exception("Please declare the enum type for column({})".format(columnid))
    value = value or ""
    for k,v,f in enum_dicts[columnid]:
        if f(value):
            return v

    raise Exception("Can't find the group of the value({1}) for column({0})".format(columnid,value))

_city_reader = None
def ip2city(ip,databaseurl=None,columnid=None,pattern=None,default=None,columnname=None,internal={"country":"DBCA","location":[115.861,-31.92]},unrecoginized="unknown"):
    global _city_reader
    city = None
    if not _city_reader:
        if not settings.GEOIP_DATABASE_HOME:
            raise Exception("Please configure env var 'GEOIP_DATABASE_HOME'")
        logger.debug("{} : Open GeoLite2-City.mmdb.".format(get_processid()))
        _city_reader = maxminddb.open_database(os.path.join(settings.GEOIP_DATABASE_HOME,'GeoLite2-City.mmdb'))

    result = _city_reader.get(ip)
    if result:
        if "city" in result:
            city = "{0} {1}".format((result.get("country") or result["registered_country"])["names"]["en"].capitalize(),result["city"]["names"]["en"].capitalize())
        else:
            city = (result.get("country") or result["registered_country"])["names"]["en"].capitalize()

        info = {"location":[result["location"]["longitude"],result["location"]["latitude"]]}
    elif "city" in internal:
        city = "{0} {1}".format(internal["country"].capitalize(),internal["city"].capitalize())
        info = {"location":internal.get("location",[115.861,-31.92])} 
    else:
        city = internal["country"].capitalize()
        info = {"location":internal.get("location",[115.861,-31.92])}

    key = city.replace("'","\\'")

    if columnid not in enum_dicts:
        enum_dicts[columnid] = {}
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select key,value from datascience_datasetenum where column_id = {}".format(columnid))
                for row in cursor.fetchall():
                    enum_dicts[columnid][row[0]] = row[1]
    
        if city in enum_dicts[columnid]:
            return enum_dicts[columnid][city]
    
    elif city in enum_dicts[columnid]:
        return enum_dicts[columnid][city]

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                data = cursor.fetchone()
                if data:
                    enum_dicts[columnid][city] = data[0]
                    return data[0]

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
                return sequence
            except:
                logger.error("Failed to convert ip to enum.columnname={},ip={},city={}. {}".format(columnname,ip,city,traceback.format_exc()))
                if unrecoginized in enum_dicts[columnid]:
                    return 0
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
                    return 0


_country_reader = None
def ip2country(ip,databaseurl=None,columnid=None,pattern=None,default=None,columnname=None,internal="DBCA",unrecoginized="unknown"):
    global _country_reader
    country = None
    if not _country_reader:
        if not settings.GEOIP_DATABASE_HOME:
            raise Exception("Please configure env var 'GEOIP_DATABASE_HOME'")
        logger.debug("{} : Open GeoLite2-Country.mmdb.".format(get_processid()))
        _country_reader = maxminddb.open_database(os.path.join(settings.GEOIP_DATABASE_HOME,'GeoLite2-Country.mmdb'))

    result = _country_reader.get(ip)
    if result:
        country = (result.get("country") or result["registered_country"])["names"]["en"].capitalize().replace("'","\\\\'")
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
            return enum_dicts[columnid][country]
    
    elif country in enum_dicts[columnid]:
        return enum_dicts[columnid][country]

    with database.Database(databaseurl).get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("select value from datascience_datasetenum where column_id = {} and key = E'{}'".format(columnid,key))
                data = cursor.fetchone()
                if data:
                    enum_dicts[columnid][country] = data[0]
                    return data[0]

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
                return sequence
            except:
                logger.error("Failed to convert ip to enum.columnname={},ip={},country={}. {}".format(columnname,ip,country,traceback.format_exc()))
                if unrecoginized in enum_dicts[columnid]:
                    return 0
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
                    return 0
        



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



transformers = [str2enum,domain2enum,number2group,str2group,ip2city,ip2country]
