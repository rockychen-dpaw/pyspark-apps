import logging
import traceback
import random
import os
import importlib
import itertools
import collections
import json
import tempfile
import base64
import shutil
from datetime import datetime,timedelta
import csv 

from pyspark_app import settings
from pyspark_app import harvester
from pyspark_app import database
from pyspark_app.utils import timezone
from pyspark_app.utils.filelock import FileLock

from pyspark_app import utils
from pyspark_app import datatransformer
from pyspark_app import operation
from pyspark_app.app.base import get_spark_session

logger = logging.getLogger("pyspark_app.app.nginxaccesslog")

HOURLY_REPORT = 1
DAILY_REPORT = 2

EXECUTOR_COLUMNID=0
EXECUTOR_COLUMNNAME=1
EXECUTOR_DTYPE=2
EXECUTOR_TRANSFORMER=3
EXECUTOR_COLUMNINFO=4
EXECUTOR_STATISTICAL=5
EXECUTOR_FILTERABLE=6
EXECUTOR_GROUPABLE=7


def get_harvester(datasetinfo):
    """
    Return a harvester which harvest the nginx access log from source repository.
    """
    harvester_config = datasetinfo.get("harvester")
    if not harvester_config:
        raise Exception("Nissing the configuration 'harvester'")
    harvester_config["name"]
    return harvester.get_harvester(harvester_config["name"],**harvester_config["parameters"])

def filter_factory(includes,excludes):
    """
    Return a filter function to filter out the noisy data from nginx access log
    """
    def _exclude(val):
        return not excludes(val)

    def _includes(val):
        for f in includes:
            if f(val):
                return True
        return False
    
    def _excludes(val):
        for f in excludes:
            if f(val):
                return False
        return True

    def _includes_and_excludes(val):
        for f in excludes:
            if f(val):
                return False

        for f in includes:
            if f(val):
                return True

        return False

    
    if not includes and not excludes:
        #no includes
        return None
    elif includes and excludes:
        #both includes and excludes are congigured
        if not isinstance(includes,list):
            includes = [includes]
        if not isinstance(excludes,list):
            excludes = [excludes]

        for i in range(len(includes)):
            includes[i] = eval(includes[i])

        for i in range(len(excludes)):
            excludes[i] = eval(excludes[i])

        return _includes_and_excludes
    elif includes:
        #only includes are configured
        if isinstance(includes,list):
            if len(includes) == 1:
                includes = includes[0]
                includes = eval(includes)
                return includes
            else:
                for i in range(len(includes)):
                    includes[i] = eval(includes[i])
                return _includes
        else:
            includes = eval(includes)
            return includes
    else:
        #only excludes are configured
        if isinstance(excludes,list):
            if len(excludes) == 1:
                excludes = excludes[0]
                excludes = eval(excludes)
                return _exclude
            else:
                for i in range(len(excludes)):
                    excludes[i] = eval(excludes[i])
                return _excludes
        else:
            excludes = eval(excludes)
            return _exclude

def report_details(data_file,indexes):
    index_index = 0
    row_index = 0
    with open(data_file) as data_f:
        datareader = csv.reader(data_f)
        for row in datareader:
            if row_index == indexes[index_index]:
                yield row
                index_index += 1
                if index_index >= len(indexes):
                    break
            row_index += 1


def analysis_factory(reportid,databaseurl,datasetid,datasetinfo,report_start,report_end,report_conditions,report_group_by,resultset,report_type):
    """
    Return a function to analysis a single nginx access log file
    """
    def analysis(data):
        """
        Analysis a single nginx access log file
        params:
            data: a tupe (datetime of the nginx access log with format "%Y%m%d%H", file name of nginx access log)
        """
        import h5py
        import numpy as np
        import pandas as pd

        cache_dir = datasetinfo.get("cache")
        if not cache_dir:
            raise Exception("Nissing the configuration 'cache_dir'")

        dataset_time = timezone.parse(data[0],"%Y%m%d%H")
        logger.debug("dataset_time = {}, str={}".format(dataset_time,data[0]))

        cache_folder = os.path.join(cache_dir,dataset_time.strftime("%Y-%m-%d"))
        utils.mkdir(cache_folder)

        harvester = None
        #get the cached local data file and data index file
        data_file = os.path.join(cache_folder,data[1])
        data_index_file = os.path.join(cache_folder,"{}.hdf5".format(data[1]))

        #load the dataset column settings, a map between columnindex and a tuple(includes and excludes,(id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn))
        allreportcolumns = {}
        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(datasetid))
                previous_columnindex = None
                columns = None
                for d in itertools.chain(cursor.fetchall(),[[-1]]):
                    if previous_columnindex is None or previous_columnindex != d[0]:
                        #new column index
                        if columns:
                            #initialize the column's includes
                            columns[0] = filter_factory(columns[0][0],columns[0][1])
                        if d[0] == -1:
                            #the end flag
                            break

                        previous_columnindex = d[0]
                        columns = [[d[5].get("include") if d[5] else None,d[5].get("exclude") if d[5] else None],[(d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8])]]
                        allreportcolumns[d[0]] = columns
                    else:
                        columns[1].append((d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8]))
                        if d[5]:
                            if d[5].get("include"):
                                if columns[0][0]:
                                    if isinstance(columns[0][0],list):
                                        columns[0][0].append(d[5].get("include"))
                                    else:
                                        columns[0][0] = [columns[0][0],d[5].get("include")]
                                else:
                                    columns[0][0] = d[5].get("include")

                            if d[5].get("exclude"):
                                if columns[0][1]:
                                    if isinstance(columns[0][1],list):
                                        columns[0][1].append(d[5].get("exclude"))
                                    else:
                                        columns[0][1] = [columns[0][1],d[5].get("exclude")]
                                else:
                                    columns[0][1] = d[5].get("exclude")


        if not os.path.exists(data_file) and os.path.exists(data_index_file):
            #if data_file doesn't exist, data_indes_file should not exist too.
            utils.remove_file(data_index_file)

        #check data index file
        dataset_size = 0
        if os.path.exists(data_index_file):
            #the data index file exist, check whether the indexes are created for all columns.if not regenerate it
            try:
                with h5py.File(data_index_file,'r') as index_file:
                    for columnindex,reportcolumns in allreportcolumns.items():
                        for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                            if  not column_filterable and not column_groupable and not column_statistical:
                                continue
                            #check whether the dataset is accessable by getting the size
                            dataset_size = index_file[column_name].shape[0]

            except:
                #some dataset does not exist or file is corrupted.
                utils.remove_file(data_index_file)

        if os.path.exists(data_index_file):
            #data index file is ready to use
            logger.debug("The index file({1}) is already generated for data file({0})".format(data_file,data_index_file))
        else:
            #data index file does not exist, generate it.
            #Obtain the file lock before generating the index file to prevend mulitiple process from generating the index file for the same access log file
            with FileLock(os.path.join(cache_folder,"{}.lock".format(data[1])),120) as lock:
                if os.path.exists(data_index_file):
                    #data index file are already generated by other process, check whether it is accessable.
                    try:
                        with h5py.File(data_index_file,'r') as index_file:
                            for columnindex,reportcolumns in allreportcolumns.items():
                                for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                    if  not column_filterable and not column_groupable and not column_statistical:
                                        continue
                                    dataset_size = index_file[column_name].shape[0]
                        #file already exist, use it directly
                        logger.debug("The index file({1}) is already generated for data file({0})".format(data_file,data_index_file))
                    except:
                        #some dataset does not exist or file is corrupted.
                        utils.remove_file(data_index_file)

                if not os.path.exists(data_index_file):
                    #generate the index file
                    #get the line counter of the file
                    try:
                        indexbuff_baseindex = 0
                        indexbuff_index = 0
                        indexdatasets = {}
                        indexbuffs = {}

                        databuff_index = 0
                        databuff = None
                        #prepare the source data file if required.
                        src_data_file = None
                        if not os.path.exists(data_file):
                            #local data file doesn't exist, download the source file if required as src_data_file
                            harvester = get_harvester(datasetinfo)
                            if harvester.is_local():
                                src_data_file = harvester.get_abs_path(data[1])
                            else:
                                with tempfile.NamedTemporaryFile(prefix="datascience_ngx_log",delete=False) as f:
                                    src_data_file = f.name
                                harvester.saveas(data[1],src_data_file)

                        #get the size of the original access log file or the local cached access log file which excludes the noisy datas.
                        dataset_size = utils.get_line_counter(src_data_file or data_file)
                        logger.debug("The file({}) has {} records".format((src_data_file or data_file),dataset_size))

                        #generate index file
                        tmp_index_file = "{}.tmp".format(data_index_file)
                        excluded_rows = 0
                        context={
                            "dataset_time":dataset_time
                        }

                        try:
                            buffer_size = datasetinfo.get("indexbatchsize",10000)
                        except:
                            buffer_size = 10000
                        try:
                            databuffer_size = datasetinfo.get("databatchsize",1000)
                        except:
                            databuffer_size = 10000

                        if src_data_file:
                            #the local cached data file doesnot exist, 
                            #should generate the local cached data file by excluding the noisy data from original dataset.
                            tmp_data_file = "{}.tmp".format(data_file)
                            data_f = open(tmp_data_file,'w')
                            logwriter = csv.writer(data_f)
                            databuff = [None] * databuffer_size
                        else:
                            #found the cached file, get the data from local cached file
                            tmp_data_file = None
                            data_f = None
                            logwriter = None
                            databuff = None


                        with h5py.File(tmp_index_file,'w') as tmp_h5:

                            reprocess_columns = None
                            while True:
                                indexbuff_baseindex = 0
                                indexbuff_index = 0
                                databuff_index = 0
                                with open(src_data_file or data_file) as f:
                                    logreader = csv.reader(f)
    
                                    for item in logreader:
                                        #check the filter first
                                        if src_data_file:
                                            #data are retrieved from source, should execute the filter logic
                                            excluded = False
                                            for columnindex,reportcolumns in allreportcolumns.items():
                                                value = item[columnindex]
                                                excluded = False
                                                if  reportcolumns[0] and not reportcolumns[0](value):
                                                    #excluded
                                                    excluded = True
                                                    break
                                            if excluded:
                                                #record are excluded
                                                excluded_rows += 1
                                                continue
                                            #data are retrieved from source,append the data to local data file
                                            databuff[databuff_index] = item
                                            databuff_index += 1
                                            if databuff_index == databuffer_size:
                                                #databuff is full, flush to file 
                                                logwriter.writerows(databuff)
                                                databuff_index = 0
                                        
                                        #generate the dataset for each index column
                                        for columnindex,reportcolumns in allreportcolumns.items():
                                            value = item[columnindex]
                                            for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                                if  not column_filterable and not column_groupable and not column_statistical:
                                                    #no need to create index 
                                                    continue
                                                if reprocess_columns and column_name not in reprocess_columns:
                                                    #this is not the first run, and this column no nedd to process again
                                                    continue
                    
                                                #create the buffer and hdf5 dataset for column
                                                if column_name not in indexdatasets:
                                                    indexdatasets[column_name] = tmp_h5.create_dataset(column_name, (dataset_size,),dtype=datatransformer.get_hdf5_type(column_dtype))
                                                    indexbuffs[column_name] = np.empty((buffer_size,),dtype=datatransformer.get_np_type(column_dtype))
                    
                                                #get the index data for each index column
                                                if column_transformer:
                                                    #data  transformation is required
                                                    if column_columninfo and column_columninfo.get("parameters"):
                                                        indexbuffs[column_name][indexbuff_index] = datatransformer.transform(column_transformer,value,databaseurl=databaseurl,columnid=column_columnid,context=context,record=item,columnname=column_name,**column_columninfo["parameters"])
                                                    else:
                                                        indexbuffs[column_name][indexbuff_index] = datatransformer.transform(column_transformer,value,databaseurl=databaseurl,columnid=column_columnid,context=context,record=item,columnname=column_name)
                                                else:
                                                    indexbuffs[column_name][indexbuff_index] = value.strip() if value else ""
                    
                                                if indexbuff_index == buffer_size - 1:
                                                    #buff is full, write to hdf5 file
                                                    try:
                                                        indexdatasets[column_name].write_direct(indexbuffs[column_name],np.s_[0:buffer_size],np.s_[indexbuff_baseindex:indexbuff_baseindex + buffer_size])
                                                    except Exception as ex:
                                                        logger.debug("Failed to write {2} records to dataset({1}) which are save in hdf5 file({0}).{3}".format(tmp_index_file,column_name,buffer_size,str(ex)))
                                                        raise
        
                                                    lock.renew()
                    
                                        indexbuff_index += 1
                                        if indexbuff_index == buffer_size:
                                            #buff is full, data is already saved to hdf5 file, set indexbuff_index and indexbuff_baseindex
                                            indexbuff_index = 0
                                            indexbuff_baseindex += buffer_size
                                            logger.debug("indexbuff_baseindex = {}".format(indexbuff_baseindex))
    
                                if src_data_file:
                                    if databuff_index > 0:
                                        #still have some data in data buff, flush it to file
                                        logwriter.writerows(databuff[:databuff_index])

                                    data_f.close()
                                    #rename the tmp data file to data file if required
                                    os.rename(tmp_data_file,data_file)

                                    if not harvester.is_local():
                                        #src data file is a temp file , delete it
                                        utils.remove_file(src_data_file)
                                        pass
                                    #set src_data_file to None, next run will read the data from data_file directly
                                    src_data_file = None
                                    logger.info("file({0}) which contains {1} rows, {2} rows were processed, {3} rows were ignored ".format(data_file,dataset_size,indexbuff_baseindex + indexbuff_index,excluded_rows))

                                
                                logger.debug("indexbuff_baseindex = {},indexbuff_index = {}, excluded_rows = {}".format(indexbuff_baseindex,indexbuff_index,excluded_rows))
                                #still have data in buff, write them to hdf5 file
                                if indexbuff_index > 0:
                                    for columnindex,reportcolumns in allreportcolumns.items():
                                        for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                            if  not column_filterable and not column_groupable and not column_statistical:
                                                continue
                    
                                            if reprocess_columns and column_name not in reprocess_columns:
                                                #this is not the first run, and this column no nedd to process again
                                                continue
                    
                                            indexdatasets[column_name].write_direct(indexbuffs[column_name],np.s_[0:indexbuff_index],np.s_[indexbuff_baseindex:indexbuff_baseindex + indexbuff_index])

                                if context.get("reprocess"):
                                    #data file is ready, can read the data from data_file directly after the first run
                                    reprocess_columns = context.pop("reprocess")
                                    logger.debug("The columns({1}) are required to reprocess for report({0})".format(reportid,reprocess_columns))
                                else:
                                    #the data file has been processed. 
                                    if indexbuff_baseindex + indexbuff_index + excluded_rows != dataset_size:
                                        raise Exception("The file({0}) has {1} records, but only {2} are written to hdf5 file({3})".format(data_file,(dataset_size - excluded_rows),indexbuff_baseindex + indexbuff_index,data_index_file))
                                    else:
                                        logger.info("The index file {1} was generated for file({0}) which contains {2} rows, {3} rows were processed, {4} rows were ignored ".format(data_file,data_index_file,dataset_size,indexbuff_baseindex + indexbuff_index,excluded_rows))
                                    break
                    
                    finally:
                        if src_data_file:
                            try:
                                data_f.close()
                            except:
                                pass

                        #release resources in datatransformer
                        datatransformer.clean()

                        #release the memory
                        indexdatasets = None
                        indexbuffs = None
                        databuff = None
                    
                    #rename the tmp file to index file
                    if excluded_rows:
                        #some rows are excluded from access log, the size of the index dataset should be shrinked to (dataset_baseindex + buff_index)
                        dataset_size = indexbuff_baseindex + indexbuff_index
                        tmp2_index_file = "{}.tmp2".format(data_index_file)
                        with h5py.File(tmp2_index_file,'w') as tmp2_h5:
                            with h5py.File(tmp_index_file,'r') as tmp_h5:
                                for columnindex,reportcolumns in allreportcolumns.items():
                                    for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                        if  not column_filterable and not column_groupable and not column_statistical:
                                            continue
                                        try:
                                            tmp2_h5.create_dataset(column_name,dtype=tmp_h5[column_name].dtype,data=tmp_h5[column_name][0:dataset_size])
                                        except Exception as ex:
                                            logger.error("Failed to resize the dataset.file={} column={}, before size={}, after size={}. {}".format(data_index_file,column_name,tmp_h5[column_name].shape[0],dataset_size,str(ex)))
                                            raise

                        os.rename(tmp2_index_file,data_index_file)

                        utils.remove_file(tmp_index_file)
                    else:
                        os.rename(tmp_index_file,data_index_file)


        #popuate the column map for fast accessing.
        column_map = {}
        for columnindex,reportcolumns in allreportcolumns.items():
            #column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable
            for col in reportcolumns[1]:
                column_map[col[1]] = col
        allreportcolumns.clear()
        allreportcolumns = None


        #begin the perform the filtering, group by and statistics logic
        #find the  share data buffer used for filtering ,group by and statistics
        int_buffer = None
        float_buffer = None
        string_buffer = None
        column_data = None
        #A map to show how to use the share data buff in filtering, group by and resultset.
        #key will be the id of the member from filtering or group by or resultset if the column included in the list member will use the databuff;
        #value is the data buff(int_buffer or float_buffer or string_buffer) which the column should use.
        data_buffers = {}

        #the varialbe for the filter result.
        cond_result = None

        #conditions are applied one by one, so it is better to share the buffers among conditions
        if report_conditions:
            #apply the conditions, try to share the np array among conditions to save memory
            for item in report_conditions:
                col = column_map[item[0]]
                col_type = col[EXECUTOR_DTYPE]
                if datatransformer.is_int_type(col_type):
                    if int_buffer:
                        int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                    else:
                        int_buffer = [col_type,None]
                    data_buffers[id(item)] = int_buffer
                elif datatransformer.is_float_type(col_type):
                    if float_buffer:
                        float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                    else:
                        float_buffer = [col_type,None]
                    data_buffers[id(item)] = float_buffer
                elif datatransformer.is_string_type(col_type):
                    if string_buffer:
                        string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                    else:
                        string_buffer = [col_type,None]
                    data_buffers[id(item)] = string_buffer

        if report_group_by:
            #group_by feature is required
            #Use pandas to implement 'group by' feature, all dataset including columns in 'group by' and 'resultset' will be loaded into the memory.
            #use the existing data buff if it already exist for some column
            if data_buffers:
                closest_int_column = [None,None,None] #three members (the item with lower type, the item with exact type,the item with upper type), each memeber is None or list with 2 members:[ group_by or resultset item,col_type]
                closest_float_column = [None,None,None]
                closest_string_column = [None,None,None]
                for item in itertools.chain(report_group_by,resultset):
                    colname = item[0] if isinstance(item,list) else item
                    if colname == "*":
                        continue
                    col = column_map[colname]
                    col_type = col[EXECUTOR_DTYPE]
                    for is_func,data_buffer,closest_column in (
                        (datatransformer.is_int_type,int_buffer,closest_int_column),
                        (datatransformer.is_float_type,float_buffer,closest_float_column),
                        (datatransformer.is_string_type,string_buffer,closest_string_column)):
                        if is_func(col_type):
                            if data_buffer:
                                if closest_column[1] and closest_column[1][1] == data_buffer[0]:
                                    #already match exactly
                                    continue
                                elif data_buffer[0] == col_type:
                                    #match exactly
                                    closest_column[1] = [item,col_type]
                                else:
                                    t = datatransformer.ceiling_type(data_buffer[0],col_type)
                                    if t == data_buffer[0]:
                                        #the int_buffer can hold the current report set column, resultset column's type is less then int_buffer's type
                                        #choose the column which is closest to int_buffer type
                                        if closest_column[0]:
                                            if closest_column[0][1] < col_type:
                                                closest_column[0][1] = [item,col_type]
                                        else:
                                            closest_column[0] = [item,col_type]
                                    elif t == col_type:
                                        #the resultset column can hold the int_buffer data, resultset column's type is greater then int_buffer's type
                                        #choose the column which is closest to int_buffer type
                                        if closest_column[2]:
                                            if closest_column[2][1] > col_type:
                                                closest_column[2][1] = [item,col_type]
                                        else:
                                            closest_column[2] = [item,col_type]
                                    else:
                                        #both the resultset column and in_buff can't hold each other,the result type is greater then int_buffer's type and resultset column type
                                        #choose the column which is closest to int_buffer type except the current chosed column's type can hold int_buffer data
                                        if closest_column[2]:
                                            if datatransformer.ceiling_type(data_buffer[0],closest_column[2][1]) == closest_column[2][1]:
                                                #the current chosed column's type can hold int_buffer data
                                                continue
                                            elif closest_column[2][1] > col_type:
                                                closest_column[2][1] = [item,col_type]
                                        else:
                                            closest_column[2] = [item,col_type]
                            break



                #choose the right column to share the data buffer chosed in report conditions
                for data_buffer,closest_column in (
                    (int_buffer,closest_int_column),
                    (float_buffer,closest_float_column),
                    (string_buffer,closest_string_column)):
                    if closest_column[1]:
                        #one column in group_by or resultset has the same type as int_buffer
                        data_buffers[id(closest_column[1][0])] = data_buffer
                    elif closest_column[2] and datatransformer.ceiling_type(data_buffer[0],closest_column[2][1]) == closest_column[2][1]:
                        #one column in group_by or resultset has a data type which is bigger than int_buffer
                        data_buffer[0] = closest_column[2][1]
                        data_buffers[id(closest_column[2][0])] = data_buffer
                    elif closest_column[0] and datatransformer.ceiling_type(data_buffer[0],closest_column[0][1]) == closest_column[0][1]:
                        #one column in group_by or resultset has a data type which is less than int_buffer
                        data_buffers[id(closest_column[0][0])] = data_buffer
                    elif closest_column[0]:
                        #choose the column whose type is less than int_buffer but closest to int_buffer
                        data_buffers[id(closest_int_column[0][0])] = data_buffer
                    elif closest_column[2]:
                        #choose the column whose type is greater than int_buffer but closest to int_buffer
                        data_buffer[0] = closest_column[2][1]
                        data_buffers[id(closest_column[2][0])] = data_buffer

        elif isinstance(resultset,(list,tuple)):
            #group_by feature is not required.
            #perform the statistics one by one, try best to share the data buffer
            for item in resultset:
                if item[0] == "*":
                    continue
                col = column_map[item[0]]
                col_type = col[EXECUTOR_DTYPE]
                if datatransformer.is_int_type(col_type):
                    if int_buffer:
                        int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                    else:
                        int_buffer = [col_type,None]
                    data_buffers[id(item)] = int_buffer
                elif datatransformer.is_float_type(col_type):
                    if float_buffer:
                        float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                    else:
                        float_buffer = [col_type,None]
                    data_buffers[id(item)] = float_buffer
                elif datatransformer.is_string_type(col_type):
                    if string_buffer:
                        string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                    else:
                        string_buffer = [col_type,None]
                    data_buffers[id(item)] = string_buffer

        with h5py.File(data_index_file,'r') as index_h5:
            #filter the dataset
            if report_conditions:
                #apply the conditions, try to share the np array among conditions to save memory
                previous_item = None
                for cond in report_conditions:
                    #each condition is a tuple(column, operator, value), value is dependent on operator and column type
                    col = column_map[cond[0]]

                    #"select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(datasetid))
                    #map the value to internal value used by dataset if it is not mapped in the driver
                    if cond[3]:
                        #value is not mapped or partly mapped.
                        if isinstance(cond[2],list):
                            for i in cond[3]:
                                if col[EXECUTOR_TRANSFORMER]:
                                    #need transformation
                                    if datatransformer.is_enum_func(col[EXECUTOR_TRANSFORMER]):
                                        #is enum type
                                        cond[2][i] = datatransformer.get_enum(cond[2][i],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID])
                                        if not cond[2]:
                                            #searching value doesn't exist
                                            cond[2][i] = None
                                            break
                                    else:
                                        if col[EXECUTOR_COLUMNINFO] and col[EXECUTOR_COLUMNINFO].get("parameters"):
                                            cond[2][i] = datatransformer.transform(col[EXECUTOR_TRANSFORMER],cond[2][i],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID],**col[EXECUTOR_COLUMNINFO]["parameters"])
                                        else:
                                            cond[2][i] = datatransformer.transform(col[EXECUTOR_TRANSFORMER],cond[2][i],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID])
                            if any(v is None for v in cond[2]):
                                #remove the None value from value list
                                cond[2] = [v for v in cond[2] if v is not None]
                            if not cond[2]:
                                #searching value doesn't exist
                                cond_result = None
                                break
                        else:
                            if col[EXECUTOR_TRANSFORMER]:
                                #need transformation
                                if datatransformer.is_enum_func(col[EXECUTOR_TRANSFORMER]):
                                    #is enum type
                                    cond[2] = datatransformer.get_enum(cond[2],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID])
                                    if not cond[2]:
                                        #searching value doesn't exist
                                        cond_result = None
                                        break
                                else:
                                    if col[EXECUTOR_COLUMNINFO] and col[EXECUTOR_COLUMNINFO].get("parameters"):
                                        cond[2] = datatransformer.transform(col[EXECUTOR_TRANSFORMER],cond[2],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID],**col[EXECUTOR_COLUMNINFO]["parameters"])
                                    else:
                                        cond[2] = datatransformer.transform(col[EXECUTOR_TRANSFORMER],cond[2],databaseurl=databaseurl,columnid=col[EXECUTOR_COLUMNID])

                    if not previous_item or previous_item[0] != cond[0]:
                        #condition is applied on different column
                        try:
                            buff = data_buffers.pop(id(cond))
                            buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                            column_data = buff[1]
                        except KeyError as ex:
                            column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col[EXECUTOR_DTYPE]))
                        
                        index_h5[col[EXECUTOR_COLUMNNAME]].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
                    if cond_result is None:
                        cond_result = operation.get_func(col[EXECUTOR_DTYPE],cond[1])(column_data,cond[2])
                    else:
                        cond_result &= operation.get_func(col[EXECUTOR_DTYPE],cond[1])(column_data,cond[2])

                    previous_item = cond

                column_data = None

                if cond_result is None:
                    filtered_rows = 0
                else:
                    filtered_rows = np.count_nonzero(cond_result)
            else:
                filtered_rows = dataset_size


            if resultset == "__details__":
                #return the detail logs
                if filtered_rows == 0:
                    logger.debug("No data found.file={}, report condition = {}".format(data[1],report_conditions))
                    return [(data[0],data[1],0,None)]
                elif filtered_rows == dataset_size:
                    #all logs are returned
                    #unlikely to happen.
                    report_file = os.path.join(cache_dir,"reports","{0}-{2}-{3}{1}".format(*os.path.splitext(data[1]),reportid,data[0]))
                    shutil.copyfile(data_file,report_file)
                    return [(data[0],data[1],dataset_size,report_file)]
                else:
                    report_size = np.count_nonzero(cond_result)
                    indexes = np.flatnonzero(cond_result)
                    report_file_folder = os.path.join(cache_dir,"reports","tmp")
                    utils.mkdir(report_file_folder)
                    report_file = os.path.join(report_file_folder,"{0}-{2}-{3}{1}".format(*os.path.splitext(data[1]),reportid,data[0]))
                    with open(report_file,'w') as report_f:
                        reportwriter = csv.writer(report_f)
                        reportwriter.writerows(report_details(data_file,indexes))
                    return [(data[0],data[1],report_size,report_file)]

            if report_group_by :
                #'group by' enabled
                #create pandas dataframe
                if filtered_rows == 0:
                    result = []
                else:
                    df_datas = collections.OrderedDict()
                    if report_type == HOURLY_REPORT:
                        df_datas["__request_time__"] = dataset_time.strftime("%Y-%m-%d %H:00:00")
                    elif report_type == DAILY_REPORT:
                        df_datas["__request_time__"] = dataset_time.strftime("%Y-%m-%d 00:00:00")
    
                    for item in itertools.chain(report_group_by,resultset):
                        colname = item[0] if isinstance(item,(list,tuple)) else item
                        if colname == "*":
                            continue
                        col = column_map[colname]
                        col_type = col[EXECUTOR_DTYPE]
                        try:
                            buff = data_buffers.pop(id(item))
                            buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                            column_data = buff[1]
                        except KeyError as ex:
                            column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col_type))
                            
                        index_h5[colname].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
                        if filtered_rows == dataset_size:
                            #all records are satisfied with the report condition
                            df_datas[colname] = column_data
                        else:
                            df_datas[colname] = column_data[cond_result]
    
                    #create pandas dataframe
                    df = pd.DataFrame(df_datas)
                    #get the group object
                    if report_type:
                        df_group = df.groupby(["__request_time__",*report_group_by],group_keys=True)
                    else:
                        df_group = df.groupby(report_group_by,group_keys=True)
                    #perfrom the statistics on group
                    #populate the statistics map
                    statics_map = collections.OrderedDict()
                    for item in resultset:
                        if item[0] == "*":
                            #use the first group by column to calculate the count
                            colname = report_group_by[0]
                        else:
                            colname = item[0]
                        if colname in statics_map:
                            statics_map[colname].append(operation.get_agg_func(item[1]))
                        else:
                            statics_map[colname] = [operation.get_agg_func(item[1])]
                    df_result = df_group.agg(statics_map)

                    logger.debug("columns = {}".format(df_result.columns))

                    result =  [d for d in zip(df_result.index, zip(*[df_result[c] for c in df_result.columns]))]
            else:
                #no 'group by', return the statistics data.
                if filtered_rows == 0:
                    report_data = [0] * len(resultset)
                    if report_type == HOURLY_REPORT:
                        report_data.insert(0,dataset_time.strftime("%Y-%m-%d %H:00:00"))
                else:
                    if report_type == HOURLY_REPORT:
                        report_data = [dataset_time.strftime("%Y-%m-%d %H:00:00")]
                    else:
                        report_data = []
                    previous_item = None
                    for item in resultset:
                        if not previous_item or previous_item[0] != item[0]:
                            #new column should be loaded
                            previous_item = item
                            if item[0] != "*":
                                col = column_map[item[0]]
                                col_type = col[EXECUTOR_DTYPE]
                                try:
                                    buff = data_buffers.pop(id(item))
                                    buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                                    column_data = buff[1]
                                except KeyError as ex:
                                    column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col_type))
                            
                                index_h5[item[0]].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
    
                        if item[0] == "*":
                            report_data.append(filtered_rows)
                        elif filtered_rows == dataset_size:
                            report_data.append(operation.get_func(col[2],item[1])(column_data))
                        else:
                            report_data.append(operation.get_func(col[2],item[1])(column_data[cond_result]))

                if report_type == DAILY_REPORT:
                    #return a dict to perform the function 'reducebykey'
                    result = [(dataset_time.strftime("%Y-%m-%d 00:00:00"),report_data)]
                else:
                    result = [report_data]
                    
            #logger.debug("Return the result from executor.reportid={0}, access log file={0}, result={2}".format(reportid,data[1],result))
            return result
    return analysis

DRIVER_COLUMNID=0
DRIVER_DTYPE=1
DRIVER_TRANSFORMER=2
DRIVER_COLUMNINFO=3
DRIVER_STATISTICAL=4
DRIVER_FILTERABLE=5
DRIVER_GROUPABLE=6

def merge_reportresult_factory(resultset):
    """
    Return a functon which can be used by reduce and reducebykey to merge the results returned by spark's map function.
    """
    def _merge(data1,data2):
        if data1 is None:
            return data2
        elif data2 is None:
            return data1
        elif isinstance(data1,list):
            #data1 is list object
            pass
        elif isinstance(data2,list):
            #swap data1 and data to guarantee data1 is list object
            tmp = data1
            data1 = data2
            data2 = tmp
        else:
            #convert data1 to list
            data1 = list(data1)

        for i in range(len(resultset)):
            data1[i] = operation.get_merge_func(resultset[i][1])(data1[i],data2[i])
        
        return data1

    return _merge

def sort_group_by_result_factory(databaseurl,column_map,report_group_by,resultset,report_sort_by):
    """
    Return a function which is used to sort the group by result.
    Desc is only supported for number column including resultset(all columns in resultset are statistical number column and groupe column in report group-by data
    params:
        column_map: a map between column name and [columnid,dtype,transformer,statistical,filterable,groupable]
    """
    #find the sort indexes in group columns and resultset columns
    #find whether the data should be converted or not before sort
    sort_indexes = [] # for group by column: a tuple (group_by index or resultset index,columnid,asc?), for resultset column: a tuple(resultset index, asc?)
    
    for item in report_sort_by:
        try:
            i = report_group_by.index(item[0])
            if item[0] == "__request_time__":
                #default sort order for __request_time__ is asc.
                sort_indexes.append((i,None,True))
                continue
            col = column_map[item[0]]
                
            #sort by the enum key only if the column is a enum type but not a enum group type
            colid = col[DRIVER_COLUMNID]  if col[DRIVER_TRANSFORMER] and datatransformer.is_enum_func(col[DRIVER_TRANSFORMER]) and not datatransformer.is_group_func(col[DRIVER_TRANSFORMER]) else None
            sort_indexes.append((i,colid,True if colid else item[1]))
        except ValueError as ex:
            #not in group by columns
            for i in range(len(resultset)):
                if resultset[i][2] == item:
                    sort_indexes.append((i,item[1]))
                    break
        except:
            pass

    if len(sort_indexes) == 1:
        #for single sort column, flat the indexes
        sort_indexes = sort_indexes[0]

    def _sort_with_single_key_column(data):
        #use one of the group by column to sort
        #group by keys can be string or a list or tuple
        if sort_indexes[0] == 0:
            if isinstance(data[0],(list,tuple)):
                sort_val = data[0][0]
            else:
                sort_val = data[0]
        else:
            sort_val = data[0][sort_indexes[0]]

        if sort_indexes[1]:
            return datatransformer.get_enum_key(sort_val,databaseurl=databaseurl,columnid=sort_indexes[1])
        elif sort_indexes[2]:
            #use the value to sort, asc
            return sort_val
        else:
            #use the value to sort, desc
            return sort_val * -1

    def _sort_with_single_value_column(data):
        #use the one of the value column to sort
        return data[1][sort_indexes[0]] if sort_indexes[1] else (-1 * data[1][sort_indexes[0]])

    def _sort(data):
        result = [None] * len(sort_indexes)
        pos = 0
        for item in sort_indexes:
            if len(item) == 3:
                #a group by column
                #group by keys can be string or a list or tuple
                if item[0] == 0:
                    if isinstance(data[0],(list,tuple)):
                        sort_val = data[0][0]
                    else:
                        sort_val = data[0]
                else:
                    sort_val = data[0][item[0]]

                if item[1]:
                    #use the key to sort
                    result[pos] = datatransformer.get_enum_key(sort_val,databaseurl=databaseurl,columnid=item[1])
                elif item[2]:
                    #use the value to sort ,asc
                    result[pos] = sort_val
                else:
                    #use the value to sort ,desc
                    result[pos] = sort_val * -1
            elif item[1]: 
                #a  data column, asc
                result[pos] = data[1][item]
            else:
                #a  data column, desc
                result[pos] = data[1][item] * -1
            pos += 1

        return result

    if isinstance(sort_indexes,tuple):
        return _sort_with_single_key_column
    elif isinstance(sort_indexes,list):
        return _sort
    else:
        return _sort_with_single_value_column

def _group_by_key_iterator(keys):
    """
    return an iterator of group keys, (group keys can be a list or a single string
    """
    if isinstance(keys,(list,tuple)):
        for k in keys:
            yield k
    else:
        yield keys

def _group_by_data_iterator(keys,databaseurl,enum_colids):
    """
    A iterator to convert the enum value to enum key if required
    The transformation is only happened for group by columns
    """
    i = 0
    for k in keys:
        if enum_colids[i]:
            yield datatransformer.get_enum_key(k,databaseurl=databaseurl,columnid=enum_colids[i])
        else:
            yield k
        i += 1

def group_by_raw_report_iterator(report_result,databaseurl,enum_colids):
    """
    Return a iterator to iterate the group by report_result as a list data which contain the group by column data and value data, also convert the gorup by column data from enumid to enum key if required.
    params:
        report_result: a iterator of tuple(keys, values)
        enum_colids: a list with len(report_group_by) or len(keys), the corresponding memeber is column id if the related column need to convert into keys; otherwise the 
    """
    if enum_colids:
        #converting the enum id to enum key is required
        for k,v in report_result:
            yield itertools.chain(_group_by_data_iterator(_group_by_key_iterator(k),databaseurl,enum_colids),v)
    else:
        #converting the enum id to enum key is not required
        for k,v in report_result:
            yield itertools.chain(_group_by_key_iterator(k),v)

def resultsetrow_iterator(row,original_resultset):
    """
    Return a iterator to iterate the raw resultset to generate a list data for report
    params:
        report_result: a iterator of tuple(keys, values)
    """
    #converting the enum id to enum key is not required
    for c in original_resultset:
        yield c[3](row)

def group_by_report_iterator(report_result,databaseurl,enum_colids,original_resultset):
    """
    Return a iterator to iterate the group by raw report_result to generate a list data for report
    params:
        report_result: a iterator of tuple(keys, values)
        enum_colids: a list with len(report_group_by) or len(keys), the corresponding memeber is column id if the related column need to convert into keys; otherwise the 
    """
    if enum_colids:
        #converting the enum id to enum key is required
        for k,v in report_result:
            yield itertools.chain(_group_by_data_iterator(_group_by_key_iterator(k),databaseurl,enum_colids),resultsetrow_iterator(v,original_resultset))
    else:
        #converting the enum id to enum key is not required
        for k,v in report_result:
            yield itertools.chain(_group_by_key_iterator(k),resultsetrow_iterator(v,original_resultset))

def resultset_iterator(report_result,original_resultset):
    """
    Return a iterator to iterate the raw resultset to generate a list data for report
    params:
        report_result: a iterator of tuple(keys, values)
    """
    #converting the enum id to enum key is not required
    for v in report_result:
        yield resultsetrow_iterator(v,original_resultset)

def get_report_data_factory(pos):
    """
    Return a method to get the column data from report data
    """
    def _func(row):
        return row[pos]

    return _func

def get_report_avg_factory(resultset,item):
    """
    Return a method to get the column avg from report data
    """
    count_pos = next(i for i in range(len(resultset)) if resultset[i][1] == "count")
    sum_pos = next(i for i in range(len(resultset)) if resultset[i][1] == "sum" and resultset[i][0] == item[0])

    def _func(row):
        if row[count_pos] <= 0:
            return 0
        else:
            return row[sum_pos] / row[count_pos]

    return _func

def get_column_data_4_sortby_factory(f_get_column_data,sort_type):

    def _func(data):
        if sort_type:
            return f_get_column_data(data[1])
        else:
            return -1 * f_get_column_data(data[1])

    return _func

def get_group_key_data_4_sortby_factory(databaseurl,report_group_by,pos,columnid,sort_type):
    def _func1(data):
        """
        For single group-by column.
        the key data is not a list type
        """
        if columnid is None:
            return data[0]
        else:
            return datatransformer.get_enum_key(data[0],databaseurl=databaseurl,columnid=columnid)

    def _func2(data):
        """
        For multiple group-by columns.
        the key data is a list type
        """
        if columnid is None:
            return data[0][pos]
        else:
            return datatransformer.get_enum_key(data[0][pos],databaseurl=databaseurl,columnid=columnid)

    return _func1 if len(report_group_by) == 1 else _func2

def run():
    """
    The entry point of pyspark application
    """
    try:
        report_status = None
        #get environment variable passed by report 
        databaseurl = os.environ.get("DATABASEURL")
        if not databaseurl:
            raise Exception("Missing env variable 'DATABASEURL'")
    
        reportid = os.environ.get("REPORTID")
        if reportid is None:
            raise Exception("Missing env variable 'REPORTID'")

        logger.debug("Begin to generate the report({})".format(reportid))
    
        column_map = {} #map between column name and [columnid,dtype,transformer,statistical,filterable,groupable]
        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select name,dataset_id,\"start\",\"end\",rtype,conditions,\"group_by\",\"sort_by\",resultset,status from datascience_report where id = {}".format(reportid))
                report = cursor.fetchone()
                if report is None:
                    raise Exception("Report({}) doesn't exist.".format(reportid))
                report_name,datasetid,report_start,report_end,report_type,report_conditions,report_group_by,report_sort_by,resultset,report_status = report
                if report_status and report_status.get("status") == "Succeed":
                    #already succeed
                    report_status = None
                    return
                elif report_status is None:
                    report_status = {}

                if report_sort_by:
                    #convert the sort type from string to bool
                    for item in report_sort_by:
                        item[1] = True if item[1] == "asc" else False
    
                cursor.execute("select name,datasetinfo from datascience_dataset where id = {}".format(datasetid))
                dataset = cursor.fetchone()
                if dataset is None:
                    raise Exception("Dataset({}) doesn't exist.".format(datasetid))
                dataset_name,dataset_info = dataset

                cursor.execute("select name,id,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} ".format(datasetid))
                for row in cursor.fetchall():
                    column_map[row[0]] = [row[1],row[2],row[3],row[4],row[5],row[6],row[7]]

                report_status["status"] = "Running"
                cursor.execute("update datascience_report set status='{1}',exec_start='{2}',exec_end=null where id = {0}".format(reportid,json.dumps(report_status),timezone.dbtime()))
                conn.commit()

        if report_type == DAILY_REPORT:
            #for daily report, the minimum time unit of report_start and report_end(included) is day; if it is not, set hour,minute,second and microsecond to 0
            report_start = timezone.localtime(report_start)
            if report_start.hour or report_start.minute or report_start.second or report_start.microsecond:
                report_start = report_start.replace(hour=0,minute=0,second=0,microsecond=0)
    
            report_end = timezone.localtime(report_end)
            if report_end.hour or report_end.minute or report_end.second or report_end.microsecond:
                report_end = report_end.replace(hour=0,minute=0,second=0,microsecond=0)
            #because report_end is included, so set report_end to next day
            report_end  += timedelta(days=1)
        else:
            #the minimum time unit of report_start and report_end(included) is hour; if it is not, set minute,second and microsecond to 0
            report_start = timezone.localtime(report_start)
            if report_start.minute or report_start.second or report_start.microsecond:
                report_start = report_start.replace(minute=0,second=0,microsecond=0)
    
            report_end = timezone.localtime(report_end)
            if report_end.minute or report_end.second or report_end.microsecond:
                report_end = report_end.replace(minute=0,second=0,microsecond=0)
            #because report_end is included, so set report_end to next hour
            report_end  += timedelta(hours=1)
    
        #populate the list of nginx access log file
        datasets = []
        dataset_time = report_start
        while dataset_time < report_end:
            if not dataset_info.get("filepattern"):
                raise Exception("Missing the config item 'filepattern' in datasetinfo, which is used to construct the nginx access log file based on datetime")
            datasets.append((dataset_time.strftime("%Y%m%d%H"),dataset_time.strftime(dataset_info.get("filepattern"))))
            dataset_time += timedelta(hours=1)

        no_data = False
        #sort the report_conditions
        if report_conditions:
            report_conditions.sort()
            #try to map the value to internal value used by dataset
            #Append a member to each cond to indicate the mapping status: if the member is False, value is mapped or no need to map; value is True or the indexes of the data which need to be mapped.
            for cond in report_conditions:
                #each condition is a tuple(column, operator, value), value is dependent on operator and column type
                col = column_map[cond[0]]
                col_type = col[DRIVER_DTYPE]

                #"select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(datasetid))
                #map the value to internal value used by dataset
                cond[2] = cond[2].strip() if cond[2] else cond[2]
                cond[2] = json.loads(cond[2])
                cond.append(False)
                if isinstance(cond[2],list):
                    for i in range(len(cond[2])):
                        if col[DRIVER_TRANSFORMER]:
                            #need transformation
                            if datatransformer.is_enum_func(col[DRIVER_TRANSFORMER]):
                                #is enum type
                                cond[2][i] = datatransformer.get_enum(cond[2][i],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID])
                                if cond[2] is None:
                                    #searching value doesn't exist, try to map it in executor
                                    if cond[3] is False:
                                        cond[3] = [i]
                                        logger.debug("The value({2}) condition({1}) of report({0}) is not resolved in drvier, let executor try again".format(reportid,cond,cond[2][i]))
                                    else:
                                        cond[3].append(i)
                            else:
                                if col[DRIVER_COLUMNINFO] and col[DRIVER_COLUMNINFO].get("parameters"):
                                    cond[2][i] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2][i],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID],**col[DRIVER_COLUMNINFO]["parameters"])
                                else:
                                    cond[2][i] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2][i],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID])
                        elif datatransformer.is_string_type(col[DRIVER_DTYPE]):
                            #encode the string value
                            cond[2][i] = (cond[2][i] or "").encode()
                        elif datatransformer.is_int_type(col[DRIVER_DTYPE]):
                            cond[2][i] = int(cond[2][i]) if cond[2][i] or cond[2][i] == 0 else None
                        elif datatransformer.is_float_type(col[DRIVER_DTYPE]):
                            cond[2][i] = float(cond[2][i]) if cond[2][i] or cond[2][i] == 0 else None
                        else:
                            raise Exception("Type({}) Not Supported".format(col[DRIVER_DTYPE]))

                else:
                    if col[DRIVER_TRANSFORMER]:
                        #need transformation
                        if datatransformer.is_enum_func(col[DRIVER_TRANSFORMER]):
                            #is enum type
                            cond[2] = datatransformer.get_enum(cond[2],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID])
                            if cond[2] is None:
                                #searching value doesn't exist, try to map it in executor
                                cond[3] = True
                                logger.debug("The value({2}) condition({1}) of report({0}) is not resolved in drvier, let executor try again".format(reportid,cond,cond[2]))
                        else:
                            if col[DRIVER_COLUMNINFO] and col[DRIVER_COLUMNINFO].get("parameters"):
                                cond[2] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID],**col[DRIVER_COLUMNINFO]["parameters"])
                            else:
                                cond[2] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2],databaseurl=databaseurl,columnid=col[DRIVER_COLUMNID])
                    elif datatransformer.is_string_type(col[DRIVER_DTYPE]):
                        #encode the string value
                        cond[2]= (cond[2] or "").encode()
                    elif datatransformer.is_int_type(col[DRIVER_DTYPE]):
                        cond[2]= int(cond[2]) if cond[2] or cond[2] == 0 else None
                    elif datatransformer.is_float_type(col[DRIVER_DTYPE]):
                        cond[2]= float(cond[2]) if cond[2] or cond[2] == 0 else None
                    else:
                        raise Exception("Type({}) Not Supported".format(col[DRIVER_DTYPE]))
        #if resultset contains a column '__all__', means this report will return acess log details, ignore other resultset columns
        #if 'count' is in resultset, change the column to * and also check whether resultset has multiple count column
        if not resultset:
            resultset = "__details__"
            original_resultset = None
        else:
            count_column = None
            count_column_required = False
            found_avg = False
            found_sum = False
            previous_item = None
            #backup the original resultset, deep clone
            original_resultset = [[c[0],c[1],c[2] if c[2] else ("{}-{}".format(c[0],c[1]) if c[1] else c[0])] for c in resultset]
            resultset.sort()
            for i in range(len(resultset) - 1,-1,-1):
                item = resultset[i]
                if previous_item and previous_item[0] != item[0]:
                    #new column
                    if found_avg and not found_sum:
                        #found column 'avg', but not found column 'sum',add a column 'sum'
                        resultset.insert(i + 1,[previous_item[0],"sum","{}_sum".format(previous_item[0])])
                    found_avg = False
                    found_sum = False
                if previous_item and previous_item[0] == item[0] and previous_item[1] == item[1]:
                    #this is a duplicate statistical column, delete it
                    del resultset[i]
                    continue

                #always use set previous_item to item to check whether the current column is the duplicate statistical column
                previous_item = item

                if item[0] == "__all__" :
                    #a  detail log report can't contain any statistics data.
                    resultset = "__details__"
                    break
                elif not item[1]:
                    raise Exception("Missing aggregation method on column({1}) for report({0})".format(reportid,item[0]))
                elif item[1] == "count":
                    #remove all count columns from resultset first, and add it later. 
                    if not count_column:
                        count_column = item
                        if not count_column[2]:
                            count_column[2] = "count"
                        count_column[0] = "*"
                    del resultset[i]
                    continue
                elif item[1] == "avg" :
                    #perform a avg on a list of avg data is incorrect, because each access log file has different records.
                    #so avg is always calcuated through summary and count
                    #delete the column 'avg' and will add a column 'sum' if required
                    found_avg = True
                    count_column_required = True
                    del resultset[i]
                elif item[1] == "sum" :
                    found_sum = True

                #use a standard column name for internal processing
                item[2] = "{}_{}".format(item[0],item[1])
                    
                col = column_map[item[0]]
                if not col[DRIVER_STATISTICAL]:
                    raise Exception("Can't apply aggregation method on non-statistical column({1}) for report({0})".format(reportid,item[0]))
                if report_group_by and item[0] in report_group_by:
                    raise Exception("Can't apply aggregation method on group-by column({1}) for report({0})".format(reportid,item[0]))
            
            if resultset != "__details__":
                if found_avg and not found_sum:
                    #found column 'avg', but not found column 'sum',add a column 'sum'
                    resultset.insert(0,[previous_item[0],"sum","{}_sum".format(previous_item[0])])
                resultset.sort()
                count_column_index = -1
                if count_column:
                    #add the column 'count' back to resultset
                    #use the first data column in resultset as the data column of count_column
                    count_column[0] = "*"
                    resultset.insert(0,count_column)
                    count_column_index = 0
                elif count_column_required:
                    #at least have one avg column, add a count column to implment avg feature
                    #column 'count' not found, add one
                    count_column = ["*","count","count"]
                    resultset.insert(0,count_column)
                    count_column_index = 0

        if resultset == "__details__":
            #this report will return all log details, report_group_by is meanless
            report_group_by = None
            report_sort_by = None
            report_type = None
        else:
            resultset.sort()

            if report_group_by:
                for item in report_group_by:
                    if item not in column_map:
                        raise Exception("The group-by column({1}) does not exist for report({0})".format(reportid,item))
                    if not column_map[item][DRIVER_GROUPABLE]:
                        raise Exception("The group-by column({1}) is not groupable for report({0})".format(reportid,item))
            else:
                #group_by is not enabled, all report data are statistical data,and only contains one row,
                #report sort by is useless
                report_sort_by = None

        spark = get_spark_session()
        rdd = spark.sparkContext.parallelize(datasets, len(datasets))
        #perform the analysis per nginx access log file
        logger.debug("Begin to generate the report({0}),report condition={1},report_group_by={2},report_sort_by={3},resultset={4},report_type={5}".format(reportid,report_conditions,report_group_by,report_sort_by,resultset,report_type))
        rdd = rdd.flatMap(analysis_factory(reportid,databaseurl,datasetid,dataset_info,report_start,report_end,report_conditions,report_group_by,resultset,report_type))

        #init the folder to place the report file
        cache_dir = dataset_info.get("cache")
        report_file_folder = os.path.join(cache_dir,"reports",report_start.strftime("%Y-%m-%d"))
        utils.mkdir(report_file_folder)
        if resultset == "__details__":
            result = rdd.collect()
            result.sort()
            if dataset_info.get("data_header"):
                report_header_file = os.path.join(cache_dir,"nginxaccesslog-report_header.csv")
                if not os.path.exists(report_header_file):
                    #report_header_file does not exist, create it
                    with open(report_header_file,'w') as f:
                        writer = csv.writer(f)
                        writer.writerow(dataset_info.get("data_header"))
            else:
                report_header_file = None

            result = [r for r in result if r[3]]
            if len(result) == 0:
                logger.debug("No data found")
                report_status["status"] = "Succeed"
                report_status["records"] = 0
                return 

            report_file = os.path.join(report_file_folder,"nginxaccesslog-report-{}{}".format(reportid,os.path.splitext(result[0][3])[1]))
            if report_header_file:
                #write the data header as report header
                files = [r[3] for r in result]
                files.insert(0,report_header_files)
                utils.concat_files(files,report_file)
                for r in result:
                    utils.remove_file(r[3])
            else:
                if len(result) == -1:
                    os.rename(result[0][3],report_file)
                else:
                    utils.concat_files([r[3] for r in result],report_file)
                    for r in result:
                        utils.remove_file(r[3])
            logger.debug("report file = {}".format(report_file))
            report_status["status"] = "Succeed"
            report_status["report"] = report_file
            if report_header_file:
                report_status["report_header"] = True
                report_status["records"] = utils.get_line_counter(report_file) - 1
            else:
                report_status["report_header"] = False
                report_status["records"] = utils.get_line_counter(report_file)
            return 
        else:
            report_raw_file = os.path.join(report_file_folder,"nginxaccesslog-report-{}-raw.csv".format(reportid))
            report_file = os.path.join(report_file_folder,"nginxaccesslog-report-{}.csv".format(reportid))
            if report_group_by:
                if report_type == HOURLY_REPORT:
                    #hourly report, each access log is one hour data, no need to reduce
                    #a new column is added to the group_by
                    report_result = rdd.collect()
                    report_group_by.insert(0,"__request_time__")
                    if report_sort_by:
                        report_sort_by.insert(0,["__request_time__",True])
                    else:
                        report_sort_by = [["__request_time__",True]]

                elif report_type == DAILY_REPORT:
                    #daily report, need to reduce the result
                    #a new column is added to the group_by
                    report_result = rdd.reduceByKey(merge_reportresult_factory(resultset)).collect()
                    report_group_by.insert(0,"__request_time__")
                    if report_sort_by:
                        report_sort_by.insert(0,["__request_time__",True])
                    else:
                        report_sort_by = [["__request_time__",True]]
                else:
                    report_result = rdd.reduceByKey(merge_reportresult_factory(resultset)).collect()
            else:
                if report_type == HOURLY_REPORT:
                    #hourly report, each access log is one hour data, no need to reduce
                    #a new column is added to resultset
                    report_result = rdd.collect()
                    resultset.insert(0,["__request_time__",None,"request_time"])
                    original_resultset.insert(0,["__request_time__",None,"request_time"])
                    #add a column 'request_time', adjust the value of count_column_index
                elif report_type == DAILY_REPORT:
                    #daily report, need to reduce the result
                    #the result is a map between day and value
                    #a new column is added to the group_by
                    report_result = rdd.reduceByKey(merge_reportresult_factory(resultset)).collect()
                    report_group_by = ["__request_time__"]
                    report_sort_by = [["__request_time__",True]]
                else:
                    report_result = [rdd.reduce(merge_reportresult_factory(resultset))]

            #find the logic to get the report row data from report raw row data
            for item in original_resultset:
                if item[1] == "avg":
                    item.append(get_report_avg_factory(resultset,item))
                elif item[1] == "count":
                    item.append(get_report_data_factory(next(i for i in range(len(resultset)) if resultset[i][1] == "count" )))
                else:
                    item.append(get_report_data_factory(next(i for i in range(len(resultset)) if resultset[i][0] == item[0] and resultset[i][1] == item[1] )))

            if report_group_by:
                #find the column ids for columns which will be converted from int value to enum key
                enum_colids = None
                for i in range(len(report_group_by)):
                    item = report_group_by[i]
                    if item == "__request_time__":
                        continue
                    colid = column_map[item][DRIVER_COLUMNID]  if column_map[item][DRIVER_TRANSFORMER] and datatransformer.is_enum_func(column_map[item][DRIVER_TRANSFORMER]) else None
                    if colid is not None:
                        if not enum_colids:
                            enum_colids = [None] * len(report_group_by)
                        enum_colids[i] = colid
                #sort the data if required
                if report_sort_by:
                    logger.debug("Before sort by = {},report_group_by={}".format(report_sort_by,report_group_by))
                    for i in range(len(report_sort_by) - 1,-1,-1): 
                        item = report_sort_by[i]
                        try:
                            #sort-by column is a group-by column
                            pos = report_group_by.index(item[0])
                            if item[0] == "__request_time__":
                                item.append(get_group_key_data_4_sortby_factory(databaseurl,report_group_by,pos,None,item[1]))
                            else:
                                col = column_map[item[0]]
                                if enum_colids and enum_colids[pos] and not datatransformer.is_group_func(col[DRIVER_TRANSFORMER]):
                                    item.append(get_group_key_data_4_sortby_factory(databaseurl,report_group_by,pos,enum_colids[pos],item[1]))
                                else:
                                    item.append(get_group_key_data_4_sortby_factory(databaseurl,report_group_by,pos,None,item[1]))
                            
                        except ValueError as ex:
                            #sort-by column is a resultset column
                            pos = next((i for i in range(len(original_resultset)) if original_resultset[i][2] == item[0] ),-1)
                            if pos == -1:
                                #invalid sorg-by column
                                del report_sort_by[i]
                            else:
                                item.append(get_column_data_4_sortby_factory(original_resultset[pos][3],item[1]))

                    logger.debug("sort by = {}".format(report_sort_by))

                    if report_sort_by:
                        if len(report_sort_by) == 1:
                            report_result = sorted(report_result,key=report_sort_by[0][2])
                        else:
                            report_result = sorted(report_result,key=lambda data:[ item[2](data) for item in report_sort_by])

            else:
                if report_type:
                    #sort by request_time
                    report_result.sort(key=lambda d:d[0])


            #save the report raw data to file and also convert the enumeration data back to string
            with open(report_raw_file, 'w', newline='') as f:
                writer = csv.writer(f)
                #writer header
                if report_group_by:
                    writer.writerow([("request_time" if c == "__request_time__" else c) for c in itertools.chain(report_group_by,[c[2] for c in resultset])])
                else:
                    writer.writerow([ c[2] or c[0] for c in resultset])
                #write rows
                if report_group_by:
                    #save the data
                    writer.writerows(group_by_raw_report_iterator(report_result,databaseurl,enum_colids))
                else:
                    #group_by is not enabled, all report data are statistical data
                    writer.writerows(report_result)
 
            #save the report to file and also convert the enumeration data back to string
            with open(report_file, 'w', newline='') as f:
                writer = csv.writer(f)
                #writer header
                if report_group_by:
                    writer.writerow([("request_time" if c == "__request_time__" else c) for c in itertools.chain(report_group_by,[c[2] for c in original_resultset])])
                else:
                    writer.writerow([ c[2] or c[0] for c in original_resultset])
                #write rows
                if report_group_by:
                    #save the data
                    writer.writerows(group_by_report_iterator(report_result,databaseurl,enum_colids,original_resultset))
                else:
                    #group_by is not enabled, all report data are statistical data
                    writer.writerows(resultset_iterator(report_result,original_resultset))

            report_status["status"] = "Succeed"
            report_status["report"] = report_file
            report_status["raw_report"] = report_raw_file
            report_status["report_header"] = True
            report_status["records"] = utils.get_line_counter(report_file) - 1
    except Exception as ex:
        msg = "Failed to generate the report.report={}.{}".format(reportid,traceback.format_exc())
        logger.error(msg)
        report_status["status"] = "Failed"
        report_status["message"] = base64.b64encode(msg.encode()).decode()
        raise 
    finally:
        if report_status:
            with database.Database(databaseurl).get_conn(True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("update datascience_report set status='{1}',exec_end='{2}' where id = {0}".format(reportid,json.dumps(report_status),timezone.dbtime()))
                    conn.commit()



if __name__ == "__main__":
    run()
        
