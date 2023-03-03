import logging
import traceback
import random
import os
import importlib
import itertools
import collections
import json
import tempfile
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

def get_harvester(datasetinfo):
    #prepare the dataset
    harvester_config = datasetinfo.get("harvester")
    if not harvester_config:
        raise Exception("Nissing the configuration 'harvester'")
    harvester_config["name"]
    return harvester.get_harvester(harvester_config["name"],**harvester_config["parameters"])

def filter_factory(filters,excludes):
    def _exclude(val):
        return not excludes(val)

    def _filters(val):
        for f in filters:
            if f(val):
                return True
        return False
    
    def _excludes(val):
        for f in excludes:
            if f(val):
                return False
        return True

    def _filters_and_excludes(val):
        for f in excludes:
            if f(val):
                return False

        for f in filters:
            if f(val):
                return True

        return False

    
    if not filters and not excludes:
        return None
    elif filters and excludes:
        if not isinstance(filters,list):
            filters = [filters]
        if not isinstance(excludes,list):
            excludes = [excludes]

        for i in range(len(filters)):
            filters[i] = eval(filters[i])

        for i in range(len(excludes)):
            excludes[i] = eval(excludes[i])

        return _filters_and_excludes
    elif filters:
        if isinstance(filters,list):
            if len(filters) == 1:
                filters = filters[0]
                filters = eval(filters)
                return filters
            else:
                for i in range(len(filters)):
                    filters[i] = eval(filters[i])
                return _filters
        else:
            filters = eval(filters)
            return filters
    else:
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

def merge_reportresult_factory(reportset):

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

        for i in range(len(reportset)):
            data1[i] = operation.get_merge_func(reportset[i][1])(data1[i],data2[i])
        
        return data1

    return _merge

def sort_group_by_result_factory(databaseurl,column_map,report_group_by,reportset,report_sort_by):
    """
    column_map between column name and [columnid,dtype,transformer,statistical,filterable,groupable]
    """
    #find the sort indexes in group columns and reportset columns
    #find whether the data should be converted or not before sort

    sort_indexes = []
    
    for item in report_sort_by:
        try:
            i = report_group_by.index(item)
            #get the column data if tranformer is required
            col = column_map[item][0]  if column_map[item][2] and datatransformer.is_enum_func(column_map[item][2]) else None
            sort_indexes.append((i,col))
        except ValueError as ex:
            #not in group by columns
            for i in range(len(reportset)):
                if reportset[i][2] == item:
                    sort_indexes.append(i)

    if len(sort_indexes) == 1:
        #for single sort column, flat the indexes
        sort_indexes = sort_indexes[0]

    def _sort_with_single_key_column(data):
        #use the key to sort
        if sort_indexes[1]:
            return datatransformer.get_enum_key(data[0][sort_indexes[0]],databaseurl=databaseurl,columnid=sort_indexes[1]]
        else:
            #use the value to sort
            return data[0][sort_indexes[0]]

    def _sort_with_single_value_column(data):
        #use the value to sort
        return data[1][sort_indexes]

    def _sort(data):
        result = [None] * len(sort_indexes)
        pos = 0
        for item in sort_indexes:
            if isinstance(item,tuple):
                #a  group by column
                if item[1]:
                    #use the key to sort
                    result[pos] = datatransformer.get_enum_key(data[0][item[0]],databaseurl=databaseurl,columnid=item[1]]
                else:
                    #use the value to sort
                    result[pos] = data[0][item[0]]
            else: 
                #a  data column
                result[pos] = data[1][item]
            pos += 1

        return result

    if isinstance(sort_indexes,tuple):
        return _sort_with_single_key_column
    elif isinstance(sort_indexes,list):
        return _sort
    else:
        return _sort_with_single_value_column

def _group_by_data_iterator(keys,databaseurl,enum_colids):
    """
    A iterator to convert the enum value to enum key if required
    The transformation is only happened for group by columns
    """
    for i in  len(keys):
        if enum_colids[i]:
            return datatransformer.get_enum_key(keys[i],databaseurl=databaseurl,columnid=enum_colids[i]]
        else:
            yield keys[i]

def group_by_report_iterator(report_result,databaseurl,enum_colids):
    """
    report_result: a iterator of tuple(keys, values)
    enum_colids: a list with len(report_group_by) or len(keys), the corresponding memeber is column id if the related column need to convert into keys; otherwise the 
    """
    if enum_colids:
        for k,v in report_result:
            yield itertools.chain(_group_by_data_iterator(k,databaseurl,enum_colids),v)
    else:
        for k,v in report_result:
            yield itertools.chain(k,v)

def analysis_factory(reportid,databaseurl,datasetid,datasetinfo,report_start,report_end,report_conditions,report_group_by,reportset,report_type):
    def analysis(data):
        import h5py
        import numpy as np
        import pandas as pd
        cache_dir = datasetinfo.get("cache")
        if not cache_dir:
            raise Exception("Nissing the configuration 'cache_dir'")

        #load the dataset columns
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
                            #initialize the column's filters
                            columns[0] = filter_factory(columns[0][0],columns[0][1])
                        if d[0] == -1:
                            #the end flag
                            break

                        previous_columnindex = d[0]
                        columns = [[d[5].get("filter") if d[5] else None,d[5].get("exclude") if d[5] else None],[(d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8])]]
                        allreportcolumns[d[0]] = columns
                    else:
                        columns[1].append((d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8]))
                        if d[5]:
                            if d[5].get("filter"):
                                if columns[0][0]:
                                    if isinstance(columns[0][0],list):
                                        columns[0][0].append(d[5].get("filter"))
                                    else:
                                        columns[0][0] = [columns[0][0],d[5].get("filter")]
                                else:
                                    columns[0][0] = d[5].get("filter")

                            if d[5].get("exclude"):
                                if columns[0][1]:
                                    if isinstance(columns[0][1],list):
                                        columns[0][1].append(d[5].get("exclude"))
                                    else:
                                        columns[0][1] = [columns[0][1],d[5].get("exclude")]
                                else:
                                    columns[0][1] = d[5].get("exclude")


        dataset_time = timezone.parse(data[0],"%Y%m%d%H")
        logger.debug("dataset_time = {}, str={}".format(dataset_time,data[0]))

        cache_folder = os.path.join(cache_dir,dataset_time.strftime("%Y-%m-%d"))
        utils.mkdir(cache_folder)

        harvester = None
        data_file = os.path.join(cache_folder,data[1])

        data_index_file = os.path.join(cache_folder,"{}.hdf5".format(data[1]))

        src_data_file = None
        if not os.path.exists(data_file):
            #local data file doesn't exist, find the source file path, and also remove the index file
            harvester = get_harvester(datasetinfo)
            if harvester.is_local():
                src_data_file = harvester.get_abs_path(data[1])
            else:
                with tempfile.NamedTemporaryFile(prefix="datascience_ngx_log",delete=False) as f:
                    src_data_file = f.name
                harvester.saveas(data[1],src_data_file)

            if os.path.exists(data_index_file):
                utils.remove_file(data_index_file)

        dataset_size = 0
        if os.path.exists(data_index_file):
            try:
                with h5py.File(data_index_file,'r') as index_file:
                    for columnindex,reportcolumns in allreportcolumns.items():
                        for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                            if  not column_filterable and not column_groupable and not column_statistical:
                                continue
                            dataset_size = index_file[column_name].shape[0]

            except:
                #some dataset does not exist or file is corrupted.
                utils.remove_file(data_index_file)

        if os.path.exists(data_index_file):
            logger.debug("The index file({1}) is already generated for data file({0})".format(data_file,data_index_file))
        else:
            with FileLock(os.path.join(cache_folder,"{}.lock".format(data[1])),120) as lock:
                if os.path.exists(data_index_file):
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
                    dataset_size = utils.get_line_counter(src_data_file or data_file)
                    logger.debug("The file({}) has {} records".format(src_data_file,dataset_size))
                    #generate index file
                    tmp_index_file = "{}.tmp".format(data_index_file)
                    excluded_rows = 0
                    context={
                        "dataset_time":dataset_time
                    }
                    if src_data_file:
                        #should get the data from source 
                        tmp_data_file = "{}.tmp".format(data_file)
                        data_f = open(tmp_data_file,'w')
                        logwriter = csv.writer(data_f)
                    else:
                        #found the cached file, get the data from local cached file
                        tmp_data_file = None
                        data_f = None
                        logwriter = None

                    try:
                        with h5py.File(tmp_index_file,'w') as tmp_h5:
                            buffer_size = 10000
                            with open(src_data_file or data_file) as f:
                                logreader = csv.reader(f)
                                dataset_baseindex = 0
                                buff_index = 0
                                datasets = {}
                                databuffs = {}
    
                                for request in logreader:
                                    #check the filter first
                                    if src_data_file:
                                        #data are retrieved from source, should execute the filter logic
                                        excluded = False
                                        for columnindex,reportcolumns in allreportcolumns.items():
                                            value = request[columnindex]
                                            excluded = False
                                            if  reportcolumns[0] and not reportcolumns[0](value):
                                                #excluded
                                                excluded = True
                                                break
                                        if excluded:
                                            excluded_rows += 1
                                            continue
                                        #data are retrieved from source,cache the data 
                                        logwriter.writerow(request)
                                        
                                    for columnindex,reportcolumns in allreportcolumns.items():
                                        value = request[columnindex]
                                        for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                            if  not column_filterable and not column_groupable and not column_statistical:
                                                continue
                
                                            #create the buffer and hdf5 dataset for column
                                            if dataset_baseindex == 0 and buff_index == 0:
                                                datasets[column_name] = tmp_h5.create_dataset(column_name, (dataset_size,),dtype=datatransformer.get_hdf5_type(column_dtype))
                                                databuffs[column_name] = np.empty((buffer_size,),dtype=datatransformer.get_np_type(column_dtype))
                
                                            #save the transformed column data to data buff
                                            if column_transformer:
                                                if column_columninfo and column_columninfo.get("parameters"):
                                                    databuffs[column_name][buff_index] = datatransformer.transform(column_transformer,value,databaseurl=databaseurl,columnid=column_columnid,context=context,**column_columninfo["parameters"])
                                                else:
                                                    databuffs[column_name][buff_index] = datatransformer.transform(column_transformer,value,databaseurl=databaseurl,columnid=column_columnid,context=context)
                                            else:
                                                databuffs[column_name][buff_index] = value.strip() if value else ""
                
                                            if buff_index == buffer_size - 1:
                                                #buff is full, write to hdf5 file
                                                try:
                                                    datasets[column_name].write_direct(databuffs[column_name],np.s_[0:buffer_size],np.s_[dataset_baseindex:dataset_baseindex + buffer_size])
                                                except Exception as ex:
                                                    logger.debug("Failed to write {2} records to dataset({1}) which are save in hdf5 file({0}).{3}".format(tmp_index_file,column_name,buffer_size,str(ex)))
                                                    raise
    
                                                lock.renew()
                
                                    buff_index += 1
                                    if buff_index == buffer_size:
                                        #buff is full, data is already saved to hdf5 file, set buff_index and dataset_baseindex
                                        buff_index = 0
                                        dataset_baseindex += buffer_size
                            
                            #still have data in buff, write them to hdf5 file
                            for columnindex,reportcolumns in allreportcolumns.items():
                                for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable in reportcolumns[1]:
                                    if  not column_filterable and not column_groupable and not column_statistical:
                                        continue
                
                                    if buff_index > 0:
                                        datasets[column_name].write_direct(databuffs[column_name],np.s_[0:buff_index],np.s_[dataset_baseindex:dataset_baseindex + buff_index])
                
                            if dataset_baseindex + buff_index + excluded_rows != dataset_size:
                                raise Exception("The file({0}) has {1} records, but only {2} are written to hdf5 file({3})".format(data_file,dataset_size,dataset_baseindex + buff_index,data_index_file))
                            else:
                                logger.info("The index file {1} was generated for file({0}) which contains {2} rows, {3} rows were processed, {4} rows were ignored ".format(data_file,data_index_file,dataset_size,dataset_baseindex + buff_index,excluded_rows))
                    finally:
                        if data_f:
                            try:
                                data_f.close()
                            except:
                                pass
        
                    #release the memory
                    datasets.clear()
                    datasets = None
                    databuffs.clear()
                    databuffs = None
                    
                    #rename the tmp data file to data file if required
                    if tmp_data_file:
                        os.rename(tmp_data_file,data_file)
                    #rename the tmp file to index file
                    if excluded_rows:
                        dataset_size = dataset_baseindex + buff_index
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

                        try:
                            os.remove(tmp_index_file)
                        except:
                            pass
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
        column_data = None
        cond_result = None
        int_buffer = [None,None]
        float_buffer = [None,None]
        string_buffer = [None,None]
        #the data buffer will put into the map data_buffers which is a map (key = id(condition or group by or reportset) value = buffer(int_buffer, float_buffer or string_buffer)
        data_buffers = {}

        #conditions are applied one by one, so it is better to share the buffers among conditions
        if report_conditions:
            #apply the conditions, try to share the np array among conditions to save memory
            for item in report_conditions:
                col = column_map[item[0]]
                col_type = col[2]
                if datatransformer.is_int_type(col_type):
                    data_buffers[id(item)] = int_buffer
                    if int_buffer[0]:
                        int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                    else:
                        int_buffer = col_type
                elif datatransformer.is_float_type(col_type):
                    data_buffers[id(item)] = float_buffer
                    if float_buffer[0]:
                        float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                    else:
                        float_buffer[0] = col_type
                elif datatransformer.is_string_type(col_type):
                    data_buffers[id(item)] = string_buffer
                    if string_buffer[0]:
                        string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                    else:
                        string_buffer[0] = col_type

        if report_group_by:
            #group_by feature is required
            #Use pandas to implement 'group by' feature, all dataset including columns in 'group by' and 'reportset' will be loaded into the memory.
            #use the existing data buff if it already exist for some column
            
            if data_buffers:
                closest_int_column = [None,None,None] #three members (the item with lower type, the item with exact type,the item with upper type), each memeber is None or list with 2 members:[ group_by or reportset item,col_type]
                closest_float_column = [None,None,None]
                closest_string_column = [None,None,None]
                for item in itertools.chain(report_group_by,reportset):
                    colname = item[0] if isinstance(item,list) else item
                    if colname == "*":
                        continue
                    col = column_map[colname]
                    col_type = col[2]
                    if datatransformer.is_int_type(col_type):
                        if int_buffer[0]:
                            if closest_int_column[1] and closest_int_column[1][1] == int_buffer[0]:
                                #already match exactly
                                continue
                            elif int_buffer[0] == col_type:
                                #match exactly
                                closest_int_column[1] = [item,col_type]
                            else:
                                t = datatransformer.ceiling_type(int_buffer[0],col_type)
                                if t == int_buffer[0]:
                                    #the int_buffer can hold the current report set column, reportset column's type is less then int_buffer's type
                                    #choose the column which is closest to int_buffer type
                                    if closest_int_column[0]:
                                        if closest_int_column[0][1] < col_type:
                                            closest_int_column[0][1] = [item,col_type]
                                    else:
                                        closest_int_column[0] = [item,col_type]
                                elif t == col_type:
                                    #the reportset column can hold the int_buffer data, reportset column's type is greater then int_buffer's type
                                    #choose the column which is closest to int_buffer type
                                    if closest_int_column[2]:
                                        if closest_int_column[2][1] > col_type:
                                            closest_int_column[2][1] = [item,col_type]
                                    else:
                                        closest_int_column[2] = [item,col_type]
                                else:
                                    #both the reportset column and in_buff can't hold each other,the result type is greater then int_buffer's type and reportset column type
                                    #choose the column which is closest to int_buffer type except the current chosed column's type can hold int_buffer data
                                    if closest_int_column[2]:
                                        if datatransformer.ceiling_type(int_buffer[0],closest_int_column[2][1]) == closest_int_column[2][1]:
                                            #the current chosed column's type can hold int_buffer data
                                            continue
                                        elif closest_int_column[2][1] > col_type:
                                            closest_int_column[2][1] = [item,col_type]
                                    else:
                                        closest_int_column[2] = [item,col_type]
                    elif datatransformer.is_float_type(col_type):
                        if float_buffer[0]:
                            if closest_float_column[1] and closest_float_column[1][1] == float_buffer[0]:
                                #already match exactly
                                continue
                            elif float_buffer[0] == col_type:
                                #match exactly
                                closest_float_column[1] = [item,col_type]
                            else:
                                t = datatransformer.ceiling_type(float_buffer[0],col_type)
                                if t == float_buffer[0]:
                                    #the float_buffer can hold the current report set column, reportset column's type is less then float_buffer's type
                                    #choose the column which is closest to float_buffer type
                                    if closest_float_column[0]:
                                        if closest_float_column[0][1] < col_type:
                                            closest_float_column[0][1] = [item,col_type]
                                    else:
                                        closest_float_column[0] = [item,col_type]
                                elif t == col_type:
                                    #the reportset column can hold the float_buffer data, reportset column's type is greater then float_buffer's type
                                    #choose the column which is closest to float_buffer type
                                    if closest_float_column[2]:
                                        if closest_float_column[2][1] > col_type:
                                            closest_float_column[2][1] = [item,col_type]
                                    else:
                                        closest_float_column[2] = [item,col_type]
                                else:
                                    #both the reportset column and in_buff can't hold each other,the result type is greater then float_buffer's type and reportset column type
                                    #choose the column which is closest to int_buffer type except the current chosed column's type can hold int_buffer data
                                    if closest_float_column[2]:
                                        if datatransformer.ceiling_type(float_buffer[0],closest_float_column[2][1]) == closest_float_column[2][1]:
                                            #the current chosed column's type can hold int_buffer data
                                            continue
                                        elif closest_float_column[2][1] > col_type:
                                            closest_float_column[2][1] = [item,col_type]
                                    else:
                                        closest_float_column[2] = [item,col_type]
                    elif datatransformer.is_string_type(col_type):
                        if string_buffer[0]:
                            if closest_string_column[1] and closest_string_column[1][1] == string_buffer[0]:
                                #already match exactly
                                continue
                            elif string_buffer[0] == col_type:
                                #match exactly
                                closest_string_column[1] = [item,col_type]
                            else:
                                t = datatransformer.ceiling_type(string_buffer[0],col_type)
                                if t == string_buffer[0]:
                                    #the string_buffer can hold the current report set column, reportset column's type is less then string_buffer's type
                                    #choose the column which is closest to string_buffer type
                                    if closest_string_column[0]:
                                        if closest_string_column[0][1] < col_type:
                                            closest_string_column[0][1] = [item,col_type]
                                    else:
                                        closest_string_column[0] = [item,col_type]
                                elif t == col_type:
                                    #the reportset column can hold the string_buffer data, reportset column's type is greater then string_buffer's type
                                    #choose the column which is closest to string_buffer type
                                    if closest_string_column[2]:
                                        if closest_string_column[2][1] > col_type:
                                            closest_string_column[2][1] = [item,col_type]
                                    else:
                                        closest_string_column[2] = [item,col_type]
                                else:
                                    #both the reportset column and in_buff can't hold each other,the result type is greater then string_buffer's type and reportset column type
                                    #choose the column which is closest to int_buffer type except the current chosed column's type can hold int_buffer data
                                    if closest_string_column[2]:
                                        if datatransformer.ceiling_type(string_buffer[0],closest_string_column[2][1]) == closest_string_column[2][1]:
                                            #the current chosed column's type can hold int_buffer data
                                            continue
                                        elif closest_string_column[2][1] > col_type:
                                            closest_string_column[2][1] = [item,col_type]
                                    else:
                                        closest_string_column[2] = [item,col_type]

                #choose the right column to share the data buffer chosed in report conditions
                if closest_int_column[1]:
                    #one column in group_by or reportset has the same type as int_buffer
                    data_buffer[id(closest_int_column[1][0]] = int_buffer
                elif closest_int_column[2] and datatransformer.ceiling_type(int_buffer[0],closest_int_column[2][1]) == closest_int_column[2][1]:
                    #one column in group_by or reportset has a data type which is bigger than int_buffer
                    int_buffer[0] = closest_int_column[2][1]
                    data_buffer[id(closest_int_column[2][0]] = int_buffer
                elif closest_int_column[0] and datatransformer.ceiling_type(int_buffer[0],closest_int_column[0][1]) == closest_int_column[0][1]:
                    #one column in group_by or reportset has a data type which is less than int_buffer
                    data_buffer[id(closest_int_column[0][0]] = int_buffer
                elif closest_int_column[0]:
                    #choose the column whose type is less than int_buffer but closest to int_buffer
                    data_buffer[id(closest_int_column[0][0]] = int_buffer
                elif closest_int_column[2]:
                    #choose the column whose type is greater than int_buffer but closest to int_buffer
                    int_buffer[0] = closest_int_column[2][1]
                    data_buffer[id(closest_int_column[2][0]] = int_buffer


                if closest_float_column[1]:
                    #one column in group_by or reportset has the same type as float_buffer
                    data_buffer[id(closest_float_column[1][0]] = float_buffer
                elif closest_float_column[2] and datatransformer.ceiling_type(float_buffer[0],closest_float_column[2][1]) == closest_float_column[2][1]:
                    #one column in group_by or reportset has a data type which is bigger than float_buffer
                    float_buffer[0] = closest_float_column[2][1]
                    data_buffer[id(closest_float_column[2][0]] = float_buffer
                elif closest_float_column[0] and datatransformer.ceiling_type(float_buffer[0],closest_float_column[0][1]) == closest_float_column[0][1]:
                    #one column in group_by or reportset has a data type which is less than float_buffer
                    data_buffer[id(closest_float_column[0][0]] = float_buffer
                elif closest_float_column[0]:
                    #choose the column whose type is less than float_buffer but closest to float_buffer
                    data_buffer[id(closest_float_column[0][0]] = float_buffer
                elif closest_float_column[2]:
                    #choose the column whose type is greater than float_buffer but closest to float_buffer
                    float_buffer[0] = closest_float_column[2][1]
                    data_buffer[id(closest_float_column[2][0]] = float_buffer

                if closest_string_column[1]:
                    #one column in group_by or reportset has the same type as string_buffer
                    data_buffer[id(closest_string_column[1][0]] = string_buffer
                elif closest_string_column[2] and datatransformer.ceiling_type(string_buffer[0],closest_string_column[2][1]) == closest_string_column[2][1]:
                    #one column in group_by or reportset has a data type which is bigger than string_buffer
                    string_buffer[0] = closest_string_column[2][1]
                    data_buffer[id(closest_string_column[2][0]] = string_buffer
                elif closest_string_column[0] and datatransformer.ceiling_type(string_buffer[0],closest_string_column[0][1]) == closest_string_column[0][1]:
                    #one column in group_by or reportset has a data type which is less than string_buffer
                    data_buffer[id(closest_string_column[0][0]] = string_buffer
                elif closest_string_column[0]:
                    #choose the column whose type is less than string_buffer but closest to string_buffer
                    data_buffer[id(closest_string_column[0][0]] = string_buffer
                elif closest_string_column[2]:
                    #choose the column whose type is greater than string_buffer but closest to string_buffer
                    string_buffer[0] = closest_string_column[2][1]
                    data_buffer[id(closest_string_column[2][0]] = string_buffer

        else:
            #group_by feature is not required.
            #perform the statistics one by one, try best to share the data buffer
            for item in reportset:
                col = column_map[item[0]]
                col_type = col[2]
                if datatransformer.is_int_type(col_type):
                    data_buffers[id(item)] = int_buffer
                    if int_buffer[0]:
                        int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                    else:
                        int_buffer = col_type
                elif datatransformer.is_float_type(col_type):
                    data_buffers[id(item)] = float_buffer
                    if float_buffer[0]:
                        float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                    else:
                        float_buffer[0] = col_type
                elif datatransformer.is_string_type(col_type):
                    data_buffers[id(item)] = string_buffer
                    if string_buffer[0]:
                        string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                    else:
                        string_buffer[0] = col_type

        with h5py.File(data_index_file,'r') as index_h5:
            #filter the dataset
            if report_conditions:
                #apply the conditions, try to share the np array among conditions to save memory

                previous_item = None
                for cond in report_conditions:
                    #each condition is a tuple(column, operator, value), value is dependent on operator and column type
                    col = column_map[cond[0]]
                    col_type = col[2]

                    #map the value to internal value used by dataset
                    cond[2] = cond[2].strip() if cond[2] else cond[2]
                    if cond[2].startswith('[') and cond[2].endswith(']'):
                        cond[2] = json.loads(cond[2])
                    if isinstance(cond[2],list):
                        for i in range(len(cond[2])):
                            if col[3]:
                                #need transformation
                                if col[5]:
                                    #is enum type
                                    cond[2][i] = datatransformer.get_enum(cond[2][i],databaseurl=databaseurl,columnid=col[0])
                                    if not cond[2]:
                                        #searching value doesn't exist
                                        cond[2][i] = None
                                        break
                                else:
                                    if col[4] and col[4].get("parameters"):
                                        cond[2][i] = datatransformer.transform(col[3],cond[2][i],databaseurl=databaseurl,columnid=col[0],**col[4]["parameters"])
                                    else:
                                        cond[2][i] = datatransformer.transform(col[3],cond[2][i],databaseurl=databaseurl,columnid=col[0])
                        if any(v is None for v in cond[2]):
                            #remove the None value from value list
                            cond[2] = [v for v in cond[2] if v is not None]
                    else:
                        if col[3]:
                            #need transformation
                            if col[5]:
                                #is enum type
                                cond[2] = datatransformer.get_enum(cond[2],databaseurl=databaseurl,columnid=col[0])
                                if not cond[2]:
                                    #searching value doesn't exist
                                    cond_result = None
                                    break
                            else:
                                if col[4] and col[4].get("parameters"):
                                    cond[2] = datatransformer.transform(col[3],cond[2],databaseurl=databaseurl,columnid=col[0],**col[4]["parameters"])
                                else:
                                    cond[2] = datatransformer.transform(col[3],cond[2],databaseurl=databaseurl,columnid=col[0])

                    if not previous_item or previous_item[0] != cond[0]:
                        #condition is applied on different column
                        try:
                            buff = data_buffers.pop(id(cond))
                            buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                            column_data = buff[1]
                        except KeyError as ex:
                            column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col[2]))
                        
                        index_h5[col[1]].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
                    if cond_result is None:
                        cond_result = operation.get_func(col[2],cond[1])(column_data,cond[2])
                    else:
                        cond_result &= operation.get_func(col[2],cond[1])(column_data,cond[2])

                    previous_item = cond

                column_data = None

                if cond_result is None:
                    filtered_rows = 0
                else:
                    filtered_rows = np.count_nonzero(cond_result)
            else:
                filtered_rows = dataset_size


            if reportset == "__details__":
                #return the detail logs
                if filtered_rows == 0:
                    return [(data[0],data[1],0,None)]
                elif filtered_rows == datast_size:
                    #all logs are returned
                    #unlikely to happen.
                    report_file = os.path.join(cache_dir,"reports","{0}-{2}-{3}{1}".format(*os.path.splitext(data[1]),reportid,data[0]))
                    shutil.copyfile(data_file,report_file)
                    return [(data[0],data[1],dataset_size,report_file)]
                else:
                    report_size = np.count_nonzero(cond_result)
                    indexes = np.flatnonzero(cond_result)
                    #line number is based on 1 instead of 0
                    indexes += 1
                    try:
                        with tempfile.NamedTemporaryFile(prefix="datascience_",delete=False) as f:
                            linenumber_file = f.name
                        np.savetxt(linenumber_file,indexes,fmt='%u',delimiter=",",newline=os.linesep)
                        report_file_folder = os.path.join(cache_dir,"reports","tmp")
                        utils.mkdir(report_file_folder)
                        report_file = os.path.join(report_file_folder,"{0}-{2}-{3}{1}".format(*os.path.splitext(data[1]),reportid,data[0]))
                        utils.filter_file_with_linenumbers(data_file,linenumber_file,report_file)
                        return [(data[0],data[1],report_size,report_file)]
                    finally:
                        utils.remove_file(linenumber_file)

            if report_group_by :
                #'group by' enabled
                #create pandas dataframe
                df_datas = collections.OrderedDict()
                if report_type == HOURLY_REPORT:
                    df_datas["request_time"] = dataset_time.strftime("%Y-%m-%d %H:00:00")
                elif report_type == DAILY_REPORT:
                    df_datas["request_time"] = dataset_time.strftime("%Y-%m-%d 00:00:00")

                for item in itertools.chain(report_group_by,reportset):
                    colname = item[0] if isinstance(item,list) else item
                    if colname == "*":
                        continue
                    col = column_map[colname]
                    col_type = col[2]
                    try:
                        buff = data_buffers.pop(id(item))
                        buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                        column_data = buff[1]
                    except KeyError as ex:
                        column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col[2]))
                        
                    index_h5[item[0]].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
                    df_datas[colname] = column_data[cond_result])

                #create pandas dataframe
                df = pd.DataFrame(df_data)
                #get the group object
                if report_type:
                    df_group = df.groupby(["request_time",*report_group_by],group_keys=True)
                else:
                    df_group = df.groupby(report_group_by,group_keys=True)
                #perfrom the statistics on group
                #populate the statistics map
                statics_map = collections.OrderedDict()
                previous_item = None
                for item in reportset:
                    col = column_map[item[0]]
                    col_type = col[2]
                    if not previous_item or previous_item[0] != item[0]:
                        statics_map[item[0]] = [operation.get_agg_func(item[1])]
                        previous_item = item
                    else:
                        statics_map[item[0]].append(operation.get_agg_func(item[1]))
                df_result = df_group.agg(statics_map)
                return dict(zip(df_result.index,df_result.values))
            else:
               #no 'group by', return the statistics data.
                if filtered_rows == 0:
                    #no cond_result 
                    return [[None for item in reportset]]

                previous_item = None
                if report_type == HOURLY_REPORT:
                    report_data = [dataset_time.strftime("%Y-%m-%d %H:00:00")]
                elif report_type == DAILY_REPORT:
                    report_data = [dataset_time.strftime("%Y-%m-%d 00:00:00")]
                else:
                    report_data = []
                for item in reportset:
                    if not previous_item or previous_item[0] != item[0]:
                        #new column should be loaded
                        previous_item = item
                        if item[0] != "*":
                            col = column_map[item[0]]
                            col_type = col[2]
                            try:
                                buff = data_buffers.pop(id(item))
                                buff[1] = np.empty((dataset_size,),dtype=datatransformer.get_np_type(buff[0]))
                                column_data = buff[1]
                            except KeyError as ex:
                                column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col[2]))
                        
                            index_h5[item[0]].read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])

                    if item[0] == "*":
                        #this operator applied on any colunn, only support count
                        if item[1] != 'count':
                            raise Exception("Only operator 'count' can operate on any column")
                        report_data.append(np.count_nonzero(filtered_rows))
                    elif filtered_rows == dataset_size:
                        report_data.append(operation.get_func(col[2],item[1])(column_data))
                    else:
                        report_data.append(operation.get_func(col[2],item[1])(column_data[cond_result]))
                logger.debug("dataset={},reportset={},report_data={}".format(data[1],reportset,report_data))

                return [report_data]
    return analysis

def run():
    """
    The entry point of pyspark application
    """
    try:
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
                cursor.execute("select name,dataset_id,\"start\",\"end\",rtype,conditions,\"group_by\",\"sort_by\",reportset from datascience_report where id = {}".format(reportid))
                report = cursor.fetchone()
                if report is None:
                    raise Exception("Report({}) doesn't exist.".format(reportid))
                report_name,datasetid,report_start,report_end,report_type,report_conditions,report_group_by,report_sort_by,reportset = report
    
                cursor.execute("select name,datasetinfo from datascience_dataset where id = {}".format(datasetid))
                dataset = cursor.fetchone()
                if dataset is None:
                    raise Exception("Dataset({}) doesn't exist.".format(datasetid))
                dataset_name,dataset_info = dataset

                cursor.execute("select name,columnid,dtype,transformer,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} ".format(datasetid))
                for row in cursor.fetchall():
                    column_map[row[0]] = [row[1],row[2],row[3],row[4],row[5],row[6]]

        if report_type == DAILY_REPORT:
            #for daily report, the minimum time unit of report_start and report_end is day; if it is not, set hour,minute,second and microsecond to 0
            report_start = timezone.localtime(report_start)
            if report_start.hour or report_start.minute or report_start.second or report_start.microsecond:
                report_start = report_start.replace(hour=0,minute=0,second=0,microsecond=0)
    
            report_end = timezone.localtime(report_end)
            if report_end.hour or report_end.minute or report_end.second or report_end.microsecond:
                report_end = report_end.replace(hour=0,minute=0,second=0,microsecond=0) + timedelta(hours=1)
        else:
            #the minimum time unit of report_start and report_end is hour; if it is not, set minute,second and microsecond to 0
            report_start = timezone.localtime(report_start)
            if report_start.minute or report_start.second or report_start.microsecond:
                report_start = report_start.replace(minute=0,second=0,microsecond=0)
    
            report_end = timezone.localtime(report_end)
            if report_end.minute or report_end.second or report_end.microsecond:
                report_end = report_end.replace(minute=0,second=0,microsecond=0) + timedelta(hours=1)
    
        #populate the list of nginx access log file
        datasets = []
        dataset_time = report_start
        while dataset_time < report_end:
            if not dataset_info.get("filepattern"):
                raise Exception("Missing the config item 'filepattern' in datasetinfo, which is used to construct the nginx access log file based on datetime")
            datasets.append((dataset_time.strftime("%Y%m%d%H"),dataset_time.strftime(dataset_info.get("filepattern"))))
            dataset_time += timedelta(hours=1)

        #the following line resets the dataset the test dataset for testing
        datasets = [("2022020811","2022020811.nginx.access.csv"),("2023010114","2023010114.nginx.access.csv")]
        
        #sort the report_conditions
        if report_conditions:
            report_conditions.sort()

        #if reportset contains a column '__all__', means this report will return acess log details, ignore other reportset columns
        #if 'count' is in reportset, change the column to * and also check whether reportset has multiple count column
        if not reportset:
            reportset = "__details__"
        else:
            count_column_index = False
            count_added = False
            found_avg = False
            for item in reportset:
                if item[1] == "count":
                    if count_column_index:
                        raise Exception("Have multiple column 'count' in report set for report({})".format(reportid))
                    else:
                        item[0] = "*"
                        count_column_index = True
                    continue
                elif item[1] == "avg" :
                    #perform a avg on a list of avg data is incorrect, because each access log file has different records.
                    #so change the operator 'avg' to 'avg_sum' to perform an operator 'sum' instead; and also add a 'count' column if doesn't exist.
                    found_avg = True
                    item[1] == "avg_sum"
                elif item[0] == "__all__" :
                    #a  detail log report can't contain any statistics data.
                    reportset = "__details__"
                    break

        if reportset == "__details__":
            #this report will return all log details, report_group_by is meanless
            report_group_by = None
            report_sort_by = None
            report_type = None
        else:
            if found_avg:
                #group_by is enabled, add a count if it doesn't exist
                if not count_column_index:
                    reportset.insert(0,["*","count","__count__"])
                    count_added = True

            reportset.sort()
            for i in range(len(reportset)):
                item = reportset[i]
                if item[1] == "count":
                    count_column_index = i
                elif not item[1]:
                    raise Exception("Missing aggregation method on column({1}) for report({0})".format(reportid,item[0]))
                else:
                    col = column_map[item[0]]
                    if not col[3]:
                        raise Exception("Can't apply aggregation method on non-statistical column({1}) for report({0})".format(reportid,item[0]))
                    if report_group_by and item[0] in report_group_by:
                        raise Exception("Can't apply aggregation method on group-by column({1}) for report({0})".format(reportid,item[0]))
            if not report_group_by:
                #group_by is not enabled, all report data are statistical data,and only contains one row,
                #report sort by is useless
                report_sort_by = None

        spark = get_spark_session()
        rdd = spark.sparkContext.parallelize(datasets, len(datasets))
        #perform the analysis per nginx access log file
        rdd = rdd.flatMap(analysis_factory(reportid,databaseurl,datasetid,dataset_info,report_start,report_end,report_conditions,report_group_by,reportset,report_type))

        #init the folder to place the report file
        cache_dir = dataset_info.get("cache")
        report_file_folder = os.path.join(cache_dir,"reports",report_start.strftime("%Y-%m-%d"))
        utils.mkdir(report_file_folder)
        if reportset == "__details__":
            result = rdd.collect()
            logger.debug("len = {}".format(len(result)))
            logger.debug("result = " + str(result))
            result.sort()
            result = [r for r in result if r[3]]
            if len(result) == 0:
                logger.debug("No data found")
                return None

            report_file = os.path.join(report_file_folder,"nginxaccesslog-report-{}{}".format(reportid,os.path.splitext(result[0][3])[1]))
            if len(result) == -1:
                os.rename(result[0][3],report_file)
            else:
                utils.concat_files([r[3] for r in result],report_file)
                for r in result:
                    utils.remove_file(r[3])
            logger.debug("report file = {}".format(report_file))
            return report_file
        else:
            report_file = os.path.join(report_file_folder,"nginxaccesslog-report-{}.csv".format(reportid))
            if report_group_by:
                if report_type == HOURLY:
                    #hourly report, each access log is one hour data, no need to reduce
                    #a new column is added to the group_by
                    report_result = rdd.collect()
                    report_group_by.insert(0,"request_time")
                elif report_type == DAILY:
                    #daily report, need to reduce the result
                    #a new column is added to the group_by
                    report_group_by.insert(0,"request_time")
                    report_result = rdd.reduceByKey(merge_reportresult_factory(reportset)).collect()
                else:
                    report_result = rdd.reduceByKey(merge_reportresult_factory(reportset)).collect()
            else:
                if report_type == HOURLY:
                    #hourly report, each access log is one hour data, no need to reduce
                    #a new column is added to reportset
                    reportset.insert(0,("request_time",None,"request_time"))
                    report_result = rdd.collect()
                elif report_type == DAILY:
                    #daily report, need to reduce the result
                    #the result is a map between day and value
                    #a new column is added to the group_by
                    reprt_group_by = ["request_time"]
                    report_result = rdd.reduceByKey(merge_reportresult_factory(reportset)).collect()
                else:
                    report_result = rdd.reduceByKey(merge_reportresult_factory(reportset)).collect()

            #calcuate the avg and remove count column if it is added for avg
            if found_avg:
                for row in report_result.values() if report_group_by else report_result:
                    i = 0
                    while i < len(reportset):
                        try:
                            if reportset[i][1] == "avg_sum":
                                row[i] = row[i] / row[count_column_index]
                        finally:
                            i += 1
                    if count_added:
                        #added for avg
                        del row[count_column_index]

                if count_added:
                    del reportset[count_added]

            #save the data to file and also convert the enumeration data back to string
            with open(report_file, 'w', newline='') as f:
                writer = csv.writer(f)
                #writer header
                if report_group_by:
                    writer.writerow([ itertoos.chain(report_group_by,c[2] or c[0] for c in reportset]))
                else:
                    writer.writerow([ c[2] or c[0] for c in reportset])
                #write rows
                if report_group_by:
                    #sort the data
                    if report_sort_by:
                        report_result = sorted(report_result.items(),key=sort_group_by_result_factory(databaseurl,column_map,report_group_by,reportset,report_sort_by))
                    else:
                        report_result = report_result.items()
                    #find the column ids for columns whose will be converted from int value to enum key
                    enum_colids = None
                    for i in range(len(report_group_by)):
                        item = report_group_by[i]
                        if item == "request_time":
                            continue
                        colid = column_map[s][0]  if column_map[item][2] and datatransformer.is_enum_func(column_map[item][2]) else None
                        if colid is not None:
                            if not enum_colids:
                                enum_colids = [None] * len(report_group_by)
                            enum_colids[i] = colid

                    #save the data
                    writer.writerows(group_by_report_iterator(report_result,databaseurl,enum_colids))
                else:
                    #group_by is not enabled, all report data are statistical data
                    writer.writerows(report_result)

        """
        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("update datascience_report set status='{{\"status\":\"Succeed\",\"report\":\"{1}\"}}',exec_end='{2}' where id = {0}".format(reportid,report_file,timezone.dbtime()))
                conn.commit()
        """
    except Exception as ex:
        logger.error("Failed to generate the report.report={}.{}".format(reportid,traceback.format_exc()))
        msg = str(ex).replace("\n","\\n").replace("\r","\\r").replace('"','\\"').replace("'","\\'").replace("\t","\\t")
        """
        with database.Database(databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("update datascience_report set status='{{\"status\":\"Failed\",\"message\":\"{1}\"}}',exec_end='{2}' where id = {0}".format(reportid,msg,timezone.dbtime()))
                except Exception as ex1:
                    logger.error("can't save the exception message({}).{}".format(str(ex),str(ex1)))
                    cursor.execute("update datascience_report set status='{{\"status\":\"Failed\",\"message\":\"{1}\"}}',exec_end='{2}' where id = {0}".format(reportid,"Failed to save the exception message.",timezone.dbtime()))
                conn.commit()
        """
        raise 


if __name__ == "__main__":
    run()
        
