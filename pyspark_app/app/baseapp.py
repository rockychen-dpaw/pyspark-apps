import logging
import traceback
import os
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
from pyspark_app.utils.filelock import FileLock,AlreadyLocked

from pyspark_app import utils
from pyspark_app import datatransformer
from pyspark_app import operation
from pyspark_app import intervals
from pyspark_app import datafile
from pyspark_app.app.base import get_spark_session

logger = logging.getLogger("pyspark_app.app.baseapp")

EXECUTOR_COLUMNID=0
EXECUTOR_COLUMNNAME=1
EXECUTOR_DTYPE=2
EXECUTOR_TRANSFORMER=3
EXECUTOR_COLUMNINFO=4
EXECUTOR_STATISTICAL=5
EXECUTOR_FILTERABLE=6
EXECUTOR_GROUPABLE=7
EXECUTOR_REFRESH_REQUESTED=8

class NoneReportType(object):
    ID = None
    NAME = ""
    PATTERN4FILENAME = "%Y%m%d%H%M%S"
    PATTERN = "%Y-%m-%d %H:%M:%S"

    @classmethod
    def format4filename(cls,t):
        return timezone.format(t,cls.PATTERN4FILENAME)

    @classmethod
    def format(cls,t):
        return timezone.format(t,cls.PATTERN)


class ExecutorContext(object):
    DOWNLOAD = 1
    ANALYSIS = 2

    DOWNLOADED = 1
    ALREADY_DOWNLOADED = 2
    RESOURCE_NOT_FOUND = -1
    DOWNLOADING_BY_OTHERS = -2

    reportid = None
    datasetid = None
    task_timestamp = None
    executor_type = None

    data_cache_dir = None
    report_cache_dir = None

    resource_harvester = None
    allreportcolumns = None
    indexbuffs = None
    databuff = None
    buffer_size = None
    databuffer_size = None

    column_map = None
    report_data_buffers = None
    cond_result = None

    @classmethod
    def can_share_context(cls,task_timestamp,reportid,executor_type,datasetid):
        if cls.reportid == reportid and cls.executor_type == executor_type and cls.task_timestamp == task_timestamp:
            logger.debug("ExecutorContext are shared. reportid={} , executor_type={}, task_timestamp={}".format(reportid,executor_type,task_timestamp))
            return True
        else:
            logger.debug("ExecutorContext are not shared. reportid={} , executor_type={}, task_timestamp={}".format(reportid,executor_type,task_timestamp))
            cls.reportid = reportid
            cls.datasetid = datasetid
            cls.executor_type = executor_type
            cls.task_timestamp = task_timestamp

            cls.data_cache_dir = None
            cls.report_cache_dir = None

            cls.resource_harvester = None
            cls.allreportcolumns = None
            cls.indexbuffs = None
            cls.databuff = None
            cls.buffer_size = None
            cls.databuffer_size = None

            cls.column_map = None
            cls.report_data_buffers = None
            cls.cond_result = None

            return False

class DatasetConfig(object):
    _harvester = None


    def datasetconfig_validate(self):
        if not self.datasetinfo :
            raise Exception("Dateset config is empty")

        def _validate(config,keys,parentkey=None):
            if isinstance(keys,str):
                if keys not in config:
                    raise Exception("Missing config item '{}'".format("{}.{}".format(parentkey,keys) if parentkey else keys))
            elif isinstance(keys,list):
                for key in keys:
                    if isinstance(key,str):
                        if key not in config:
                            raise Exception("Missing config item '{}'".format("{}.{}".format(parentkey,key) if parentkey else key))
                    elif isinstance(key,tuple) and len(key) == 2:
                        if key[0] not in config:
                            raise Exception("Missing config item '{}'".format("{}.{}".format(parentkey,key[0]) if parentkey else key[0]))
                        _validate(config[key[0]],key[1],parentkey="{}.{}".format(parentkey,key[0]) if parentkey else key[0])

                    else:
                        raise Exception("Invalid checking keys({}({}))".format(key.__class__.__name__,key))
            elif isinstance(keys,tuple) and len(keys) == 2:
                if keys[0] not in config:
                    raise Exception("Missing config item '{}'".format("{}.{}".format(parentkey,keys[0]) if parentkey else keys[0]))
                _validate(config[keys[0]],keys[1],parentkey="{}.{}".format(parentkey,keys[0]) if parentkey else keys[0])
            else:
                raise Exception("Invalid checking keys({}({}))".format(keys.__class__.__name__,keys))

        self.datasetinfo["generate_report"] = self.datasetinfo.get("generate_report",{})

        _validate(self.datasetinfo,[
            ("datafile",["filetype","filename","data_interval"]),
            "cache",
            ("download",[("harvester",["name","parameters"])])
        ])

    _download_concurrency = None
    @property
    def download_concurrency(self):
        if self._download_concurrency is None:
            download_concurrency = self.datasetinfo["download"].get("concurrency")
            if download_concurrency and isinstance(download_concurrency,str) and download_concurrency.startswith("lambda"):
                download_concurrency = eval(download_concurrency)
            elif download_concurrency:
                download_concurrency = eval("lambda files:{}".format(download_concurrency))
            else:
                download_concurrency = lambda files:None

            self._download_concurrency = download_concurrency

        return self._download_concurrency

    _report_concurrency = None
    @property
    def report_concurrency(self):
        if self._report_concurrency is None:
            report_concurrency = self.datasetinfo["generate_report"].get("concurrency")
            if report_concurrency and isinstance(report_concurrency,str) and report_concurrency.startswith("lambda"):
                report_concurrency = eval(report_concurrency)
            elif report_concurrency:
                report_concurrency = eval("lambda files:{}".format(report_concurrency))
            else:
                report_concurrency = lambda files:None
            self._report_concurrency = report_concurrency

        return self._report_concurrency

    _data_interval = None
    @property
    def data_interval(self):
        if not self._data_interval:
            self._data_interval = intervals.get_interval(self.datasetinfo["datafile"]["data_interval"])

        return self._data_interval
     

    _f_datafile = None
    def get_datafilename(self,starttime,endtime):
        if self._f_datafile is None:
            if self.datasetinfo["datafile"]["filename"].startswith("lambda"):
                self._f_datafile = eval(self.datasetinfo["datafile"]["filename"])
            else:
                self._f_datafile = lambda starttime,endtime:starttime.strftime(self.datasetinfo["datafile"]["filename"])

        return self._f_datafile(starttime,endtime)



    _cache_timeout = None
    @property
    def cache_timeout(self):
        if self._cache_timeout is None:
            try:
                self._cache_timeout = self.datasetinfo["download"].get("cache_timeout",28) #in days
            except:
                self._cache_timeout = 28

        return self._cache_timeout

    @property
    def harvester(self):
        """
        Return a harvester which harvest the nginx access log from source repository.
        """
        if not self._harvester:
            harvester_config = self.datasetinfo["download"]["harvester"]
            self._harvester = harvester.get_harvester(harvester_config["name"],**harvester_config["parameters"])

        return self._harvester

    def get_datafilewriter(self,**kwargs):
        try:
            return datafile.writer(self.datasetinfo["datafile"]["filetype"],**kwargs)
        except KeyError as ex:
            raise Exception("Incomplete configuration 'datafile'.{}".format(str(ex)))

    def get_srcdatafilereader(self,file):
        try:
            if "has_header" in self.datasetinfo["download"]:
                return datafile.reader(self.datasetinfo["datafile"]["filetype"],file,headers=self.datasetinfo["datafile"].get("headers"),has_header=self.datasetinfo["download"]["has_header"])
            else:
                return datafile.reader(self.datasetinfo["datafile"]["filetype"],file,headers=self.datasetinfo["datafile"].get("headers"))
        except KeyError as ex:
            raise Exception("Incomplete configuration 'datafile'.{}".format(str(ex)))

    def get_datafilereader(self,file):
        try:
            return datafile.reader(self.datasetinfo["datafile"]["filetype"],file,headers=self.datasetinfo["datafile"].get("headers"),has_header=False)
        except KeyError as ex:
            raise Exception("Incomplete configuration 'datafile'.{}".format(str(ex)))

    @property
    def data_headers(self):
        try:
            return self.datasetinfo["datafile"].get("headers")
        except:
            return None
        

    @property
    def databuffer_size(self):
        try:
            return self.datasetinfo["download"].get("download_buffer",10000)
        except :
            return 10000


    @property
    def indexbuffer_size(self):
        try:
            return self.datasetinfo["download"].get("index_buffer",10000)
        except:
            return 10000

    @property
    def cachefolder(self):
        return self.datasetinfo["cache"]

    @property
    def filelock_timeout(self):
        try:
            return self.datasetinfo["download"].get("lock_timeout",600)
        except :
            return 600

    @property
    def ignore_missing_datafile(self):
        try:
            return self.datasetinfo["download"].get("ignore_missing_datafile",False)
        except:
            return False

    @property
    def reportbuffer_size(self):
        try:
            return self.datasetinfo["generate_report"].get("buffer_size")
        except:
            return None

    @property
    def dataset_read_direct(self):
        try:
            self.datasetinfo["generate_report"].get("read_direct")
        except:
            return None

class DatasetColumnConfig(DatasetConfig):

    def column_read_direct(self,col):
        try:
            result = col[EXECUTOR_COLUMNINFO]["read_direct"]
            if result is None:
                result = not datatransformer.is_string_type(col[EXECUTOR_DTYPE]) 
                col[EXECUTOR_COLUMNINFO]["read_direct"] = result
        except:
            result = self.dataset_read_direct
            if result is None:
                result = not datatransformer.is_string_type(col[EXECUTOR_DTYPE]) 
            col[EXECUTOR_COLUMNINFO]["read_direct"] = result

        return result

    def columnbuffer_size(self,col):
        try:
            return col[EXECUTOR_COLUMNINFO]["buffer_size"]
        except:
            return self.reportbuffer_size


class DatasetAppDownloadExecutor(DatasetColumnConfig):
    def __init__(self,task_timestamp,databaseurl,datasetid,datasetinfo,dataset_refresh_requested,lock_timeout=None):
        self.task_timestamp = task_timestamp
        self.databaseurl = databaseurl
        self.datasetid = datasetid
        self.datasetinfo = datasetinfo
        self.dataset_refresh_requested = dataset_refresh_requested
        self.lock_timeout = lock_timeout
         
    @staticmethod
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

    def run(self,data):
        """
        download a single nginx access log file
        params:
            data: a tupe (datafile start time, datafile end time, datafile  name)
        Return a tuple(datetime of the nginx access log with format "%Y%m%d%H", file name of nginx access log, -1 resource not found; 0, already downlaoded before, 1 download successfully)
        """
        import h5py
        import numpy as np
        try:
            dataset_time = timezone.parse(data[0])

            if not ExecutorContext.can_share_context(self.task_timestamp,None,ExecutorContext.DOWNLOAD,self.datasetid):
                ExecutorContext.buffer_size = self.indexbuffer_size

                cache_dir = self.cachefolder
    
                ExecutorContext.data_cache_dir = os.path.join(cache_dir,"data")

                #load the dataset column settings, a map between columnindex and a tuple(includes and excludes,(id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn))
                ExecutorContext.allreportcolumns = {}
                with database.Database(self.databaseurl).get_conn(True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable,refresh_requested from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(self.datasetid))
                        previous_columnindex = None
                        columns = None
                        for d in itertools.chain(cursor.fetchall(),[[-1]]):
                            if previous_columnindex is None or previous_columnindex != d[0]:
                                #new column index
                                if columns:
                                    #initialize the column's includes
                                    columns[0] = self.filter_factory(columns[0][0],columns[0][1])
                                if d[0] == -1:
                                    #the end flag
                                    break
        
                                previous_columnindex = d[0]
                                columns = [[d[5].get("include") if d[5] else None,d[5].get("exclude") if d[5] else None],[(d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9])]]
                                ExecutorContext.allreportcolumns[d[0]] = columns
                            else:
                                columns[1].append((d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9]))
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
            

            cache_folder = os.path.join(ExecutorContext.data_cache_dir,dataset_time.strftime("%Y-%m-%d"))
            utils.mkdir(cache_folder)
    
            #get the cached local data file and data index file
            data_file = os.path.join(cache_folder,data[2])
            data_index_file = os.path.join(cache_folder,"{}.hdf5".format(data[2]))
    
            if os.path.exists(data_file) and self.dataset_refresh_requested and utils.file_mtime(data_file) < self.dataset_refresh_requested:
                #data_file is cached before refersh requested, need to refresh again.
                logger.debug("The cached data file({}) was cached at {}, but refresh was requesed at {}, refresh the cached data file".format(data_file,timezone.format(utils.file_mtime(data_file)),timezone.format(self.dataset_refresh_requested)))
                utils.remove_file(data_file)
    
    
            if not os.path.exists(data_file) and os.path.exists(data_index_file):
                #if data_file doesn't exist, data_indes_file should not exist too.
                utils.remove_file(data_index_file)
    
            #check data index file
            process_required_columns = set()
            dataset_size = 0
            if os.path.exists(data_index_file):
                #the data index file exist, check whether the indexes are created for all columns.if not regenerate it
                try:
                    with h5py.File(data_index_file,'r') as index_file:
                        for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
                            for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable,column_refresh_requested in reportcolumns[1]:
                                if  not column_filterable and not column_groupable and not column_statistical:
                                    continue
                                try:
                                    #check whether the dataset is accessable by getting the size
                                    dataset_size = index_file[column_name].shape[0]
                                    if column_refresh_requested and (not index_file[column_name].attrs['created'] or timezone.timestamp(column_refresh_requested) > index_file[column_name].attrs['created']):
                                        #The column's index was created before the refresh required by the user
                                        process_required_columns.add(column_name)
                                except KeyError as ex:
                                    #this dataset does not exist , regenerate it
                                    process_required_columns.add(column_name)
                except:
                    #other unexpected exception occur, the index file is corrupted. regenerate the whole index file
                    logger.error(traceback.format_exc())
                    utils.remove_file(data_index_file)
                    process_required_columns.clear()
    
            if os.path.exists(data_index_file) and not process_required_columns:
                #data index file is already downloaded
                logger.debug("The index file({1}) is already generated and up-to-date for data file({0})".format(data_file,data_index_file))
                return [[*data,ExecutorContext.ALREADY_DOWNLOADED]]
            else:
                #data index file does not exist, generate it.
                #Obtain the file lock before generating the index file to prevend mulitiple process from generating the index file for the same access log file
                before_get_lock = timezone.localtime()
                try:
                    with FileLock(os.path.join(cache_folder,"{}.lock".format(data[2])),self.filelock_timeout,timeout = self.lock_timeout) as lock:
                        if (timezone.localtime() - before_get_lock).total_seconds() >= 0.5:
                            #spend at least 1 second to get the lock, some other process worked on the same file too.
                            #regenerate the process_required_columns
                            process_required_columns.clear()
                            if os.path.exists(data_index_file):
                                #the data index file exist, check whether the indexes are created for all columns.if not regenerate it
                                try:
                                    with h5py.File(data_index_file,'r') as index_file:
                                        for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
                                            for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable,column_refresh_requested in reportcolumns[1]:
                                                if  not column_filterable and not column_groupable and not column_statistical:
                                                    continue
                                                try:
                                                    #check whether the dataset is accessable by getting the size
                                                    dataset_size = index_file[column_name].shape[0]
                                                    if column_refresh_requested and (not index_file[column_name].attrs['created'] or timezone.timestamp(column_refresh_requested) > index_file[column_name].attrs['created']):
                                                        #The column's index was created before the refresh required by the user
                                                        process_required_columns.add(column_name)
                                                except KeyError as ex:
                                                    #this dataset does not exist , regenerate it
                                                    process_required_columns.add(column_name)
                                except:
                                    #other unexpected exception occur, the index file is corrupted. regenerate the whole index file
                                    utils.remove_file(data_index_file)
                                    process_required_columns.clear()
        
                        if os.path.exists(data_index_file) and not process_required_columns:
                            #data index file is already downloaded
                            logger.debug("The index file({1}) is already generated and up-to-date for data file({0})".format(data_file,data_index_file))
                            return [[*data,ExecutorContext.ALREADY_DOWNLOADED]]
        
                            #generate the index file
                        #get the line counter of the file
                        try:
                            indexbuff_baseindex = 0
                            indexbuff_index = 0
    
                            databuff_index = 0
                            #prepare the source data file if required.
                            src_data_file = None
                            if not os.path.exists(data_file):
                                #local data file doesn't exist, download the source file if required as src_data_file
                                if not ExecutorContext.resource_harvester:
                                    ExecutorContext.resource_harvester = self.harvester

                                if ExecutorContext.resource_harvester.is_local():
                                    src_data_file = ExecutorContext.resource_harvester.get_abs_path(data[2])
                                else:
                                    with tempfile.NamedTemporaryFile(prefix="dataset_{}".format(self.datasetid),delete=False) as f:
                                        src_data_file = f.name
                                    ExecutorContext.resource_harvester.saveas(data[2],src_data_file)
    
                            #generate index file
                            tmp_index_file = "{}.tmp".format(data_index_file)
                            if os.path.exists(data_index_file):
                                #the index file already exist, only part of the columns need to be refreshed.
                                #rename the index file to tmp_index_file for processing
                                shutil.copy(data_index_file,tmp_index_file)
                                logger.debug("The columns({1}) need to be refreshed in index file({0})".format(data_index_file,process_required_columns))
                            else:
                                if os.path.exists(tmp_index_file):
                                    #tmp index file exists, delete it
                                    utils.remove_file(tmp_index_file)
                                if src_data_file:
                                    dataset_size = self.get_srcdatafilereader(src_data_file).records
                                else:
                                    dataset_size = self.get_datafilereader(data_file).records

                                logger.debug("Create the the index file({0})".format(data_index_file))
    
                            logger.debug("The file({}) has {} records".format((src_data_file or data_file),dataset_size))
                                
                            excluded_rows = 0
                            context={
                                "dataset_time":dataset_time
                            }

                            if src_data_file:
                                #the local cached data file doesnot exist, 
                                #should generate the local cached data file by excluding the noisy data from original dataset.
                                tmp_data_file = "{}.tmp".format(data_file)
                                datafilewriter = self.get_datafilewriter(file=tmp_data_file)
                                if not ExecutorContext.databuff:
                                    ExecutorContext.databuffer_size = self.databuffer_size
                                    ExecutorContext.databuff = [None] * ExecutorContext.databuffer_size
                            else:
                                #found the cached file, get the data from local cached file
                                tmp_data_file = None
                                datafilewriter = None
    
                            created = timezone.timestamp()
                            indexdatasets = {}
                            if ExecutorContext.indexbuffs is None:
                                ExecutorContext.indexbuffs = {}
                            with h5py.File(tmp_index_file,'a') as tmp_h5:
                                while True:
                                    indexbuff_baseindex = 0
                                    indexbuff_index = 0
                                    databuff_index = 0
                                    with (self.get_srcdatafilereader(src_data_file) if src_data_file else self.get_datafilereader(data_file))  as datafilereader:
                                        for item in datafilereader.rows:
                                            #check the filter first
                                            if src_data_file:
                                                #data are retrieved from source, should execute the filter logic
                                                excluded = False
                                                for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
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
                                                ExecutorContext.databuff[databuff_index] = item
                                                databuff_index += 1
                                                if databuff_index == ExecutorContext.databuffer_size:
                                                    #databuff is full, flush to file 
                                                    datafilewriter.writerows(ExecutorContext.databuff)
                                                    databuff_index = 0
                                            
                                            #generate the dataset for each index column
                                            for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
                                                value = item[columnindex]
    
                                                for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable,column_refresh_required in reportcolumns[1]:
                                                    if  not column_filterable and not column_groupable and not column_statistical:
                                                        #no need to create index 
                                                        continue
                                                    if process_required_columns and column_name not in process_required_columns:
                                                        #this is not the first run, and this column no nedd to process again
                                                        continue
    
                                                    column_size = column_columninfo.get("size",64) if column_columninfo else 64
                        
                                                    #create the buffer and hdf5 dataset for column
                                                    if column_name not in indexdatasets:
                                                        if column_name in tmp_h5:
                                                            indexdatasets[column_name] = tmp_h5[column_name]
                                                        else:
                                                            indexdatasets[column_name] = tmp_h5.create_dataset(column_name, (dataset_size,),dtype=datatransformer.get_hdf5_type(column_dtype,column_columninfo))
                                                        indexdatasets[column_name].attrs["created"] = created

                                                    if column_name not in ExecutorContext.indexbuffs:
                                                        ExecutorContext.indexbuffs[column_name] = np.empty((ExecutorContext.buffer_size,),dtype=datatransformer.get_np_type(column_dtype,column_size))
                        
                                                    #get the index data for each index column
                                                    if column_transformer:
                                                        #data  transformation is required
                                                        if column_columninfo and column_columninfo.get("parameters"):
                                                            ExecutorContext.indexbuffs[column_name][indexbuff_index] = datatransformer.transform(column_transformer,value,databaseurl=self.databaseurl,columnid=column_columnid,context=context,record=item,columnname=column_name,**column_columninfo["parameters"])
                                                        else:
                                                            ExecutorContext.indexbuffs[column_name][indexbuff_index] = datatransformer.transform(column_transformer,value,databaseurl=self.databaseurl,columnid=column_columnid,context=context,record=item,columnname=column_name)
                                                    else:
                                                        if datatransformer.is_int_type(column_dtype):
                                                            try:
                                                                value = int(value.strip()) if value else 0
                                                            except:
                                                                value = 0
                                                        elif datatransformer.is_float_type(column_dtype):
                                                            try:
                                                                value = float(value.strip()) if value else 0
                                                            except:
                                                                value = 0
                                                        else:
                                                            #remove non printable characters
                                                            value = value.encode("ascii",errors="ignore").decode().strip() if value else ""
                                                            #value is too long,cut to the column size
                                                            if len(value) >= column_size:
                                                                value = value[0:column_size]
    
                                                        ExecutorContext.indexbuffs[column_name][indexbuff_index] = value
                        
                                                    if indexbuff_index == ExecutorContext.buffer_size - 1:
                                                        #buff is full, write to hdf5 file
                                                        try:
                                                            indexdatasets[column_name].write_direct(ExecutorContext.indexbuffs[column_name],np.s_[0:ExecutorContext.buffer_size],np.s_[indexbuff_baseindex:indexbuff_baseindex + ExecutorContext.buffer_size])
                                                        except Exception as ex:
                                                            logger.debug("Failed to write {2} records to dataset({1}) which are save in hdf5 file({0}).{3}".format(tmp_index_file,column_name,ExecutorContext.buffer_size,str(ex)))
                                                            raise
            
                                                        lock.renew()
                        
                                            indexbuff_index += 1
                                            if indexbuff_index == ExecutorContext.buffer_size:
                                                #buff is full, data is already saved to hdf5 file, set indexbuff_index and indexbuff_baseindex
                                                indexbuff_index = 0
                                                indexbuff_baseindex += ExecutorContext.buffer_size
                                                logger.debug("indexbuff_baseindex = {}".format(indexbuff_baseindex))
        
                                    if src_data_file:
                                        if databuff_index > 0:
                                            #still have some data in data buff, flush it to file
                                            datafilewriter.writerows(ExecutorContext.databuff[:databuff_index])
    
                                        datafilewriter.close()
                                        #rename the tmp data file to data file if required
                                        os.rename(tmp_data_file,data_file)
    
                                        if not ExecutorContext.resource_harvester.is_local():
                                            #src data file is a temp file , delete it
                                            utils.remove_file(src_data_file)
                                            pass
                                        #set src_data_file to None, next run will read the data from data_file directly
                                        src_data_file = None
                                        logger.info("file({0}) which contains {1} rows, {2} rows were processed, {3} rows were ignored ".format(data_file,dataset_size,indexbuff_baseindex + indexbuff_index,excluded_rows))
    
                                    
                                    logger.debug("indexbuff_baseindex = {},indexbuff_index = {}, excluded_rows = {}".format(indexbuff_baseindex,indexbuff_index,excluded_rows))
                                    #still have data in buff, write them to hdf5 file
                                    if indexbuff_index > 0:
                                        for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
                                            for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable,column_refresh_requested in reportcolumns[1]:
                                                if  not column_filterable and not column_groupable and not column_statistical:
                                                    continue
                        
                                                if process_required_columns and column_name not in process_required_columns:
                                                    #this is not the first run, and this column no nedd to process again
                                                    continue
                        
                                                indexdatasets[column_name].write_direct(ExecutorContext.indexbuffs[column_name],np.s_[0:indexbuff_index],np.s_[indexbuff_baseindex:indexbuff_baseindex + indexbuff_index])
    
                                    if context.get("reprocess"):
                                        #some columns need to be reprocess again
                                        process_required_columns.clear()
                                        for col in context.get("reprocess"):
                                            process_required_columns.add(col)
                                        context.get("reprocess").clear()
                                        logger.debug("The columns({1}) are required to reprocess for dataset({0})".format(self.datasetid,process_required_columns))
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
                                    datafilewriter.close()
                                except:
                                    pass
    
                        #rename the tmp file to index file
                        if excluded_rows:
                            #some rows are excluded from access log, the size of the index dataset should be shrinked to (dataset_baseindex + buff_index)
                            dataset_size = indexbuff_baseindex + indexbuff_index
                            tmp2_index_file = "{}.tmp2".format(data_index_file)
                            with h5py.File(tmp2_index_file,'w') as tmp2_h5:
                                with h5py.File(tmp_index_file,'r') as tmp_h5:
                                    for columnindex,reportcolumns in ExecutorContext.allreportcolumns.items():
                                        for column_columnid,column_name,column_dtype,column_transformer,column_columninfo,column_statistical,column_filterable,column_groupable,column_refresh_requested in reportcolumns[1]:
                                            if  not column_filterable and not column_groupable and not column_statistical:
                                                continue
                                            try:
                                                tmp2_h5.create_dataset(column_name,dtype=tmp_h5[column_name].dtype,data=tmp_h5[column_name][0:dataset_size])
                                                tmp2_h5[column_name].attrs["created"] = created
                                            except Exception as ex:
                                                logger.error("Failed to resize the dataset.file={} column={}, before size={}, after size={}. {}".format(data_index_file,column_name,tmp_h5[column_name].shape[0],dataset_size,str(ex)))
                                                raise
    
                            os.rename(tmp2_index_file,data_index_file)
    
                            utils.remove_file(tmp_index_file)
                        else:
                            os.rename(tmp_index_file,data_index_file)
                    return [[*data,ExecutorContext.DOWNLOADED]]
                except AlreadyLocked as ex:
                    logger.debug("The index file({1}) is downloading by other executor({0}).{2}".format(data_file,data_index_file,ex))
                    return [[*data,ExecutorContext.DOWNLOADING_BY_OTHERS]]
        except harvester.exceptions.ResourceNotFound as ex:
            if self.ignore_missing_datafile:
                return [[*data,ExecutorContext.RESOURCE_NOT_FOUND]]
            else:
                raise

report_condition_id = lambda i : 1000 + i
report_group_by_id = lambda i:2000 + i
resultset_id = lambda i:3000 + i

class DatasetAppReportExecutor(DatasetColumnConfig):
    def __init__(self,task_timestamp,reportid,databaseurl,datasetid,datasetinfo,report_conditions,report_group_by,resultset,report_type):
        self.task_timestamp = task_timestamp
        self.reportid = reportid
        self.databaseurl = databaseurl
        self.datasetid = datasetid
        self.datasetinfo = datasetinfo
        self.report_conditions = report_conditions
        self.report_group_by = report_group_by
        self.resultset = resultset
        self.report_type = report_type
         
    def report_details(self,data_file,indexes):
        index_index = 0
        row_index = 0
        with self.get_datafilereader(data_file) as datareader:
            for row in datareader.rows:
                if row_index == indexes[index_index]:
                    yield row
                    index_index += 1
                    if index_index >= len(indexes):
                        break
                row_index += 1

    def run(self,data):
        """
        Analysis a single nginx access log file
        params:
            data: a tupe (datafile start time, datafile end time, datafile name)
        """
        import h5py
        import numpy as np
        import pandas as pd
        try:
            dataset_time = timezone.parse(data[0])
            logger.debug("dataset_time = {}".format(dataset_time))
    
            if not ExecutorContext.can_share_context(self.task_timestamp,self.reportid,ExecutorContext.ANALYSIS,self.datasetid):
                if not ExecutorContext.report_cache_dir:
                    cache_dir = self.cachefolder
                    if not cache_dir:
                        raise Exception("Nissing the configuration 'cache_dir'")
    
                    ExecutorContext.data_cache_dir = os.path.join(cache_dir,"data")
                    ExecutorContext.report_cache_dir = os.path.join(cache_dir,"report")

                #load the dataset column settings, a map between columnindex and a tuple(includes and excludes,(id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn))
                if ExecutorContext.column_map is None:
                    ExecutorContext.column_map = {}
                    with database.Database(self.databaseurl).get_conn(True) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable,refresh_requested from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(self.datasetid))
                            for d in cursor.fetchall():
                                ExecutorContext.column_map[d[2]] = (d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9])

    
                #find the  share data buffer used for filtering ,group by and statistics
                ExecutorContext.report_data_buffers = {}
                int_buffer = None
                float_buffer = None
                string_buffer = None
                #A map to show how to use the share data buff in filtering, group by and resultset.
                #key will be the id of the member from filtering or group by or resultset if the column included in the list member will use the databuff;
                #value is the data buff(int_buffer or float_buffer or string_buffer) which the column should use.
        
                #conditions are applied one by one, so it is better to share the buffers among conditions
                if self.report_conditions:
                    #apply the conditions, try to share the np array among conditions to save memory
                    i = -1
                    for item in self.report_conditions:
                        i += 1
                        col = ExecutorContext.column_map[item[0]]
                        col_type = (col[EXECUTOR_DTYPE],col[EXECUTOR_COLUMNINFO].get("size") if col[EXECUTOR_COLUMNINFO] else None)
                        if datatransformer.is_int_type(col_type[0]):
                            if int_buffer:
                                int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                            else:
                                int_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[report_condition_id(i)] = int_buffer
                        elif datatransformer.is_float_type(col_type[0]):
                            if float_buffer:
                                float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                            else:
                                float_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[report_condition_id(i)] = float_buffer
                        elif datatransformer.is_string_type(col_type[0]):
                            if string_buffer:
                                string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                            else:
                                string_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[report_condition_id(i)] = string_buffer
        
                if self.report_group_by:
                    #group_by feature is required
                    #Use pandas to implement 'group by' feature, all dataset including columns in 'group by' and 'resultset' will be loaded into the memory.
                    #use the existing data buff if it already exist for some column
                    if ExecutorContext.report_data_buffers:
                        closest_int_column = [None,None,None] #three members (the item with lower type, the item with exact type,the item with upper type), each memeber is None or list with 2 members:[ group_by or resultset item,col_type]
                        closest_float_column = [None,None,None]
                        closest_string_column = [None,None,None]
                        get_item_id = report_group_by_id
                        seq = -1
                        for item in itertools.chain(self.report_group_by,["|"],self.resultset):
                            seq += 1
                            if item == "|":
                                #A separator between report group by and reset set
                                get_item_id = resultset_id
                                seq = -1
                                continue
                            itemid = get_item_id(seq)
                            colname = item[0] if isinstance(item,list) else item
                            if colname == "*":
                                continue
                            col = ExecutorContext.column_map[colname]

                            if datatransformer.is_int_type(col[EXECUTOR_DTYPE]):
                                data_buffer = int_buffer 
                                closest_column = closest_int_column
                            elif datatransformer.is_float_type(col[EXECUTOR_DTYPE]):
                                data_buffer = float_buffer 
                                closest_column = closest_float_column
                            elif datatransformer.is_string_type(col[EXECUTOR_DTYPE]):
                                data_buffer = string_buffer
                                closest_column = closest_string_column
                            else:
                                continue

                            if data_buffer:
                                #a same type data buffer used by report condition, try to share the buffer between report condition and (group-by or reportresult)
                                col_type = (col[EXECUTOR_DTYPE],col[EXECUTOR_COLUMNINFO].get("size") if data_buffer == string_buffer and col[EXECUTOR_COLUMNINFO] else None)
                                if closest_column[1] and closest_column[1][1] == data_buffer[0]:
                                    #already match exactly
                                    pass
                                elif data_buffer[0] == col_type:
                                    #match exactly
                                    closest_column[1] = [itemid,col_type]
                                else:
                                    t = datatransformer.ceiling_type(data_buffer[0],col_type)
                                    if t == data_buffer[0]:
                                        #the buffer can hold the current  column, the column's type is less then buffer's type
                                        #choose the column which is closest to buffer type
                                        if closest_column[0]:
                                            if datatransformer.bigger_type(closest_column[0][1],col_type) == col_type:
                                                closest_column[0] = [itemid,col_type]
                                        else:
                                            closest_column[0] = [itemid,col_type]
                                    elif t == col_type:
                                        #the column can hold the buffer data, the column's type is greater then buffer's type
                                        #choose the column which is closest to buffer type
                                        if closest_column[2]:
                                            if datatransformer.bigger_type(closest_column[2][1],col_type) == closest_column[2][1]:
                                                closest_column[2] = [itemid,col_type]
                                        else:
                                            closest_column[2] = [itemid,col_type]
                                    else:
                                        #both the column and buff can't hold each other,the result type is greater then buffer's type and the column type
                                        #choose the column which is closest to buffer type except the current chosed column's type can hold buffer data
                                        if closest_column[2]:
                                            if datatransformer.ceiling_type(data_buffer[0],closest_column[2][1]) == closest_column[2][1]:
                                                #the current chosed cloest column's type can hold buffer data
                                                pass
                                            elif datatransformer.bigger_type(closest_column[2][1],col_type) == closest_column[2][1]:
                                                #the current chosed cloest column's type can't hold buffer data, choose the smaller type
                                                closest_column[2] = [itemid,col_type]
                                        else:
                                            closest_column[2] = [itemid,col_type]
        
        
        
                        #choose the right column to share the data buffer between report condition and (group by or reportresult)
                        for data_buffer,closest_column in (
                            (int_buffer,closest_int_column),
                            (float_buffer,closest_float_column),
                            (string_buffer,closest_string_column)):
                            if closest_column[1]:
                                #one column has the same type as int_buffer
                                ExecutorContext.report_data_buffers[closest_column[1][0]] = data_buffer
                            elif closest_column[2] and datatransformer.ceiling_type(data_buffer[0],closest_column[2][1]) == closest_column[2][1]:
                                #one column has a data type which can holder the data type of the buffer, use the column's data type as buffer's data type
                                data_buffer[0] = closest_column[2][1]
                                ExecutorContext.report_data_buffers[closest_column[2][0]] = data_buffer
                            elif closest_column[0] and datatransformer.ceiling_type(data_buffer[0],closest_column[0][1]) == closest_column[0][1]:
                                #the data type of the buffer can hold the data type of the column
                                ExecutorContext.report_data_buffers[closest_column[0][0]] = data_buffer
                            elif closest_column[0]:
                                #choose the ceiling type of the closest smaller type and buffer type
                                data_buffer[0] = datatransformer.ceiling_type(data_buffer[0],closest_column[0][1])
                                ExecutorContext.report_data_buffers[closest_column[0][0]] = data_buffer
                            elif closest_column[2]:
                                #choose the ceiling type of the closest greater type and buffer type
                                data_buffer[0] = datatransformer.ceiling_type(data_buffer[0],closest_column[2][1])
                                ExecutorContext.report_data_buffers[closest_column[2][0]] = data_buffer
        
                elif isinstance(self.resultset,(list,tuple)):
                    #group_by feature is not required.
                    #perform the statistics one by one, try best to share the data buffer
                    i = -1
                    for item in self.resultset:
                        i += 1
                        if item[0] == "*":
                            continue
                        col = ExecutorContext.column_map[item[0]]
                        if datatransformer.is_int_type(col[EXECUTOR_DTYPE]):
                            col_type = (col[EXECUTOR_DTYPE],None)
                            if int_buffer:
                                int_buffer[0] = datatransformer.ceiling_type(int_buffer[0],col_type)
                            else:
                                int_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[resultset_id(i)] = int_buffer
                        elif datatransformer.is_float_type(col[EXECUTOR_DTYPE]):
                            col_type = (col[EXECUTOR_DTYPE],None)
                            if float_buffer:
                                float_buffer[0] = datatransformer.ceiling_type(float_buffer[0],col_type)
                            else:
                                float_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[resultset_id(i)] = float_buffer
                        elif datatransformer.is_string_type(col[EXECUTOR_DTYPE]):
                            col_type = (col[EXECUTOR_DTYPE],col[EXECUTOR_COLUMNINFO].get("size") if col[EXECUTOR_COLUMNINFO] else None)
                            if string_buffer:
                                string_buffer[0] = datatransformer.ceiling_type(string_buffer[0],col_type)
                            else:
                                string_buffer = [col_type,None]
                            ExecutorContext.report_data_buffers[resultset_id(i)] = string_buffer
        
            cache_folder = os.path.join(ExecutorContext.data_cache_dir,dataset_time.strftime("%Y-%m-%d"))
            #get the cached local data file and data index file
            data_file = os.path.join(cache_folder,data[2])
            data_index_file = os.path.join(cache_folder,"{}.hdf5".format(data[2]))
    
            #the varialbe for the filter result.
            cond_result = None
            column_data = None
    
            with h5py.File(data_index_file,'r') as index_h5:
                #filter the dataset
                for ds in index_h5.values():
                    dataset_size = ds.shape[0]
                    break

                if self.report_conditions:
                    #apply the conditions, try to share the np array among conditions to save memory

                    if ExecutorContext.cond_result is None:
                        ExecutorContext.cond_result = np.empty((dataset_size,),dtype=bool)
                        ExecutorContext.cond_result.fill(True)
                        cond_result = ExecutorContext.cond_result
                    elif ExecutorContext.cond_result.shape[0] < dataset_size:
                        ExecutorContext.cond_result.resize((dataset_size,))
                        ExecutorContext.cond_result.fill(True)
                        cond_result = ExecutorContext.cond_result
                    elif ExecutorContext.cond_result.shape[0] > dataset_size:
                        ExecutorContext.cond_result.fill(True)
                        cond_result = ExecutorContext.cond_result[:dataset_size]
                    else:
                        ExecutorContext.cond_result.fill(True)
                        cond_result = ExecutorContext.cond_result

                    previous_item = None
                    seq = -1
                    conds = []
                    for cond in itertools.chain(self.report_conditions,[("$",)]):
                        if previous_item and previous_item != cond[0] and conds:
                            #process the conditions for the previous column
                            itemid = conds[0][0]
                            col = ExecutorContext.column_map[conds[0][1][0]]
                            buffer_size = self.columnbuffer_size(col) or dataset_size
                            if buffer_size > dataset_size:
                                buffer_size = dataset_size

                            #a config to control how to read the data from h5 file to memory
                            read_direct = self.column_read_direct(col)
                            logger.debug("To check the report conditons, Load the data of the column({}) from h5 file one by one.buffer={}".format(cond[0],buffer_size if buffer_size < dataset_size else 0))

                            #find the buffer to hold the data
                            buffer = ExecutorContext.report_data_buffers.get(itemid)
                            if buffer:
                                if buffer[1] is None:
                                    buffer[1] = np.empty((buffer_size,),dtype=datatransformer.get_np_type(*buffer[0]))
                                    column_data = buffer[1]
                                elif buffer[1].shape[0] < buffer_size:
                                    buffer[1].resize((buffer_size,))
                                    column_data = buffer[1]
                                elif buffer[1].shape[0] > buffer_size:
                                    column_data = buffer[1][:buffer_size]
                                else:
                                    column_data = buffer[1]
                            else:
                                column_data = np.empty((buffer_size,),dtype=datatransformer.get_np_type(col[EXECUTOR_DTYPE],col[EXECUTOR_COLUMNINFO]))
                                ExecutorContext.report_data_buffers[itemid] = [(col[EXECUTOR_DTYPE],col[EXECUTOR_COLUMNINFO]),column_data]

                            ds = index_h5[col[EXECUTOR_COLUMNNAME]]
                            #load the data from file to buffer and check the condition
                            if buffer_size == dataset_size:
                                #buffer size is dataset's size
                                #load the data from file to buffer
                                if read_direct:
                                    ds.read_direct(column_data,np.s_[0:dataset_size],np.s_[0:dataset_size])
                                else:
                                    i = 0
                                    if datatransformer.is_string_type(col[EXECUTOR_DTYPE]):
                                        while i < dataset_size:
                                            if cond_result[i]:
                                                #only read the data which is selected by the previous conditons
                                                column_data[i] = ds[i].decode() 
                                            i += 1
                                    else:
                                        while i < dataset_size:
                                            if cond_result[i]:
                                                #only read the data which is selected by the previous conditons
                                                column_data[i] = ds[i] 
                                            i += 1
                                #check the conditions
                                for itemid,col_cond in conds:
                                    cond_result &= operation.get_func(col[EXECUTOR_DTYPE],col_cond[1])(column_data,col_cond[2])
                            else:
                                #buffer size is smaller than dataset's size
                                start_index = 0
                                while start_index < dataset_size:
                                    end_index = start_index + buffer_size
                                    if end_index >= dataset_size:
                                        end_index = dataset_size
                                        data_len = end_index - start_index
                                    else:
                                        data_len = buffer_size

                                    #load the data from file to buffer
                                    if read_direct:
                                        index_h5[col[EXECUTOR_COLUMNNAME]].read_direct(column_data,np.s_[start_index:end_index],np.s_[0:data_len])
                                    else:
                                        i = start_index
                                        j = 0
                                        if datatransformer.is_string_type(col[EXECUTOR_DTYPE]):
                                            while i < end_index:
                                                if cond_result[i]:
                                                    #only read the data which is selected by the previous conditons
                                                    column_data[j] = ds[i].decode() 
                                                i += 1
                                                j += 1
                                        else:
                                            while i < end_index:
                                                if cond_result[i]:
                                                    #only read the data which is selected by the previous conditons
                                                    column_data[j] = ds[i]
                                                i += 1
                                                j += 1

                                    #check the conditions
                                    v_cond_result = cond_result[start_index:end_index]
                                    for itemid,col_cond in conds:
                                        if data_len < buffer_size:
                                            v_cond_result &= operation.get_func(col[EXECUTOR_DTYPE],col_cond[1])(column_data[:data_len],col_cond[2])
                                        else:
                                            v_cond_result &= operation.get_func(col[EXECUTOR_DTYPE],col_cond[1])(column_data,col_cond[2])

                                    start_index += buffer_size

                            conds.clear()

                        if cond == "$":
                            #finsihed
                            continue
                        seq += 1
                        itemid = report_condition_id(seq)
                        previous_item = cond[0]
                        conds.append((itemid,cond))
    
                    column_data = None
    
                    if cond_result is None:
                        filtered_rows = 0
                    else:
                        filtered_rows = np.count_nonzero(cond_result)
                else:
                    filtered_rows = dataset_size
    
    
                logger.debug("{}: {} records are selected by the report condition ({})".format(utils.get_processid(),filtered_rows,self.report_conditions))
                if self.resultset == "__details__":
                    #return the detail logs
                    if filtered_rows == 0:
                        logger.debug("{}: No data found.file={}, report condition = {}".format(utils.get_processid(),data[2],self.report_conditions))
                        return [(data[0],data[1],data[2],0,None)]
                    else:
                        reportfile_folder = os.path.join(ExecutorContext.report_cache_dir,"tmp")
                        utils.mkdir(reportfile_folder)
                        reportfile = os.path.join(reportfile_folder,"{0}-{2}-{3}{1}".format(*os.path.splitext(data[2]),self.reportid,data[0]))
                        logger.debug("{}: return result in file. file={}, report condition = {}".format(utils.get_processid(),data[2],self.report_conditions))
                        if filtered_rows == dataset_size:
                            #all logs are returned
                            #unlikely to happen.
                            shutil.copyfile(data_file,reportfile)
                            return [(data[0],data[1],data[2],dataset_size,reportfile)]
                        else:
                            report_size = np.count_nonzero(cond_result)
                            indexes = np.flatnonzero(cond_result)
                            with self.get_datafilewriter(file=reportfile) as reportwriter:
                                reportwriter.writerows(self.report_details(data_file,indexes))
                            return [(data[0],data[1],data[2],report_size,reportfile)]
    
                elif self.report_group_by :
                    #'group by' enabled
                    #create pandas dataframe
                    if filtered_rows == 0:
                        result = []
                    else:
                        buffer_size = None
                        for item in itertools.chain(self.report_group_by,self.resultset):
                            colname = item[0] if isinstance(item,(list,tuple)) else item
                            if colname == "*":
                                continue
                            col = ExecutorContext.column_map[colname]
                            size = self.columnbuffer_size(col) or dataset_size

                            if size > dataset_size:
                               size = dataset_size

                            if buffer_size is None:
                                buffer_size = size
                            elif buffer_size > size:
                                buffer_size = size

                        df_datas = collections.OrderedDict()
        
                        #populate the statistics map
                        statics_map = collections.OrderedDict()
                        for item in self.resultset:
                            if item[0] == "*":
                                #use the first group by column to calculate the count
                                colname = self.report_group_by[0]
                            else:
                                colname = item[0]
                            if colname in statics_map:
                                statics_map[colname].append(operation.get_agg_func(item[1]))
                            else:
                                statics_map[colname] = [operation.get_agg_func(item[1])]

                        start_index = 0
                        end_index = 0
                        result = []
                        #load the data from file to a configued buffer to prevent from out of memory
                        while start_index < dataset_size:
                            end_index = start_index + buffer_size
                            if end_index >= dataset_size:
                                end_index = dataset_size

                            result_size = (end_index - start_index) if filtered_rows == dataset_size else np.count_nonzero(cond_result[start_index:end_index])
                            logger.debug("{}: {} records are selected by the report condition ({}),access log file={}, start_index={}, end_index={} ".format(utils.get_processid(),result_size,self.report_conditions,data[2],start_index,end_index))
                            if result_size == 0:
                                start_index += buffer_size
                                continue

                            get_item_id = report_group_by_id
                            seq = -1
                            previous_item = None
                            df_datas.clear()
                            if self.report_type != NoneReportType:
                                df_datas["__request_time__"] = self.report_type.format(dataset_time)

                            for item in itertools.chain(self.report_group_by,["|"],self.resultset):
                                seq += 1
                                if item == "|":
                                    #A separator between report group by and reset set
                                    get_item_id = resultset_id
                                    seq = -1
                                    continue
                                itemid = get_item_id(seq)
                                colname = item[0] if isinstance(item,(list,tuple)) else item
                                if colname == "*":
                                    continue
                                if not previous_item or previous_item != colname:
                                    previous_item = colname
                                    col = ExecutorContext.column_map[colname]
                                    col_type = col[EXECUTOR_DTYPE]
                                    buffer = ExecutorContext.report_data_buffers.get(itemid)
                                    read_direct = self.column_read_direct(col)
                                    if buffer:
                                        if buffer[1] is None:
                                            buffer[1] = np.empty((buffer_size,),dtype=datatransformer.get_np_type(*buffer[0]))
                                            column_data = buffer[1]
                                        elif buffer[1].shape[0] < buffer_size:
                                            buffer[1].resize((buffer_size,))
                                            column_data = buffer[1]
                                        elif buffer[1].shape[0] > buffer_size:
                                            column_data = buffer[1][:buffer_size]
                                        else:
                                            column_data = buffer[1]
                                    else:
                                        column_data = np.empty((dataset_size,),dtype=datatransformer.get_np_type(col_type,col[EXECUTOR_COLUMNINFO]))
                                        ExecutorContext.report_data_buffers[itemid] = [(col_type,col[EXECUTOR_COLUMNINFO]),column_data]
                             
                                    ds = index_h5[colname]
                                    if read_direct:
                                        #to reduce the file io, read all data into memory
                                        data_len = end_index - start_index
                                        ds.read_direct(column_data,np.s_[start_index:end_index],np.s_[0:data_len])
                                    else:
                                        if filtered_rows == dataset_size:
                                            #all data are selected by the condition
                                            i = 0
                                            if datatransformer.is_string_type(col_type):
                                                for x in ds[start_index:end_index]:
                                                    column_data[i] = x.decode() 
                                                    i += 1
                                            else:
                                                for x in ds[start_index:end_index]:
                                                    column_data[i] = x
                                                    i += 1
                                            data_len = end_index - start_index
                                        else:
                                            #part of the data are selected by the condition,only read the selected data from file
                                            data_len = 0
                                            j = start_index
                                            if datatransformer.is_string_type(col_type):
                                                while j < end_index:
                                                    if cond_result[j]:
                                                        #selected by the condition
                                                        column_data[data_len] = ds[j].decode()
                                                        data_len += 1
                                                    j += 1

                                            else:
                                                while j < end_index:
                                                    if cond_result[j]:
                                                        #selected by the condition
                                                        column_data[data_len] = ds[j]
                                                        data_len += 1
                                                    j += 1
    
                                if filtered_rows == dataset_size:
                                    #all records are satisfied with the report condition
                                    df_datas[colname] = column_data[:data_len]
                                elif read_direct:
                                    df_datas[colname] = column_data[:data_len][cond_result[start_index:end_index]]
                                else:
                                    df_datas[colname] = column_data[:data_len]
            
                            #create pandas dataframe
                            df = pd.DataFrame(df_datas)
                            #get the group object
                            if self.report_type == NoneReportType:
                                df_group = df.groupby(self.report_group_by,group_keys=True)
                            else:
                                df_group = df.groupby(["__request_time__",*self.report_group_by],group_keys=True)
                            #perfrom the statistics on group
                            df_result = df_group.agg(statics_map)
        
                            for d in zip(df_result.index, zip(*[df_result[c] for c in df_result.columns])):
                                result.append(d)

                            start_index += buffer_size
                else:
                    #no 'group by', return the statistics data.
                    if filtered_rows == 0:
                        report_data = [0] * len(self.resultset)

                        if self.report_type == NoneReportType:
                            result = [report_data]
                        else:
                            #return a dict to perform the function 'reducebykey'
                            result = [(dataset_time.strftime("%Y-%m-%d 00:00:00"),report_data)]

                    else:
                        buffer_size = None
                        for item in self.resultset:
                            if item[0] == '*':
                                size = dataset_size
                            else:
                                col = ExecutorContext.column_map[item[0]]
                                size = self.columnbuffer_size(col) or dataset_size

                                if size > dataset_size:
                                   size = dataset_size

                            if buffer_size is None:
                                buffer_size = size
                            elif buffer_size > size:
                                buffer_size = size

                        start_index = 0
                        end_index = 0
                        result = []
                        #load the data from file to a configued buffer to prevent from out of memory
                        while start_index < dataset_size:
                            end_index = start_index + buffer_size
                            if end_index >= dataset_size:
                                end_index = dataset_size

                            result_size = (end_index - start_index) if filtered_rows == dataset_size else np.count_nonzero(cond_result[start_index:end_index])
                            if result_size == 0:
                                start_index += buffer_size
                                continue

                            report_data = []

                            previous_item = None
                            seq = -1
                            read_direct = None
                            for item in self.resultset:
                                seq += 1
                                itemid = resultset_id(seq)
                                if not previous_item or previous_item[0] != item[0]:
                                    #new column should be loaded
                                    previous_item = item
                                    data_len = end_index - start_index
                                    if item[0] != "*":
                                        col = ExecutorContext.column_map[item[0]]
                                        col_type = col[EXECUTOR_DTYPE]
                                        buffer = ExecutorContext.report_data_buffers.get(itemid)
                                        read_direct = self.column_read_direct(col)
                                        if buffer:
                                            if buffer[1] is None:
                                                buffer[1] = np.empty((buffer_size,),dtype=datatransformer.get_np_type(*buffer[0]))
                                                column_data = buffer[1]
                                            elif buffer[1].shape[0] < buffer_size:
                                                buffer[1].resize((buffer_size,))
                                                column_data = buffer[1]
                                            elif buffer[1].shape[0] > buffer_size:
                                                column_data = buffer[1][:buffer_size]
                                            else:
                                                column_data = buffer[1]
                                        else:
                                            column_data = np.empty((buffer_size,),dtype=datatransformer.get_np_type(col_type,col[EXECUTOR_COLUMNINFO]))
                                            ExecutorContext.report_data_buffers[itemid] = [(col_type,col[EXECUTOR_COLUMNINFO]),column_data]
                                    
                                        ds = index_h5[item[0]]
                                        if read_direct :
                                            #to reduce the file io, read all data into memory
                                            ds.read_direct(column_data,np.s_[start_index:end_index],np.s_[0:data_len])
                                        else:
                                            if filtered_rows == dataset_size:
                                                #all data are selected by the condition
                                                i = 0
                                                if datatransformer.is_string_type(col_type):
                                                    for x in ds[start_index:end_index]:
                                                        column_data[i] = x.decode() 
                                                        i += 1
                                                else:
                                                    for x in ds[start_index:end_index]:
                                                        column_data[i] = x
                                                        i += 1
                                            else:
                                                #part of the data are selected by the condition,only read the selected data from file
                                                data_len = 0
                                                j = start_index
                                                if datatransformer.is_string_type(col_type):
                                                    while j < end_index:
                                                        if cond_result[j]:
                                                            #selected by the condition
                                                            column_data[data_len] = ds[j].decode()
                                                            data_len += 1
                                                        j += 1

                                                else:
                                                    while j < end_index:
                                                        if cond_result[j]:
                                                            #selected by the condition
                                                            column_data[data_len] = ds[j]
                                                            data_len += 1
                                                        j += 1
                
                                if item[0] == "*":
                                    report_data.append(result_size)
                                elif filtered_rows == dataset_size:
                                    report_data.append(operation.get_func(col[2],item[1])(column_data[:data_len]))
                                elif read_direct:
                                    report_data.append(operation.get_func(col[2],item[1])(column_data[:data_len][cond_result[start_index:end_index]]))
                                else:
                                    report_data.append(operation.get_func(col[2],item[1])(column_data[:data_len]))

                            if self.report_type == NoneReportType:
                                result.append(report_data)
                            else:
                                #return a dict to perform the function 'reducebykey'
                                result.append((dataset_time.strftime("%Y-%m-%d 00:00:00"),report_data))

                            start_index += buffer_size
                        
                logger.debug("{} : Return the result from executor.reportid={}, access log file={}".format(utils.get_processid(),self.reportid,data[2]))
                logger.debug("****************\n{}".format(result))
                return result
        finally:
            pass

DRIVER_COLUMNID=0
DRIVER_DTYPE=1
DRIVER_TRANSFORMER=2
DRIVER_COLUMNINFO=3
DRIVER_STATISTICAL=4
DRIVER_FILTERABLE=5
DRIVER_GROUPABLE=6

class ReportAlreadyGenerated(Exception):
    pass
    
class DatasetAppDownloadDriver(DatasetColumnConfig):
    task_timestamp = None
    def __init__(self):
        super().__init__()
        #env parameters
        self.databaseurl = None
        self.datasetid = None

        #app_configs
        self.column_map = {} #map between column name and [columnid,dtype,transformer,statistical,filterable,groupable]
        self.datasetname = None
        self.datasetinfo = None
        self.dataset_refresh_requested = None

        self.datafiles = None
        self.missingfiles = None

    def load_env(self):
        self.datasetid = os.environ.get("DATASET")
        if not self.datasetid:
            raise Exception("Missing env variable 'DATASET'")
        self.datasetid = int(self.datasetid)

        #get enviro nment variable passed by report 
        self.databaseurl = os.environ.get("DATABASEURL")
        if not self.databaseurl:
            raise Exception("Missing env variable 'DATABASEURL'")

    def load_app_config(self):
        with database.Database(self.databaseurl).get_conn(True) as conn:
            with conn.cursor() as cursor:
                self._load_app_config(conn,cursor)

    def _load_app_config(self,conn,cursor):
        cursor.execute("select name,datasetinfo,refresh_requested from datascience_dataset where id = {}".format(self.datasetid))
        dataset = cursor.fetchone()
        if dataset is None:
            raise Exception("Dataset({}) doesn't exist.".format(self.datasetid))
        self.datasetname,self.datasetinfo,self.dataset_refresh_requested = dataset

        cursor.execute("select name,id,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} ".format(self.datasetid))
        for row in cursor.fetchall():
            self.column_map[row[0]] = [row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
    
    def post_init(self):
        #validate the datasetinfo
        self.datasetconfig_validate()

    def find_datafiles(self):
        pass

    def download_datafiles(self,spark):
        #download the file first,download files in one executor
        concurrency = self.download_concurrency(len(self.datafiles)) 

        if not concurrency or len(self.datafiles) < concurrency:
            concurrency = len(self.datafiles)

        if not concurrency:
            concurrency = 1

        rdd = spark.sparkContext.parallelize(self.datafiles, concurrency) 
        rdd = rdd.flatMap(DatasetAppDownloadExecutor(self.task_timestamp,self.databaseurl,self.datasetid,self.datasetinfo,self.dataset_refresh_requested,1).run)
        result = rdd.collect()
        self.missingfiles = [r[2] for r in result if r[3] == ExecutorContext.RESOURCE_NOT_FOUND]
        waitingfiles = [(r[0],r[1],r[2]) for r in result if r[3] == ExecutorContext.DOWNLOADING_BY_OTHERS]
        self.datafiles = [(r[0],r[1],r[2]) for r in result if r[3] in (ExecutorContext.DOWNLOADED,ExecutorContext.ALREADY_DOWNLOADED)]

        #downloading the wating files in sync mode
        if waitingfiles:
            logger.debug("The {0} files({1}) are downloading by other process".format(self.datasetname,waitingfiles))
            rdd = spark.sparkContext.parallelize(waitingfiles, 1) 
            rdd = rdd.flatMap(DatasetAppDownloadExecutor(self.task_timestamp,self.databaseurl,self.datasetid,self.datasetinfo,self.dataset_refresh_requested).run)
            result = rdd.collect()

            if self.missingfiles:
                self.missingfiles += [r[2] for r in result if r[3] == ExecutorContext.RESOURCE_NOT_FOUND]
            else:
                self.missingfiles = [r[2] for r in result if r[3] == ExecutorContext.RESOURCE_NOT_FOUND]

            if self.datafiles:   
                self.datafiles += [(r[0],r[1],r[2]) for r in result if r[3] in (ExecutorContext.DOWNLOADED,ExecutorContext.ALREADY_DOWNLOADED)]
            else:
                self.datafiles = [(r[0],r[1],r[2]) for r in result if r[3] in (ExecutorContext.DOWNLOADED,ExecutorContext.ALREADY_DOWNLOADED)]

    def delete_expired_datafiles(self):
        if self.cache_timeout > 0:
            data_cache_dir = os.path.join(self.cachefolder,"data")
            folders = [os.path.join(data_cache_dir,f) for f in os.listdir(data_cache_dir) if os.path.isdir(os.path.join(data_cache_dir,f))]
            logger.debug("Found {} cache folders".format(len(folders)))
            if len(folders) > self.cache_timeout:
                #some cached data files are expired
                folders.sort()
                for i in range(len(folders) - self.cache_timeout):
                    logger.debug("Remove the expired cached data file folder({1}) for {0}".format(self.datasetname,folders[i]))
                    utils.remove_dir(folders[i])

    def run(self):
        try:
            self.task_timestamp = timezone.timestamp()

            self.load_env()
            self.load_app_config()
            self.post_init()
        
            logger.debug("Begin to download the data files for {}".format(self.datasetname))

            #populate the list of nginx access log file
            self.find_datafiles()

            spark = get_spark_session()
            self.download_datafiles(spark)
    
            if self.missingfiles:
                logger.warning("The {0} files({1}) are missing".format(" , ".join(self.datasetname,self.missingfiles)))
    
            if self.datafiles:
                logger.info("The {0} files({1}) have been downloaded or already downloaded before".format(self.datasetname,self.datafiles))
            else:
                logger.info("No {0} file was downloaded.".format(self.datasetname))

            self.delete_expired_datafiles()
    
        except Exception as ex:
            logger.error("Failed to download {0} files.{1}".format(self.datasetname,traceback.format_exc()))
            raise 
    
class DatasetAppReportDriver(DatasetAppDownloadDriver):
    def __init__(self):
        super().__init__()
        #env parameters
        self.reportid = None
        self.periodic_report = False

        #app_configs
        self.report_name = None
        self.report_type = None
        self.report_group_by = None
        self.report_sort_by = None
        self.resultset = None
        self.report_status = None
    
        self.reporttime = None
        self.report_populate_status = None

        self.report_interval = None

    def merge_reportresult(self,data1,data2):
        """
         used by nreduce and reducebykey to merge the results returned by spark's map function.
        """
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

        for i in range(len(self.resultset)):
            data1[i] = operation.get_merge_func(self.resultset[i][1])(data1[i],data2[i])
        
        return data1
    
    @staticmethod
    def get_report_data_factory(pos):
        """
        Return a method to get the column data from report data
        """
        def _func(row):
            return row[pos]
    
        return _func
    
    def get_report_avg_factory(self,item):
        """
        Return a method to get the column avg from report data
        """
        count_pos = next(i for i in range(len(self.resultset)) if self.resultset[i][1] == "count")
        sum_pos = next(i for i in range(len(self.resultset)) if self.resultset[i][1] == "sum" and self.resultset[i][0] == item[0])
    
        def _func(row):
            if row[count_pos] <= 0:
                return 0
            else:
                return row[sum_pos] / row[count_pos]
    
        return _func

    def get_group_key_data_4_sortby_factory(self,pos,columnid,sort_type):
        def _func1(data):
            """
            For single group-by column.
            the key data is not a list type
            """
            if columnid is None:
                return data[0]
            else:
                return datatransformer.get_enum_key(data[0],databaseurl=self.databaseurl,columnid=columnid)
    
        def _func2(data):
            """
            For multiple group-by columns.
            the key data is a list type
            """
            if columnid is None:
                return data[0][pos]
            else:
                return datatransformer.get_enum_key(data[0][pos],databaseurl=self.databaseurl,columnid=columnid)
    
        return _func1 if len(self.report_group_by) == 1 else _func2

    @staticmethod
    def get_column_data_4_sortby_factory(f_get_column_data,sort_type):
    
        def _func(data):
            if sort_type:
                return f_get_column_data(data[1])
            else:
                return -1 * f_get_column_data(data[1])
    
        return _func

    @staticmethod
    def _group_by_key_iterator(keys):
        """
        return an iterator of group keys, (group keys can be a list or a single string
        """
        if isinstance(keys,(list,tuple)):
            for k in keys:
                yield k
        else:
            yield keys
    
    def _group_by_data_iterator(self,keys,enum_colids):
        """
        A iterator to convert the enum value to enum key if required
        The transformation is only happened for group by columns
        """
        i = 0
        for k in keys:
            if enum_colids[i]:
                yield datatransformer.get_enum_key(k,databaseurl=self.databaseurl,columnid=enum_colids[i])
            else:
                yield k
            i += 1
    
    def group_by_raw_report_iterator(self,report_result,enum_colids):
        """
        Return a iterator to iterate the group by report_result as a list data which contain the group by column data and value data, also convert the gorup by column data from enumid to enum key if required.
        params:
            report_result: a iterator of tuple(keys, values)
            enum_colids: a list with len(report_group_by) or len(keys), the corresponding memeber is column id if the related column need to convert into keys; otherwise the 
        """
        if enum_colids:
            #converting the enum id to enum key is required
            for k,v in report_result:
                yield itertools.chain(self._group_by_data_iterator(self._group_by_key_iterator(k),enum_colids),v)
        else:
            #converting the enum id to enum key is not required
            for k,v in report_result:
                yield itertools.chain(self._group_by_key_iterator(k),v)

    @staticmethod
    def resultsetrow_iterator(row,original_resultset):
        """
        Return a iterator to iterate the raw resultset to generate a list data for report
        params:
            report_result: a iterator of tuple(keys, values)
        """
        #converting the enum id to enum key is not required
        for c in original_resultset:
            yield c[3](row)

    def group_by_report_iterator(self,report_result,enum_colids,original_resultset):
        """
        Return a iterator to iterate the group by raw report_result to generate a list data for report
        params:
            report_result: a iterator of tuple(keys, values)
            enum_colids: a list with len(report_group_by) or len(keys), the corresponding memeber is column id if the related column need to convert into keys; otherwise the 
        """
        if enum_colids:
            #converting the enum id to enum key is required
            for k,v in report_result:
                yield itertools.chain(self._group_by_data_iterator(self._group_by_key_iterator(k),enum_colids),DatasetAppReportDriver.resultsetrow_iterator(v,original_resultset))
        else:
            #converting the enum id to enum key is not required
            for k,v in report_result:
                yield itertools.chain(self._group_by_key_iterator(k),DatasetAppReportDriver.resultsetrow_iterator(v,original_resultset))

    @staticmethod
    def resultset_iterator(report_result,original_resultset):
        """
        Return a iterator to iterate the raw resultset to generate a list data for report
        params:
            report_result: a iterator of tuple(keys, values)
        """
        #converting the enum id to enum key is not required
        for v in report_result:
            yield DatasetAppReportDriver.resultsetrow_iterator(v,original_resultset)

    def load_env(self):
        self.reportid =  os.environ.get("REPORTID")
        self.periodic_report = os.environ.get("PERIODIC_REPORT","false") == "true"
        #get environment variable passed by report 
        self.databaseurl = os.environ.get("DATABASEURL")
        if not self.databaseurl:
            raise Exception("Missing env variable 'DATABASEURL'")
        
    def _load_app_config(self,conn,cursor):
        cursor.execute((self.PERIODIC_REPORT_SQL if self.periodic_report else self.ADHOC_REPORT_SQL).format(self.reportid))
        report = cursor.fetchone()
        if report is None:
            raise Exception("Report({}) doesn't exist.".format(self.reportid))
        if self.periodic_report:
            self.report_name,self.datasetid,self.starttime,self.endtime,self.report_type,self.report_conditions,self.report_group_by,self.report_sort_by,self.resultset,self.report_status,self.report_interval = report
        else:
            self.report_name,self.datasetid,self.starttime,self.endtime,self.report_type,self.report_conditions,self.report_group_by,self.report_sort_by,self.resultset,self.report_status = report

        if self.periodic_report:
            if self.report_status is None:
                self.report_status  = {"report":{}}
            elif "report" not in self.report_status:
                self.report_status["report"]  = {}

            self.report_populate_status = self.report_status["report"]
        else:
            if self.report_status is None:
                self.report_status = {}
            self.report_populate_status = self.report_status

        if self.report_populate_status and self.report_populate_status.get("status") == "Succeed":
            #already succeed
            self.report_status = None
            raise ReportAlreadyGenerated()
        else:
            if "message" in self.report_populate_status:
                del self.report_populate_status["message"]

        if self.report_sort_by:
            #convert the sort type from string to bool
            for item in self.report_sort_by:
                item[1] = True if item[1] == "asc" else False

        super()._load_app_config(conn,cursor)

        self.report_populate_status["status"] = "Running"
        if self.periodic_report:
            self.report_populate_status["exec_start"] = timezone.format()
            cursor.execute("update datascience_periodicreportinstance set status='{1}' where id = {0}".format(self.reportid,json.dumps(self.report_status)))
        else:
            cursor.execute("update datascience_report set status='{1}',exec_start='{2}',exec_end=null where id = {0}".format(self.reportid,json.dumps(self.report_status),timezone.dbtime()))
            conn.commit()
    
    def post_init(self):
        super().post_init()
        if self.report_type:
            self.report_type = intervals.get_interval(self.report_type)
            if self.report_type.ID < self.data_interval.ID:
                raise Exception("The intreval represented by Report type should be larger than data interval")
        else:
            self.report_type = NoneReportType
        
        if self.report_interval:
            self.report_interval = intervals.get_interval(self.report_interval)



    def run(self):
        """
        generate report
        """
        try:
            reportfile = None
            reportsize = None

            self.task_timestamp = timezone.timestamp()

            logger.debug("Begin to generate the report({})".format(self.reportid))
            self.load_env()
            self.load_app_config()
            self.post_init()
            self.find_datafiles()
            spark = get_spark_session()
            self.download_datafiles(spark)
            if self.datafiles:
                logger.info("The {0} files({1}) have been downloaded or already downloaded before".format(self.datasetname,self.datafiles))
            else:
                logger.info("No {0} file was downloaded.".format(self.datasetname))

    
            if self.missingfiles:
                if self.report_populate_status is None:
                    self.report_populate_status = {"message":"The files({}) are missing".format(" , ".join(self.missingfiles))}
                else:
                    self.report_populate_status["message"] = "The files({}) are missing".format(" , ".join(self.missingfiles))
    
            if not self.datafiles:
                self.report_populate_status["status"] = "Succeed"
                reportfile = None
                reportsize = None
                return 
    
            #sort the report_conditions
            if self.report_conditions:
                #sort the report conditions, the conditions with read_direct column will occur before the other columns.
                self.report_conditions.sort(key=lambda cond:(0 if self.column_map[cond[0]][DRIVER_COLUMNINFO].get("read_direct",False if datatransformer.is_string_type(self.column_map[cond[0]][DRIVER_DTYPE]) else True) else 1,cond))
                #try to map the value to internal value used by dataset
                #Append a member to each cond to indicate the mapping status: if the member is False, value is mapped or no need to map; value is True or the indexes of the data which need to be mapped.
                for cond in self.report_conditions:
                    #each condition is a tuple(column, operator, value), value is dependent on operator and column type
                    col = self.column_map[cond[0]]
                    col_type = col[DRIVER_DTYPE]
    
                    #"select columnindex,id,name,dtype,transformer,columninfo,statistical,filterable,groupable from datascience_datasetcolumn where dataset_id = {} order by columnindex".format(datasetid))
                    #map the value to internal value used by dataset
                    cond[2] = cond[2].strip() if cond[2] else cond[2]
                    cond[2] = json.loads(cond[2])
                    if isinstance(cond[2],list):
                        for i in range(len(cond[2])):
                            if col[DRIVER_TRANSFORMER]:
                                #need transformation
                                if datatransformer.is_enum_func(col[DRIVER_TRANSFORMER]):
                                    #is enum type
                                    cond[2][i] = datatransformer.get_enum(cond[2][i],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID])
                                else:
                                    if col[DRIVER_COLUMNINFO] and col[DRIVER_COLUMNINFO].get("parameters"):
                                        cond[2][i] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2][i],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID],**col[DRIVER_COLUMNINFO]["parameters"])
                                    else:
                                        cond[2][i] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2][i],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID])
                            elif datatransformer.is_string_type(col[DRIVER_DTYPE]):
                                #encode the string value
                                cond[2][i] = (cond[2][i] or "").encode()
                            elif datatransformer.is_int_type(col[DRIVER_DTYPE]):
                                cond[2][i] = int(cond[2][i]) if cond[2][i] or cond[2][i] == 0 else None
                            elif datatransformer.is_float_type(col[DRIVER_DTYPE]):
                                cond[2][i] = float(cond[2][i]) if cond[2][i] or cond[2][i] == 0 else None
                            else:
                                raise Exception("Type({}) Not Supported".format(col[DRIVER_DTYPE]))
                        cond[2] = [v for v in cond[2] if v is not None]
                        if not cond[2]:
                            #no data
                            self.report_populate_status["status"] = "Succeed"
                            reportfile = None
                            reportsize = None
                            return 
                    else:
                        if col[DRIVER_TRANSFORMER]:
                            #need transformation
                            if datatransformer.is_enum_func(col[DRIVER_TRANSFORMER]):
                                #is enum type
                                cond[2] = datatransformer.get_enum(cond[2],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID])
                            else:
                                if col[DRIVER_COLUMNINFO] and col[DRIVER_COLUMNINFO].get("parameters"):
                                    cond[2] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID],**col[DRIVER_COLUMNINFO]["parameters"])
                                else:
                                    cond[2] = datatransformer.transform(col[DRIVER_TRANSFORMER],cond[2],databaseurl=self.databaseurl,columnid=col[DRIVER_COLUMNID])
                        elif datatransformer.is_string_type(col[DRIVER_DTYPE]):
                            #encode the string value
                            cond[2]= (cond[2] or "").encode()
                        elif datatransformer.is_int_type(col[DRIVER_DTYPE]):
                            cond[2]= int(cond[2]) if cond[2] or cond[2] == 0 else None
                        elif datatransformer.is_float_type(col[DRIVER_DTYPE]):
                            cond[2]= float(cond[2]) if cond[2] or cond[2] == 0 else None
                        else:
                            raise Exception("Type({}) Not Supported".format(col[DRIVER_DTYPE]))
                        if cond[2] is None:
                            #no data
                            self.report_populate_status["status"] = "Succeed"
                            reportfile = None
                            reportsize = None
                            return 
    
            #if resultset contains a column '__all__', means this report will return acess log details, ignore other resultset columns
            #if 'count' is in resultset, change the column to * and also check whether resultset has multiple count column
            if not self.resultset:
                self.resultset = "__details__"
                original_resultset = None
            else:
                count_column = None
                count_column_required = False
                found_avg = False
                found_sum = False
                previous_item = None
                #backup the original resultset, deep clone
                original_resultset = [[c[0],c[1],c[2] if c[2] else ("{}-{}".format(c[0],c[1]) if c[1] else c[0])] for c in self.resultset]
                self.resultset.sort()
                for i in range(len(self.resultset) - 1,-1,-1):
                    item = self.resultset[i]
                    if previous_item and previous_item[0] != item[0]:
                        #the condition with  new column
                        if found_avg and not found_sum:
                            #found column 'avg', but not found column 'sum',add a column 'sum'
                            self.resultset.insert(i + 1,[previous_item[0],"sum","{}_sum".format(previous_item[0])])
                        found_avg = False
                        found_sum = False
                    if previous_item and previous_item[0] == item[0] and previous_item[1] == item[1]:
                        #this is a duplicate statistical column, delete it
                        del self.resultset[i]
                        continue
    
                    #always use set previous_item to item to check whether the current column is the duplicate statistical column
                    previous_item = item
    
                    if item[0] == "__all__" :
                        #a  detail log report can't contain any statistics data.
                        self.resultset = "__details__"
                        break
                    elif not item[1]:
                        raise Exception("Missing aggregation method on column({1}) for report({0})".format(self.reportid,item[0]))
                    elif item[1] == "count":
                        #remove all count columns from resultset first, and add it later. 
                        if not count_column:
                            count_column = item
                            if not count_column[2]:
                                count_column[2] = "count"
                            count_column[0] = "*"
                        del self.resultset[i]
                        continue
                    elif item[1] == "avg" :
                        #perform a avg on a list of avg data is incorrect, because each access log file has different records.
                        #so avg is always calcuated through summary and count
                        #delete the column 'avg' and will add a column 'sum' if required
                        found_avg = True
                        count_column_required = True
                        del self.resultset[i]
                    elif item[1] == "sum" :
                        found_sum = True
    
                    #use a standard column name for internal processing
                    item[2] = "{}_{}".format(item[0],item[1])
                        
                    col = self.column_map[item[0]]
                    if not col[DRIVER_STATISTICAL]:
                        raise Exception("Can't apply aggregation method on non-statistical column({1}) for report({0})".format(self.reportid,item[0]))
                    if self.report_group_by and item[0] in self.report_group_by:
                        raise Exception("Can't apply aggregation method on group-by column({1}) for report({0})".format(self.reportid,item[0]))
                
                if self.resultset != "__details__":
                    if found_avg and not found_sum:
                        #found column 'avg', but not found column 'sum',add a column 'sum'
                        self.resultset.insert(0,[previous_item[0],"sum","{}_sum".format(previous_item[0])])
                    self.resultset.sort()
                    count_column_index = -1
                    if count_column:
                        #add the column 'count' back to resultset
                        #use the first data column in resultset as the data column of count_column
                        count_column[0] = "*"
                        self.resultset.insert(0,count_column)
                        count_column_index = 0
                    elif count_column_required:
                        #at least have one avg column, add a count column to implment avg feature
                        #column 'count' not found, add one
                        count_column = ["*","count","count"]
                        self.resultset.insert(0,count_column)
                        count_column_index = 0
    
            if self.resultset == "__details__":
                #this report will return all log details, report_group_by is meanless
                self.report_group_by = None
                self.report_sort_by = None
                self.report_type = NoneReportType
            else:
                if self.report_group_by:
                    for item in self.report_group_by:
                        if item not in self.column_map:
                            raise Exception("The group-by column({1}) does not exist for report({0})".format(self.reportid,item))
                        if not self.column_map[item][DRIVER_GROUPABLE]:
                            raise Exception("The group-by column({1}) is not groupable for report({0})".format(self.reportid,item))
                else:
                    #group_by is not enabled, all report data are statistical data,and only contains one row,
                    #report sort by is useless
                    self.report_sort_by = None
    
            concurrency = self.report_concurrency(len(self.datafiles)) 
            if not concurrency or len(self.datafiles) < concurrency:
                concurrency = len(self.datafiles)

            rdd = spark.sparkContext.parallelize(self.datafiles, concurrency)
            #perform the analysis per nginx access log file
            logger.debug("Begin to generate the report({0}),report condition={1},report_group_by={2},report_sort_by={3},resultset={4},report_type={5}".format(
                self.reportid,
                self.report_conditions,
                self.report_group_by,
                self.report_sort_by,
                self.resultset,
                self.report_type.NAME
            ))
            rdd = rdd.flatMap(DatasetAppReportExecutor(self.task_timestamp,self.reportid,self.databaseurl,self.datasetid,self.datasetinfo,self.report_conditions,self.report_group_by,self.resultset,self.report_type).run)
    
            #init the folder to place the report file
            report_cache_dir = os.path.join(self.cachefolder,"report")
            if self.periodic_report:
                reportfile_folder = os.path.join(report_cache_dir,"periodic",self.reportid)
            else:
                reportfile_folder = os.path.join(report_cache_dir,"adhoc",self.reporttime.strftime("%Y-%m-%d"))
            utils.mkdir(reportfile_folder)
    
            if self.resultset == "__details__":
                result = rdd.collect()
                result.sort()
                if self.data_headers:
                    report_header_file = os.path.join(report_cache_dir,"detail_report_header.csv")
                    if not os.path.exists(report_header_file):
                        #report_header_file does not exist, create it
                        with open(report_header_file,'w') as f:
                            writer = csv.writer(f)
                            writer.writerow(self.data_headers)
                else:
                    report_header_file = None
    
                result = [r for r in result if r[4]]
                if len(result) == 0:
                    logger.debug("No data found")
                    self.report_populate_status["status"] = "Succeed"
                    reportfile = None
                    reportsize = None
                    return 

                if self.report_interval:
                    reportfile = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}{}".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_interval.format4filename(self.starttime),
                        self.report_interval.format4filename(self.endtime),
                        self.report_type.NAME,
                        os.path.splitext(result[0][4])[1])
                    )
                else:
                    reportfile = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}{}".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_type.format4filename(self.starttime),
                        self.report_type.format4filename(self.endtime),
                        self.report_type.NAME,
                        os.path.splitext(result[0][4])[1])
                    )
                if report_header_file:
                    #write the data header as report header
                    files = [r[4] for r in result]
                    files.insert(0,report_header_file)
                    utils.concat_files(files,reportfile)
                    for r in result:
                        utils.remove_file(r[4])
                else:
                    if len(result) == 1:
                        os.rename(result[0][4],reportfile)
                    else:
                        utils.concat_files([r[4] for r in result],reportfile)
                        for r in result:
                            utils.remove_file(r[4])
                logger.debug("report file = {}".format(reportfile))
                self.report_populate_status["status"] = "Succeed"
                if report_header_file:
                    self.report_populate_status["report_header"] = True
                    reportsize = datafile.reader("csv",reportfile,has_header=True).records
                else:
                    self.report_populate_status["report_header"] = False
                    reportsize = datafile.reader("csv",reportfile,has_header=False).records
                return 
            else:
                if self.report_interval:
                    reportfile_raw = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}-raw.csv".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_interval.format4filename(self.starttime),
                        self.report_interval.format4filename(self.endtime),
                        self.report_type.NAME
                    ))
                    reportfile = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}.csv".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_interval.format4filename(self.starttime),
                        self.report_interval.format4filename(self.endtime),
                        self.report_type.NAME
                    ))
                else:
                    reportfile_raw = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}-raw.csv".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_type.format4filename(self.starttime),
                        self.report_type.format4filename(self.endtime),
                        self.report_type.NAME
                    ))
                    reportfile = os.path.join(reportfile_folder,"{}-{}-{}-{}-{}.csv".format(
                        self.reportid,
                        self.report_name.replace(" ","_"),
                        self.report_type.format4filename(self.starttime),
                        self.report_type.format4filename(self.endtime),
                        self.report_type.NAME
                    ))

                if self.report_group_by:
                    if self.report_type == NoneReportType:
                        report_result = rdd.reduceByKey(self.merge_reportresult).collect()
                    else:
                        #the result is grouped by report_type
                        #a new column is added to the group_by
                        report_result = rdd.reduceByKey(self.merge_reportresult).collect()
                        self.report_group_by.insert(0,"__request_time__")
                        if self.report_sort_by:
                            self.report_sort_by.insert(0,["__request_time__",True])
                        else:
                            self.report_sort_by = [["__request_time__",True]]
                else:
                    if self.report_type == NoneReportType:
                        report_result = [rdd.reduce(self.merge_reportresult)]
                    else:
                        #the result is grouped by report_type
                        #a new column is added to resultset
                        report_result = rdd.reduceByKey(self.merge_reportresult).collect()
                        self.report_group_by = ["__request_time__"]
                        self.report_sort_by = [["__request_time__",True]]
    
                if not report_result:
                    self.report_populate_status["status"] = "Succeed"
                    reportfile = None
                    reportsize = None
                    return 
    
                #find the logic to get the report row data from report raw row data
                for item in original_resultset:
                    if item[1] == "avg":
                        item.append(self.get_report_avg_factory(item))
                    elif item[1] == "count":
                        item.append(self.get_report_data_factory(next(i for i in range(len(self.resultset)) if self.resultset[i][1] == "count" )))
                    else:
                        item.append(self.get_report_data_factory(next(i for i in range(len(self.resultset)) if self.resultset[i][0] == item[0] and self.resultset[i][1] == item[1] )))
    
                if self.report_group_by:
                    #find the column ids for columns which will be converted from int value to enum key
                    enum_colids = None
                    for i in range(len(self.report_group_by)):
                        item = self.report_group_by[i]
                        if item == "__request_time__":
                            continue
                        colid = self.column_map[item][DRIVER_COLUMNID]  if self.column_map[item][DRIVER_TRANSFORMER] and datatransformer.is_enum_func(self.column_map[item][DRIVER_TRANSFORMER]) else None
                        if colid is not None:
                            if not enum_colids:
                                enum_colids = [None] * len(self.report_group_by)
                            enum_colids[i] = colid
                    #sort the data if required
                    if self.report_sort_by:
                        for i in range(len(self.report_sort_by) - 1,-1,-1): 
                            item = self.report_sort_by[i]
                            try:
                                #sort-by column is a group-by column
                                pos = self.report_group_by.index(item[0])
                                if item[0] == "__request_time__":
                                    item.append(self.get_group_key_data_4_sortby_factory(pos,None,item[1]))
                                else:
                                    col = self.column_map[item[0]]
                                    if enum_colids and enum_colids[pos] and not datatransformer.is_group_func(col[DRIVER_TRANSFORMER]):
                                        item.append(self.get_group_key_data_4_sortby_factory(pos,enum_colids[pos],item[1]))
                                    else:
                                        item.append(self.get_group_key_data_4_sortby_factory(pos,None,item[1]))
                                
                            except ValueError as ex:
                                #sort-by column is a resultset column
                                pos = next((i for i in range(len(original_resultset)) if original_resultset[i][2] == item[0] ),-1)
                                if pos == -1:
                                    #invalid sorg-by column
                                    del self.report_sort_by[i]
                                else:
                                    item.append(self.get_column_data_4_sortby_factory(original_resultset[pos][3],item[1]))
    
    
                        if self.report_sort_by:
                            if len(self.report_sort_by) == 1:
                                report_result = sorted(report_result,key=self.report_sort_by[0][2])
                            else:
                                report_result = sorted(report_result,key=lambda data:[ item[2](data) for item in self.report_sort_by])
    
                elif self.report_type != NoneReportType:
                    #sort by request_time
                    report_result.sort(key=lambda d:d[0])

                #save the report raw data to file and also convert the enumeration data back to string
                with open(reportfile_raw, 'w', newline='') as f:
                    writer = csv.writer(f)
                    #writer header
                    if self.report_group_by:
                        writer.writerow([("request_time" if c == "__request_time__" else c) for c in itertools.chain(self.report_group_by,[c[2] for c in self.resultset])])
                    else:
                        writer.writerow([ c[2] or c[0] for c in self.resultset])
                    #write rows
                    if self.report_group_by:
                        #save the data
                        writer.writerows(self.group_by_raw_report_iterator(report_result,enum_colids))
                    else:
                        #group_by is not enabled, all report data are statistical data
                        writer.writerows(report_result)
     
                #save the report to file and also convert the enumeration data back to string
                with open(reportfile, 'w', newline='') as f:
                    writer = csv.writer(f)
                    #writer header
                    if self.report_group_by:
                        writer.writerow([("request_time" if c == "__request_time__" else c) for c in itertools.chain(self.report_group_by,[c[2] for c in original_resultset])])
                    else:
                        writer.writerow([ c[2] or c[0] for c in original_resultset])
                    #write rows
                    if self.report_group_by:
                        #save the data
                        writer.writerows(self.group_by_report_iterator(report_result,enum_colids,original_resultset))
                    else:
                        #group_by is not enabled, all report data are statistical data
                        writer.writerows(self.resultset_iterator(report_result,original_resultset))
    
                self.report_populate_status["status"] = "Succeed"
                self.report_populate_status["raw_report"] = reportfile_raw
                self.report_populate_status["report_header"] = True
                reportfile = reportfile
                reportsize = datafile.reader("csv",reportfile,has_header=True).records
        except ReportAlreadyGenerated as ex:
            #report already generated
            pass
        except Exception as ex:
            msg = "Failed to generate the report.report={}.{}".format(self.reportid,traceback.format_exc())
            logger.error(msg)
            if self.report_status is None:
                if self.periodic_report:
                    if self.report_status is None:
                        self.report_status  = {"report":{}}
    
                    self.report_populate_status = self.report_status["report"]
                else:
                    self.report_status = {}
                    self.report_populate_status = self.report_status
            self.report_populate_status["status"] = "Failed"
            self.report_populate_status["message"] = base64.b64encode(msg.encode()).decode()
            raise 
        finally:
            if self.report_status:
                with database.Database(self.databaseurl).get_conn(True) as conn:
                    with conn.cursor() as cursor:
                        if self.periodic_report:
                            self.report_populate_status["exec_end"] = timezone.format()
    
                            cursor.execute("update datascience_periodicreportinstance set rawfile='{1}',reportsize={2}, status='{3}' where id = {0}".format(
                                self.reportid,
                                reportfile if reportfile else 'null',
                                'null' if reportsize is None else reportsize,
                                json.dumps(self.report_status)
                            ))
                        else:
                            cursor.execute("update datascience_report set reportfile='{1}',reportsize={2}, status='{3}', exec_end='{4}' where id = {0}".format(
                                self.reportid,
                                reportfile if reportfile else 'null',
                                'null' if reportsize is None else reportsize,
                                json.dumps(self.report_status),
                                timezone.dbtime()
                            ))
                        conn.commit()
    
            self.delete_expired_datafiles()
    
