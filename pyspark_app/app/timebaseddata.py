import logging
import traceback
import os
from pyspark_app.utils import timezone

from pyspark_app import intervals
from pyspark_app.app.baseapp import DatasetAppDownloadDriver,DatasetAppReportDriver,NoneReportType

logger = logging.getLogger("pyspark_app.app.timebasedapp")

class TimeBasedDatasetAppDownloadDriver(DatasetAppDownloadDriver):
    def __init__(self):
        super().__init__()
        #env parameters
        self.starttime = None
        self.endtime = None


    def load_env(self):
        super().load_env()
        #get enviro nment variable passed by report 
        self.databaseurl = os.environ.get("DATABASEURL")
        if not self.databaseurl:
            raise Exception("Missing env variable 'DATABASEURL'")

        try:
            self.starttime = os.environ.get("START_TIME")
            if not self.starttime:
                raise Exception("Please configure env variable 'START_TIME' to download access log files")
            self.starttime = timezone.parse(self.starttime,"%Y-%m-%dT%H:%M:%S")
        except Exception as ex:
            raise Exception("Failed to parse env variable 'START_TIME'({}).{}".format(self.starttime,str(ex)))

        try:
            self.endtime = os.environ.get("END_TIME")
            if not self.endtime:
                raise Exception("Please configure env variable 'END_TIME' to download access log files")
            self.endtime = timezone.parse(self.endtime,"%Y-%m-%dT%H:%M:%S")
        except Exception as ex:
            raise Exception("Failed to parse env variable 'END_TIME'({}).{}".format(self.endtime,str(ex)))


    def post_init(self):
        super().post_init()
        self.starttime = self.data_interval.interval_starttime(starttime=self.starttime)
        self.endtime = self.data_interval.interval_starttime(starttime=self.endtime)
        self.reporttime = self.starttime

    def find_datafiles(self):
        self.datafiles = []
        for interval in self.data_interval.intervals(starttime=self.starttime,endtime=self.endtime):
            self.datafiles.append((timezone.format(interval[0]),timezone.format(interval[1]),self.get_datafilename(interval[0],interval[1])))

class TimeBasedDatasetAppReportDriver(DatasetAppReportDriver):
    ADHOC_REPORT_SQL = "select name,dataset_id,\"start\",\"end\",rtype,conditions,rawdataconditions,\"group_by\",\"sort_by\",resultset,status from datascience_report where id = {}"
    PERIODIC_REPORT_SQL = "select b.name,b.dataset_id,a.interval_start as start,a.interval_end as end,b.rtype,b.conditions,b.rawdataconditions,b.\"group_by\" as \"group_by\",b.\"sort_by\" as \"sort_by\",b.resultset,a.status,b.interval,b.id as periodic_reportid from datascience_periodicreportinstance a join datascience_periodicreport b on a.report_id = b.id where a.id = {}"

    def find_datafiles(self):
        self.datafiles = []
        for interval in self.data_interval.intervals(starttime=self.starttime,endtime=self.endtime):
            self.datafiles.append((timezone.format(interval[0]),timezone.format(interval[1]),self.get_datafilename(interval[0],interval[1])))

    def post_init(self):
        super().post_init()
        if self.periodic_report:
            self.starttime = self.report_interval.interval_starttime(starttime=self.starttime)
            self.endtime = self.report_interval.interval_starttime(starttime=self.endtime)
        elif self.report_type == NoneReportType:
            self.starttime = self.data_interval.interval_starttime(starttime=self.starttime) 
            self.endtime = self.data_interval.interval_endtime(self.data_interval.interval_starttime(starttime=self.endtime))
        else:
            self.starttime = self.report_type.interval_starttime(starttime=self.starttime) 
            self.endtime = self.report_type.interval_endtime(self.report_type.interval_starttime(starttime=self.endtime))

        self.reporttime = self.starttime

if __name__ == "__main__":
    reportid =  os.environ.get("REPORTID")
    if reportid:
        TimeBasedDatasetAppReportDriver().run()
    else:
        TimeBasedDatasetAppDownloadDriver().run()

