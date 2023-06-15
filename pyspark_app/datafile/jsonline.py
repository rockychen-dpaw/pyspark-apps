import json
import os

from .. import serializers

format = 'jsonline'
file_ext = ".jsonl"
description="Export report data as json line file. each line is a dict object encoded as json object"

def writer(file=None,**kwargs):
    if "headers" in kwargs:
        headers = kwargs.pop("headers")
    else:
        headers = None

    if file:
        #normal file
        return JSONLineWriter(file,headers,open(file,'w'))
    else:
        #tempfile
        output = tempfile.NamedTemporaryFile(mode='w',**kwargs)
        return JSONlineWriter(output.name,headers,output)

def reader(file,headers=None,has_header=False):
    return JSONLineReader(file,headers)

class JSONLineReader(object):
    _headers_row = None
    def __init__(self,file,headers):
        self.file = file
        self._headers = headers
        self.file_input = None

    def open(self):
        if self.file_input is None:
            self.file_input = open(self.file)

    @property 
    def records(self):
        self.close()
        self.open()

        lines = 0
        try:
            data = self.file_input.readline()
            while data:
                try:
                    data = data.strip()
                    if not data:
                        continue
                    row = json.loads(data)
                    if not row:
                        continue
                    lines += 1
                finally:
                    data = self.file_input.readline()
        finally:
            self.close()

        return lines


    @property
    def rows(self):
        self.open()

        #read the headers first
        headers = self.headers

        if self._headers_row:
            yield self._headers_row

        data = self.file_input.readline()
        while data:
            try:
                data = data.strip()
                if not data:
                    continue
                row = json.loads(data)
                if not row:
                    continue

                yield [row.get(col) for col in self._headers]
            finally:
                data = self.file_input.readline()

    @property
    def headers(self):
        """
        Return column headers
        """
        if self._headers is not None:
            return self._headers

        self.open()

        data = self.file_input.readline()
        while data:
            data = data.strip()
            if not data:
                #empty row
                data = self.file_input.readline()
                continue
            row = json.loads(data)
            if not row:
                #empty row
                data = self.file_input.readline()
                continue

            self._headers = [k for k in row.keys()]
            self._headers.sort()
            self._headers_row = [row.get(col) for col in self.headers]
            return self._headers

        #empty file
        self._headers = []
        self._headers_row = []
        return self._headers

    def close(self):
        try:
            if self.file_input:
                self.file_input.close()
        except:
            pass
        self.file_input = None
        if self._headers_row:
            self._headers_row = None
            self._headers = None


    def __enter__(self):
        return self

    def __exit__(self,t,value,tb):
        self.close()
        return False if value else True

class JSONLineWriter(object):
    def __init__(self,file,headers,file_output):
        self.file = file
        self.headers = headers
        self.file_output = file_output
        self.first_row = True

    def writerows(self,rows):
        if not self.file_output:
            raise Exception("File({}) was already closed".format(self.file))
        if not rows:
            return
        jsondata = {}
        for row in rows:
            if row is None:
                continue
            self._writerrow(row,jsondata=jsondata)
    
    def _writerrow(self,row,jsondata={}):
        if not isinstance(row,dict):
            if not self.headers:
                raise Exception("row is not a dictionary data, please provide the column headers")
            for k,v in zip(self.headers,row):
                jsondata[k] = v
            row = jsondata
        elif self.headers:
            for k in self.headers:
                jsondata[k] = row.get(k)
            row = jsondata
            
        if self.first_row:
            self.first_row = False
            self.file_output.write("\r\n{}".format(json.dumps(row,cls=serializers.JSONFormater)))
        else:
            output.write(json.dumps(row,cls=serializers.JSONFormater))

    def writerow(self,row):
        if not self.file_output:
            raise Exception("File({}) was already closed".format(self.file))
        if row is None:
            return
        self._writerrow(row)

    def close(self):
        try:
            if self.file_output:
                self.file_output.close()
        except:
            pass

        self.file_output = None

    def __enter__(self):
        return self

    def __exit__(self,t,value,tb):
        self.close()
        return False if value else True
