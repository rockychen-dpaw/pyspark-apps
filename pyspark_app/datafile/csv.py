import tempfile
import os
import csv 


format = 'csv'
file_ext = ".csv"
description="Export report data as csv "

def writer(filetype_kwargs=None,file=None,**kwargs):
    if "header" in kwargs:
        header = kwargs.pop("header")
    else:
        header = None

    if file:
        #normal file
        if filetype_kwargs:
            return CSVWriter(file,open(file,'w'),header=header,**filetype_kwargs)
        else:
            return CSVWriter(file,open(file,'w'),header=header)
    else:
        #tempfile
        output = tempfile.NamedTemporaryFile(mode='w',**kwargs)
        if filetype_kwargs:
            return CSVWriter(output.name,output,header=header,**filetype_kwargs)
        else:
            return CSVWriter(output.name,output,header=header)

def reader(file,filetype_kwargs=None,has_header=True,header=None):
    if filetype_kwargs:
        return CSVReader(file,has_header,**filetype_kwargs)
    else:
        return CSVReader(file,has_header)

class CSVReader(object):
    _header = None
    def __init__(self,file,has_header,escapechar='"'):
        self.file = file
        self.has_header = has_header
        self.file_input = None
        self.reader = None
        self.escapechar = escapechar


    def open(self):
        if self.file_input is None:
            self.file_input = open(self.file)
            self.reader = csv.reader(self.file_input,escapechar=self.escapechar)

    @property 
    def records(self):
        self.close()
        self.open()
        try:
            if self.has_header:
                self.header

            lines = 0
            for row in self.reader:
                if not row:
                    continue
                lines += 1
        except Exception as ex:
            raise Exception("Failed to read csv file({0}). {1}({2})".format(self.file,ex.__class__.__name__,str(ex)))
        finally:
            self.close()

        return lines

    @property
    def rows(self):
        self.open()

        if self.has_header:
            self.header
        for row in self.reader:
            if not row:
                continue
            yield row
     
    @property
    def header(self):
        """
        Return column header
        """
        if not self.has_header:
            return None

        if self._header is not None:
            return self._header

        self.open()

        for row in self.reader:
            if row:
                self._header = row
                return self._header

        self._header = []
        return self._header

    def close(self):
        try:
            if self.file_input:
                self.file_input.close()
        except:
            pass
        self.file_input = None
        self.reader = None
        self._header = None

    def __enter__(self):
        return self

    def __exit__(self,t,value,tb):
        self.close()
        return False if value else True

class CSVWriter(object):
    def __init__(self,file,file_output,header=None,escapechar='"'):
        self.file = file
        self.escapechar = escapechar
        self.file_output = file_output
        self.header = header
        self.writer = csv.writer(self.file_output,escapechar=self.escapechar)
        if self.header:
            self.writer.writerow(self.header)

    def writerows(self,rows):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if not rows:
            return
        self.writer.writerows(rows)
    
    def writerow(self,row):
        if not self.writer:
            raise Exception("File({}) was already closed".format(self.file))
        if row is None:
            return
        self.writer.writerow(row)

    def close(self):
        try:
            if self.file_output:
                self.file_output.close()
        except:
            pass
        self.file_output = None
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self,t,value,tb):
        self.close()
        return False if value else True
