import boto
import shutil
from mrjob.job import MRJob
from os.path import basename
import os
import errno
import re
import sys
import StringIO
import gzip
import codecs
import time
import json
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from tempfile import TemporaryFile
from pywb.warc.cdxindexer import write_cdx_index
from gzip import GzipFile
import urllib
from urllib2 import urlopen
 

WORD_RE = re.compile(r"[\w']+")

class IndexArcs(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol


    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat' 

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.job.jvm.numtasks': '-1',
                'mapreduce.input.lineinputformat.linespermap': 2, 
		'mapred.job.priority': 'VERY_HIGH'
                }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(IndexArcs, self).configure_options()
    def mapper_init(self):
        self.index_options = {
            'surt_ordered': True,
            'sort': True,
            'cdxj': True,
        }

    def mapper(self, _, line):
        attempts = 5
        warc_path = line
        try:
            self._load_and_index(warc_path)
        except Exception as exc:
            try:
                if exc.errno == errno.EPIPE:
                    if attempts > 0:
                        time.sleep(5) #wait 5 seconds - servers must be busy
                        attempts -= 1
                        self._load_and_index(warc_path) #try again in case of Broken Pipe Error  
                    else:
                        self.stderr.write("Broken Pipe tried more than 3 times.\n")
                        self.stderr.write("Arcname:\t" + warc_path+"\n" + str(exc))

                else:            
                    self.stderr.write("Arcname:\t" + warc_path+"\n" + str(exc))
            except AttributeError: #Exception has no ErrorNo lets just print the error to the output
                self.stderr.write("Arcname:\t" + warc_path+"\n" + str(exc))
            except Exception as ex2:
               self.stderr.write("Arcname:\t" + warc_path+"\n" + str(exc)) 


    def _conv_warc_to_cdx_path(self, warc_path):
        cdx_path = warc_path.replace('.arc.gz', '.cdx.gz')
        return cdx_path

    def _load_and_index(self, warc_path):
        warctempURL = urlopen(warc_path)
        warctemp=StringIO.StringIO(warctempURL.read()) 

        with TemporaryFile(mode='w+b') as cdxtemp:
            cdxfile = GzipFile(fileobj=cdxtemp, mode='w+b')
            write_cdx_index(cdxfile, warctemp, basename(warc_path), **self.index_options)
            cdxfile.close()
            cdxtemp.seek(0)
            cdxtempString=StringIO.StringIO(cdxtemp.read())
            cdxtempobj = gzip.GzipFile(fileobj=cdxtempString)
            fileLines = cdxtempobj.read().split("\n")
            for line in fileLines:
                if not (line.startswith("Error") or line.startswith("Invalid ARC record") or line.startswith("Unknown archive format")):
                    " ".join(line.split(" ")[2:])
                    jsonStr = " ".join(line.split(" ")[2:])
                    cdx_record = json.loads(jsonStr)
                    status = cdx_record.get('status', None)
                    if status:
                        if re.search(r'2.*|3.*|1.*', status):
                            self.stdout.write(line + "\n")
            cdxtemp.flush()
if __name__ == '__main__':
    IndexArcs.run()
