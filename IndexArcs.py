import boto
import shutil
from mrjob.job import MRJob
from os.path import basename
import os
import errno
import re
import sys
import io
import gzip
import codecs
import time
import json
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from io import StringIO
from io import BytesIO
from tempfile import TemporaryFile
from pywb.warc.cdxindexer import write_cdx_index
from pywb.utils.canonicalize import UrlCanonicalizeException
from json.decoder import JSONDecodeError


from gzip import GzipFile
import urllib
from urllib.request import urlopen
 

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
                'mapreduce.input.lineinputformat.linespermap': 1,
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

        warc_path = line.split('\t')[1]  #need to do this for the hadoop input type need to check if hadoop or local if we want to make this also work on local mode
        try:
            self._load_and_index(warc_path)
        except UrlCanonicalizeException as exc:
            self.stderr.write( ("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8'))         
            pass
        except JSONDecodeError as exc:
            self.stderr.write( ("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8'))
            pass
        except Exception as exc:
            try:
                if exc.errno == errno.EPIPE:
                    if attempts > 0:
                        time.sleep(5) #wait 5 seconds - servers must be busy
                        attempts -= 1
                        self._load_and_index(warc_path) #try again in case of Broken Pipe Error  
                    else:
                        self.stderr.write(("Broken Pipe tried more than 3 times.\n").encode('utf-8'))
                        self.stderr.write( ("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8'))                     
                else:            
                    self.stderr.write( ("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8'))
                    pass
            except AttributeError: #Exception has no ErrorNo lets just print the error to the output
                self.stderr.write( (("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8')) )
                pass
            except Exception as ex2:
               self.stderr.write( ("Arcname:\t" + warc_path+"\n" + str(exc)).encode('utf-8'))
               pass 



    def _load_and_index(self, warc_path):
        warctempURL = urlopen(warc_path)
        warctemp=BytesIO(warctempURL.read()) 

        with TemporaryFile(mode='w+b') as cdxtemp:
            cdxfile = GzipFile(fileobj=cdxtemp, mode='w+b')
            write_cdx_index(cdxfile, warctemp, basename(warc_path), **self.index_options)
            cdxfile.close()
            cdxtemp.seek(0)
            cdxtempString=BytesIO(cdxtemp.read())
            cdxtempobj = gzip.GzipFile(fileobj=cdxtempString)
            cdxRecordsString = cdxtempobj.read().decode('utf-8')

            fileLines = cdxRecordsString.split("\n")
            for line in fileLines:
                if not (line.startswith("Error") or line.startswith("Invalid ARC record") or line.startswith("Unknown archive format")):
                    " ".join(line.split(" ")[2:])
                    jsonStr = " ".join(line.split(" ")[2:])
                    cdx_record = json.loads(jsonStr)
                    status = cdx_record.get('status', None)
                    if status:
                        if re.search(r'2.*|3.*|1.*', status):
                            self.stdout.write( (line + "\n").encode('utf-8') )               

            cdxtemp.flush()
if __name__ == '__main__':
    IndexArcs.run()
