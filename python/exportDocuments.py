#!/usr/bin/python2.7
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
import json
import sys


if len(sys.argv)==4:
  file = sys.argv[1]
  host = sys.argv[2]
  bucket = sys.argv[3]
else:
  print "usage: "+sys.argv[0]+" source_file host bucket"
  exit(1)
c = Couchbase.connect(bucket=bucket, host=host)

  

def storeDocument(key, doc):

  try:
    c.set(key,doc)
  except CouchbaseError as e:
    print "Couldn't store document with key:"+key+" and value "+str(doc)
    raise


with open(file) as f:
  while True:
    s = f.readline().replace('\n','')
    if s == '':
      break
    i = s.find(",")
    key = s[:i]
    strdoc = s[i+1:]
    doc = json.loads(strdoc)
    storeDocument(key,doc)
  
