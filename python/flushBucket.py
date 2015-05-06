#!/usr/bin/python2.7
from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
from couchbase.admin import Admin
import json
import sys


user='admin'
password='password'
if len(sys.argv)==3:
  host = sys.argv[1]
  bucket = sys.argv[2]

else:
  print "usage: "+sys.argv[0]+" host bucket"
  exit(1)


#make an administrative connection using Admin object
try:
    admin = Admin(username=user,password=password,host=host,port=8091)
except CouchbaseError as e:
    print " Sorry , we could not create admin connection , due to " , e
else :
    print "Successfully made an admin connection "


#retrieve bucket information for bucket named "default" 
#   "default" is just the name of the bucket I set up for trying this out
try:
    htres = admin.http_request("/pools/default/buckets/"+bucket)
except Exception as e:
    print "ERROR: ", e
    sys.exit()

print "number of items before flush: ", htres.value['basicStats']['itemCount']

print htres.value['controllers']


try:
    htres = admin.http_request('/pools/default/buckets/'+bucket+'/controller/doFlush',"POST")
except Exception as e:
    print "ERROR: ", e
    sys.exit()

print "number of items after flush: ", htres.value['basicStats']['itemCount']

