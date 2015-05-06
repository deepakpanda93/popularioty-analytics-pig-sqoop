#!/bin/bash

#This script imports the whole bucket from ES into the HFS
#COUCHBASE_IP=192.168.56.105
#COUCHBASE_PORT=8091
source ../env.sh
BUCKET=reputation-aggregations

input='data'
if [ -d "$1"  ]
  then "exporting dump from "$1
  input=$1
fi

echo "attempting to export "$input" to server "$COUCHBASE_POP_IP":"$COUCHBASE_POP_PORT" to bucket "$BUCKET
sqoop export  --connect http://$COUCHBASE_POP_IP:$COUCHBASE_POP_PORT/pools \
 --table couchbaseExportJob \
 --username $BUCKET \
 --export-dir $input



