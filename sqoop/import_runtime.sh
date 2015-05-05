#/bin/bash

source ../env.sh

hadoop fs -rm -r DUMP
sqoop import --username reputation --verbose \
    --connect http://$COUCHBASE_IP:$COUCHBASE_PORT/pools --table DUMP
hadoop fs -getmerge  DUMP/ data/runtime_input.txt

if [ -d "$1"  ]
  then "moving dump of runtime to "$1
  haddop fs -mv data/runtime_input.txt $1/
fi

hadoop fs -rm -r DUMP
hadoop fs -rm DUMP.java
