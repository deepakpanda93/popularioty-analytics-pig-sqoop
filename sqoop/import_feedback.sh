#!/bin/bash

#source env.sh


hadoop fs -rm -r DUMP
sqoop import --username feedback --verbose \
    --connect http://$COUCHBASE_POP_IP:$COUCHBASE_POP_PORT/pools --table DUMP
hadoop fs -getmerge  DUMP feedback
hadoop fs -rm -r DUMP*
sqoop import --username meta-feedback --verbose \
    --connect http://$COUCHBASE_POP_IP:$COUCHBASE_POP_PORT/pools --table DUMP
hadoop fs -mv feedback DUMP/feedback
hadoop fs -rm DUMP.java
hadoop fs -rm DUMP/*.crc
hadoop fs -rm DUMP/_SUCCESS*
hadoop fs -getmerge  DUMP/ data/import_feedback.txt
hadoop fs -rm -r DUMP
if [ -d "$1"  ]
  then "moving dump of runtime to "$1
  hadoop fs -mv data/import_feedback.txt $1/import_feedback.txt
fi

