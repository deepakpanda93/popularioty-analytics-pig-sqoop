# popularioty-analytics-pig-sqoop
This project contains pig and sqoop scripts to import, export and do basic normalization on the data...

## Requirements

* sqoop-1.4.5 (or higher?)
* pig-0.14.0 (or higher?) 
* Hadoop connector 1.2 (http://docs.couchbase.com/admin/hadoop/hadoop-1.2.html)
* Hadoop file system (using Hadoop 2.6.0)

##The scripts

Scripts are divided in folders. That means, scripts inside sqoop use sqoop to import database information, while pig scripts execute certain data operations.

### Pig

### Sqoop

Sqoop scripts accept a single (optional) parameter that, if provided, should point to a folder in the Hadoop file system, where the result text file will be placed in.
In case this parameter is not provided it will be stored in the local data folder. In any case, only one file is placed (if the parameter is passed then the data folder won't include the result file)

The scripts provided are the following:
* import runtime: Brings data for the runtime tracking system to the hdfs. It uses Couchbase hadoop connector at the moment.
* import feedback: Imports the feedback, and metafeedback buckets from couchbase to hdfs.
