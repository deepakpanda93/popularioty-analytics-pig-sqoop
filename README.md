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

Pig scripts get the input file as a parameter, and also the destination folder. For example, the following line would try to read the input.txt file in the Desktop folder, and it will create folders containing the output of the execution inside ~/Desktop again:

`  pig -param INPUTFILE=/home/user/Desktop/input.txt -param OUTFOLDER=/home/user/Desktop normalize_runtime_info.pig`

* normalize_runtime_info.pig: Normalizes the data, and places two new folders in the destination, namely, so_popularity_and_activity and stream_popularity_and_activity. 

### Sqoop

Sqoop scripts accept a single (optional) parameter that, if provided, should point to a folder in the Hadoop file system, where the result text file will be placed in.
In case this parameter is not provided it will be stored in the local data folder. In any case, only one file is placed (if the parameter is passed then the data folder won't include the result file)

The scripts provided are the following:
* import runtime: Brings data for the runtime tracking system to the hdfs. It uses Couchbase hadoop connector at the moment.
* import feedback: Imports the feedback, and metafeedback buckets from couchbase to hdfs.

### Python

Due to an issue when exporting couchbase documents with sqoop back... We have a temporary solution with a python script. 
This requires the python couchbase client (http://docs.couchbase.com/couchbase-sdk-python-1.2/), and therefore also the C-couchbase API (http://docs.couchbase.com/developer/c-2.4/download-install.html)

