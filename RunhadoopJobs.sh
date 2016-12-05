#!/bin/bash
export HADOOP_HOME=/opt/hadoop-1.2.1
FILE=$1
while read line; do
     echo "$line"
     python /opt/webarchive-indexing/IndexArcs.py -r hadoop /opt/webarchive-indexing/arcsList/"$line"_ARCS.txt --step-num=1 --output-dir hdfs:///user/root/"$line" --no-output  --jobconf mapred.job.name="$line" &
done < $FILE