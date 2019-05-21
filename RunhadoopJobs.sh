#!/bin/bash
export HADOOP_HOME=/opt/hadoop-3.0.3
FILE=$1
while read line; do
     echo "$line"
     python3.5 /opt/webarchive-indexing/IndexArcs.py -r hadoop /opt/webarchive-indexing/arcsList/"$line"_ARCS.txt --step-num=1 --output-dir hdfs:///user/root/"$line" --no-output  --jobconf mapred.job.name="$line" &
done < $FILE
