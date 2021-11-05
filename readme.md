# lagstat.py

## Overview
This is a simple command line tool to calculate and print the lag of a Kakfa topic consumed by a Spark Structured Streaming application. The tool calculate the latest lag by comparing the committed offsets in the Structured Streaming checkpoint directory in HDFS with the latest offsets from the Kafka topic. 

The main purpose of this tool is to display a rough estimation of the lag of each partition. It is useful to determine f the lag is growing or stable. The lag values reported is just an estimation and not an accurate respresentation of the amount of unprocessed events in the partitions, since the offsets in the checkpoint only gets updated at the start of the streaming trigger interval.

## Configuration

To use this tool, modify the config.ini and change the following parameters:

* TOPIC - name of the Kafka topic that the Spark Structured Streaming application is reading from
* BROKER_LIST - list of Kafka brokers in the format *host1.domain.com:9093,host2.domain.com:9093,host3.domain.com:9093*
* CHECKPOINT - the full path of the HDFS directory where the Spark Structured Streaming application is located

## Usage

```
$ ./lagstat.py -h
usage: lagstat.py [-h] [-c col] [-j]

Kafka streaming lag monitoring utility

optional arguments:
  -h, --help         show this help message and exit
  -c col, --col col  Number of columns when printing output (default=4)
  -j, --json         Print output as json
```

The script has to be run on a Cloudera server (e.g. edge node) that has the HDFS gateway role.
