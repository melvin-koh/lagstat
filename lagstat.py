#!/usr/bin/env python

import subprocess, sys, json, argparse

def get_var(varname):
  cmd = "echo $(source config.ini; echo $%s)" % varname
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
  return p.stdout.readlines()[0].strip()

##### retrieve properties from config.ini #####
TOPIC=get_var("TOPIC")
BROKER_LIST=get_var("BROKER_LIST")
CHECKPOINT=get_var("CHECKPOINT")
##############################################

SPARK_OFFSET_COMMAND="hdfs dfs -ls -t -r %s/offsets | tail -1 | awk '{print $8}' | xargs hdfs dfs -text" % CHECKPOINT
KAFKA_CONSUMER_OPTS="--consumer.config client.properties"
KAFKA_OFFSET_COMMAND="kafka-run-class kafka.tools.GetOffsetShell --broker-list %s --topic %s --time -1 %s" % (BROKER_LIST, TOPIC, KAFKA_CONSUMER_OPTS)
PRINT_COLUMN=4
OUTPUT_FORMAT=""


def parse_spark_offset(output):
  offsets_json=json.loads(output.split("\n",2)[2])  # remove first two lines
  return offsets_json


def parse_kafka_offset(output):
  offsets_json = {}
  part_offset = {}
  for line in output.split('\n'):
    if(line != ""):
      tmp = line.split(':')
      val = int(tmp[2])
      part_offset[tmp[1]] = val
      offsets_json[tmp[0]] = part_offset
  return offsets_json


def pretty_print(json, col):
  for topic, offsets in json.items():
    #keys = list(offsets.keys())
    print("\033[1mTopic Name: %s\033[0m" % topic)
    print("\t"),
    colidx = 1
    for key in sorted(offsets):
      print("Partition: %s Lag: %s" % (str(key).ljust(3), str(offsets[key]).ljust(12))),
      colidx += 1
      if colidx > PRINT_COLUMN:
        colidx = 1
        print("")
        print("\t"),


def fetch_lags():
  p1 = subprocess.Popen(SPARK_OFFSET_COMMAND, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  p2 = subprocess.Popen(KAFKA_OFFSET_COMMAND, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

  # wait for both commands to finish running
  p1.wait()
  p2.wait()

  if(p1.returncode != 0):
    err = p1.stderr.read()
    print("Unable to retrieve offsets from checkpoint. ERROR=%s" % err)
    p2.terminate()
    sys.exit(1)
 
  if(p2.returncode != 0):
    print("Unable to retrieve offsets from Kafka")
    p1.terminate()
    sys.exit(2)

  spark_offsets = parse_spark_offset(p1.stdout.read())
  kafka_offsets = parse_kafka_offset(p2.stdout.read())

  lags = {}
  for topic, offsets in kafka_offsets.items():
    part_lag = {}
    for partition, offset in offsets.items():
      part_lag[int(partition)] = offset - spark_offsets[topic][partition]
    lags[topic] = part_lag

  return lags


def main():
  lags = fetch_lags()
  if(OUTPUT_FORMAT == "json"):
    print(lags)
  else:
    pretty_print(lags, col=PRINT_COLUMN)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Kafka streaming lag monitoring utility")
  parser.add_argument("-c", "--col", type=int, metavar="col", required=False, default=PRINT_COLUMN, help="Number of columns when printing output (default=4)")
  parser.add_argument("-j", "--json", required=False, action="store_true", help="Print output as json")
  opt=parser.parse_args()

  if(opt.json):
    OUTPUT_FORMAT="json"

  PRINT_COLUMN = opt.col 
  main()
