#!/bin/bash
/opt/cloudera/parcels/CDH/bin/flume-ng agent -C /root/flume/flume-regex-0.0.1-SNAPSHOT.jar -n hdfs-xml-agent -f ./hdfs-xml-agent.conf