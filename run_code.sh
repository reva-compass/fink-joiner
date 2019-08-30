#!/usr/bin/env bash

/Users/rkandoji/Documents/Software/flink-1.8.1/bin/flink run target/scala-2.11/flink-joiner-assembly-0.1-SNAPSHOT.jar \
--bootstrap-server  b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092 \
--listings-topic poc_test_listing \
--agents-topic poc_test_agent \
--sink-topic joined

