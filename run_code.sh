#!/usr/bin/env bash

/Users/rkandoji/Documents/Software/flink-1.8.1/bin/flink run target/scala-2.11/flink-joiner-assembly-0.1-SNAPSHOT.jar \
--bootstrap-server  b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092 \
--listings-topic la_crmls_rets-listings-neo \
--agents-topic la_crmls_rets-agents-neo \
--oh-topic la_crmls_rets-openhouses-neo \
--office-topic la_crmls_rets-offices-neo \
--media-topic la_crmls_rets-media-neo \
--history-topic la_crmls_rets-history-neo

