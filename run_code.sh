#!/usr/bin/env bash

/Users/rkandoji/Documents/Software/flink-1.8.1/bin/flink run target/scala-2.11/flink-joiner-assembly-0.1-SNAPSHOT.jar \
--state-path "file:///Users/rkandoji/Documents/Software/flink-1.8.1/statebackend" \
--bootstrap-server b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092,b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092,b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092 \
--listings-topic la_crmls_rets-listings-neo \
--agents-topic la_crmls_rets-agents-neo \
--oh-topic la_crmls_rets-openhouses-neo \
--office-topic la_crmls_rets-offices-neo \
--media-topic la_crmls_rets-media-neo \
--history-topic la_crmls_rets-history-neo
