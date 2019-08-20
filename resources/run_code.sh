#!/usr/bin/env bash
~/flinkPOC/flink-1.8.0/bin/flink run target/scala-2.11/flink-joiner-assembly-0.1-SNAPSHOT.jar \
--listings-topic listings \
--images-topic images \
--sink-topic joined \
&



