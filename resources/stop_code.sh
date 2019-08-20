#!/usr/bin/env bash
~/flinkPOC/flink-1.8.0/bin/flink list | ~/flinkPOC/flink-1.8.0/bin/flink cancel `awk '{ getline;getline;print $4}'`

