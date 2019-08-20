#!/usr/bin/env bash

/Users/rkandoji/Documents/Software/flink-1.8.1/bin/flink list | /Users/rkandoji/Documents/Software/flink-1.8.1/bin/flink cancel `awk '{ getline;getline;print $4}'`

