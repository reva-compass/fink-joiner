//package com.urbancompass.data.pipeline.flink
//
///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//
//
//object WordCount {
//
//  def main(args: Array[String]) {
//
//    // Checking input parameters
//    val params = ParameterTool.fromArgs(args)
//
//    val host = "localhost"
//    val port = 9300
//
//    // get the execution environment
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // get input data by connecting to the socket
//    val text: DataStream[String] = env.socketTextStream(host, port, '\n')
//
//    // parse the data, group it, window it, and aggregate the counts
//    val windowCounts = text
//      .flatMap { w => w.split("\\s") }
//      .map { w => WordWithCount(w, 1) }
//      .keyBy("word")
//      .timeWindow(Time.seconds(5))
//      .sum("count")
//
//    // print the results with a single thread, rather than in parallel
//    windowCounts.print().setParallelism(1)
//
//    env.execute("Socket Window WordCount")
//  }
//
//  /** Data type for words with count */
//  case class WordWithCount(word: String, count: Long)
//}
