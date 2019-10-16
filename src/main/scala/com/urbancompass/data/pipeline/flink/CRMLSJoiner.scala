package com.urbancompass.data.pipeline.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object CRMLSJoiner {

  def main(args: Array[String]) {
    println("### Entering CRMLSJoiner")

    /*
     Checking input parameters
     */
    val params = ParameterTool.fromArgs(args)
    val bootstrapServers = params.getRequired("bootstrap-server")
    val kafkaListingsTopic = params.getRequired("listings-topic")
    val kafkaAgentsTopic = params.getRequired("agents-topic")
    println(("# bootstrapServers: " + bootstrapServers))
    println("# kafkaListingsTopic: " + kafkaListingsTopic)
    println("## kafkaAgentsTopic: " + kafkaAgentsTopic)
    /*
     Set up environment
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    //   val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(new FsStateBackend("file:///Users/rkandoji/Documents/Software/flink-1.8.1/statebackend"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    env.getConfig.enableForceAvro()
    env.getConfig.disableForceKryo()

    //object reuse mode
    val config = env.getConfig
    config.enableObjectReuse()

    // checkpointing
    // start a checkpoint every 1000 ms
    env.enableCheckpointing(1000)
    // advanced options:
    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // make sure 500 ms of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    lazy val mapper = new ObjectMapper()

    /*
     Set up Kafka details
     */
    val (consumerProps, producerProps) = getConsumerAndProducerProps(bootstrapServers)

    //  val kafkaProducer = new FlinkKafkaProducer[String](bootstrapServers, "my-topic", new SimpleStringSchema)

    // Serializers used for reading topics
    val serdeSchema = new SimpleStringSchema
    val jsonDeserdeSchema = new JSONKeyValueDeserializationSchema(true)
    val kafkaListingsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaListingsTopic, jsonDeserdeSchema, consumerProps)
    kafkaListingsConsumer.setStartFromEarliest()

    val rowListingType = new RowTypeInfo(
      Types.STRING, // data
      Types.STRING, // uc_pk
      Types.STRING, // uc_update_ts
      Types.STRING, // uc_version
      Types.LONG, // uc_created_ts
      Types.STRING, // uc_row_type
      Types.STRING, // uc_type
      Types.LONG, // uc_valid_day
      Types.LONG, //uc_valid_ts
      Types.STRING, //ListAgentKeyNumeric
      Types.STRING, //BuyerAgentKeyNumeric
      Types.STRING, //CoListAgentKeyNumeric
      Types.STRING //CoBuyerAgentKeyNumeric
    )

    def listingMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      val da = obj.get("data").asText()
      val dataNode = mapper.readValue[JsonNode](da, classOf[JsonNode])
      val listAgentKey = if (dataNode.has("ListAgentKeyNumeric")) dataNode.get("ListAgentKeyNumeric").asText() else null
      val buyerAgentKey = if (dataNode.has("BuyerAgentKeyNumeric")) dataNode.get("BuyerAgentKeyNumeric").asText() else null
      val coListAgentKey = if (dataNode.has("CoListAgentKeyNumeric")) dataNode.get("CoListAgentKeyNumeric").asText() else null
      val coBuyerAgentKey = if (dataNode.has("CoBuyerAgentKeyNumeric")) dataNode.get("CoBuyerAgentKeyNumeric").asText() else null
      val createdTS: java.lang.Long = obj.get("uc_created_ts").asText().toLong
      val validDay: java.lang.Long = obj.get("uc_valid_day").asText().toLong
      val validTS: java.lang.Long = obj.get("uc_valid_ts").asText().toLong
      return Row.of(
        if (obj.has("data")) obj.get("data").asText() else "",
        if (obj.has("uc_pk")) obj.get("uc_pk").asText() else "",
        if (obj.has("uc_update_ts")) obj.get("uc_update_ts").asText() else "",
        if (obj.has("uc_version")) obj.get("uc_version").asText() else "",
        createdTS,
        if (obj.has("uc_row_type")) obj.get("uc_row_type").asText() else "",
        if (obj.has("uc_type")) obj.get("uc_type").asText() else "",
        validDay,
        validTS,
        listAgentKey,
        buyerAgentKey,
        coListAgentKey,
        coBuyerAgentKey
      )
    }

    val listingsStream = env.addSource(kafkaListingsConsumer).map(x => listingMapper(x))(rowListingType)
    val keyedListingStream = listingsStream.keyBy(0)
    val listingsTbl = tEnv.fromDataStream(keyedListingStream,
      'l_data,
      'l_uc_pk,
      'l_uc_update_ts,
      'l_uc_version,
      'l_uc_created_ts,
      'l_uc_row_type,
      'l_uc_type,
      'l_uc_valid_day,
      'l_uc_valid_ts,
      'l_list_agent_key,
      'l_buyer_agent_key,
      'l_co_list_agent_key,
      'l_co_buyer_agent_key
    )
    tEnv.registerTable("listings_tbl", listingsTbl)

    // Table with latest listings, and no duplicates
    val listingsTblTs = tEnv.sqlQuery("SELECT * FROM listings_tbl WHERE (l_uc_pk, l_uc_created_ts) IN (SELECT l_uc_pk, MAX(l_uc_created_ts) FROM listings_tbl GROUP BY l_uc_pk)")
    tEnv.registerTable("listings_tbl_ts", listingsTblTs)
    //    val lRow: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](listingsTblTs)
    //    lRow.print()


    val kafkaAgentsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaAgentsTopic, jsonDeserdeSchema, consumerProps)
    kafkaAgentsConsumer.setStartFromEarliest()
    val rowAgentType = new RowTypeInfo(
      Types.STRING, // data
      Types.STRING, // uc_pk
      Types.STRING, // uc_update_ts
      Types.STRING, // uc_version
      Types.LONG, // uc_created_ts
      Types.STRING, // uc_row_type
      Types.STRING, // uc_type
      Types.LONG, // uc_valid_day
      Types.LONG // uc_valid_ts
    )

    def agentMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      val createdTS: java.lang.Long = obj.get("uc_created_ts").asText().toLong
      val validDay: java.lang.Long = obj.get("uc_valid_day").asText().toLong
      val validTS: java.lang.Long = obj.get("uc_valid_ts").asText().toLong
      return Row.of(
        if (obj.has("data")) obj.get("data").asText() else "",
        if (obj.has("uc_pk")) obj.get("uc_pk").asText() else "",
        if (obj.has("uc_update_ts")) obj.get("uc_update_ts").asText() else "",
        if (obj.has("uc_version")) obj.get("uc_version").asText() else "",
        createdTS,
        if (obj.has("uc_row_type")) obj.get("uc_row_type").asText() else "",
        if (obj.has("uc_type")) obj.get("uc_type").asText() else "",
        validDay,
        validTS
      )
    }

    val agentsStream = env.addSource(kafkaAgentsConsumer).map(x => agentMapper(x))(rowAgentType)
    val agentsTbl = tEnv.fromDataStream(agentsStream,
      'a_data,
      'a_uc_pk,
      'a_uc_update_ts,
      'a_uc_version,
      'a_uc_created_ts,
      'a_uc_row_type,
      'a_uc_type,
      'a_uc_valid_day,
      'a_uc_valid_ts
    )
    tEnv.registerTable("agents_tbl", agentsTbl)

    // Table with latest agents, and no duplicates
    val agentsTblTs = tEnv.sqlQuery("SELECT * FROM agents_tbl WHERE (a_uc_pk, a_uc_created_ts) IN (SELECT a_uc_pk, MAX(a_uc_created_ts) FROM agents_tbl GROUP BY a_uc_pk)")
    tEnv.registerTable("agents_tbl_ts", agentsTblTs)
    //   val aRow: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](agentsTblTs)
    //    aRow.print()

    //        val leftJoinQuery =
    //          """
    //            | SELECT *
    //            | FROM listings_tbl_ts l
    //            | LEFT JOIN agents_tbl_ts a
    //            | ON l.l_list_agent_key = a.a_uc_pk
    //            | """.stripMargin
    //        val leftResult = tEnv.sqlQuery(leftJoinQuery)
    //        tEnv.registerTable("leftResult_tbl", leftResult)
    //        val leftJoinRow: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](leftResult)
    //        println("### LEFT JOIN result")
    //        leftJoinRow.print()

    val leftJoinQuery2 =
      """
        | SELECT *
        | FROM listings_tbl_ts l
        | LEFT JOIN agents_tbl_ts aa ON l.l_list_agent_key = aa.a_uc_pk
        | LEFT JOIN agents_tbl_ts ab ON l.l_buyer_agent_key = ab.a_uc_pk
        | LEFT JOIN agents_tbl_ts ac ON l.l_co_list_agent_key = ac.a_uc_pk
        | LEFT JOIN agents_tbl_ts ad ON l.l_co_buyer_agent_key = ad.a_uc_pk
        | """.stripMargin
    val leftResult2 = tEnv.sqlQuery(leftJoinQuery2)
    tEnv.registerTable("leftResult_tbl", leftResult2)
    //  val oStream: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](leftResult2)
    //    println("### LEFT JOIN2 result")
    //leftJoinRow2.print()

    val countTbl = tEnv.sqlQuery("SELECT COUNT(*) FROM leftResult_tbl")
    val cRow: DataStream[(Boolean, Long)] = tEnv.toRetractStream[Long](countTbl)
    println("### COUNT")
    // cRow.print()

    // Execute flow
    env.execute("Flink Joiner App")

  }


  private def getConsumerAndProducerProps(bootstrapServers: String): (Properties, Properties) = {

    val consumerProps = new Properties
    consumerProps.setProperty("bootstrap.servers", bootstrapServers)
    consumerProps.setProperty("group.id", s"flink-kafka-test-${
      System.currentTimeMillis
    } ")

    // Producer properties: we just need the broker list
    val producerProps = new Properties
    producerProps.setProperty("bootstrap.servers", bootstrapServers)

    // Return the properties as a pair
    (consumerProps, producerProps)

  }


}
