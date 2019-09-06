package com.urbancompass.data.pipeline.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TestJoiner {

  def main(args: Array[String]) {

    //    val logger = LoggerFactory.getLogger(FlinkJoiner.getClass)
    //    logger.info("### Hello from flink joiner ")

    println("### Entering Test Joiner")
    /*
    Set up Avro schema dynamically
    Using avro4 https://github.com/sksamuel/avro4s#schemas

    2019-07-22: Not currently using this code
     */
    val messageSchema =
      """
    {
      "name": "pipeline_message",
      "type": "record",
      "namespace": "pipeline",
      "fields": [
      {"name": "trace_id", "type": "string"},
      {"name": "data_version", "type": "string"},
      {"name": "ts_created_at", "type": "string"},
      {"name": "payload", "type": "string"},
      ]
    }
    """

    /*
     Checking input parameters
     */
    val params = ParameterTool.fromArgs(args)
    val bootstrapServers = params.getRequired("bootstrap-server")
    val kafkaListingsTopic = params.getRequired("listings-topic")
    val kafkaAgentsTopic = params.getRequired("agents-topic")
    val kafkaOHTopic = params.getRequired("oh-topic")
    //val kafkaJoinedTopic = params.getRequired("sink-topic")

    /*
     Set up environment
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    //   val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(new FsStateBackend("file:///Users/rkandoji/Documents/Software/flink-1.8.1/statebackend"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.getConfig.enableForceAvro()
    env.getConfig.disableForceKryo()


    /*
     Set up Kafka details
     */
    val (consumerProps, producerProps) = getConsumerAndProducerProps(bootstrapServers)

    // Serializers used for reading topics
    val serdeSchema = new SimpleStringSchema
    val jsonDeserdeSchema = new JSONKeyValueDeserializationSchema(true)
    val kafkaListingsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaListingsTopic, jsonDeserdeSchema, consumerProps)
    kafkaListingsConsumer.setStartFromEarliest()

    val kafkaAgentsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaAgentsTopic, jsonDeserdeSchema, consumerProps)
    kafkaAgentsConsumer.setStartFromEarliest()

    val kafkaOHConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaOHTopic, jsonDeserdeSchema, consumerProps)
    kafkaOHConsumer.setStartFromEarliest()

    val rowListingType = new RowTypeInfo(
      Types.STRING, // listing_id
      Types.STRING, // earnest_$_payable_to
      Types.STRING, // status_change_date
      Types.STRING, // inclusions
      Types.STRING, // county
      Types.STRING, // agent_id
      Types.STRING, // terms_offered
      Types.STRING, // nbr_of_acres
      Types.STRING, // colisting_member_url
      Types.STRING, // colist_agent_id
      Types.STRING, // list_office_board_code
      Types.STRING // list_207
    )

    def listingMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      return Row.of(
        if (obj.has("Listing ID")) obj.get("Listing ID").asText() else "",
        if (obj.has("Earnest $ Payable To")) obj.get("Earnest $ Payable To").asText() else "",
        if (obj.has("Status Change Date")) obj.get("Status Change Date").asText() else "",
        if (obj.has("Inclusions")) obj.get("Inclusions").asText() else "",
        if (obj.has("County")) obj.get("County").asText() else "",
        if (obj.has("Agent ID")) obj.get("Agent ID").asText() else "",
        if (obj.has("Terms Offered")) obj.get("Terms Offered").asText() else "",
        if (obj.has("Nbr of Acres")) obj.get("Nbr of Acres").asText() else "",
        if (obj.has("CoListingMemberUrl")) obj.get("CoListingMemberUrl").asText() else "",
        if (obj.has("CoList Agent ID")) obj.get("CoList Agent ID").asText() else "",
        if (obj.has("List Office Board Code")) obj.get("List Office Board Code").asText() else "",
        if (obj.has("LIST_207")) obj.get("LIST_207").asText() else ""
      )
    }


    val listingsStream = env.addSource(kafkaListingsConsumer).map(x => listingMapper(x))(rowListingType)
    val keyedListingStream = listingsStream.keyBy(0)
    val listingsTbl = tEnv.fromDataStream(keyedListingStream,
      'listing_id,
      'earnest_$_payable_to,
      'status_change_date,
      'inclusions,
      'county,
      'l_agent_id,
      'terms_offered,
      'nbr_of_acres,
      'colisting_member_url,
      'l_colist_agent_id,
      'list_office_board_code,
      'list_207
    )
    tEnv.registerTable("listings_tbl", listingsTbl)

    //    val resListings = tEnv.sqlQuery("SELECT * from listings_tbl")
    ////    val lRow: DataStream[Row] = tEnv.toAppendStream[Row](resListings)
    ////    lRow.print()

    val rowAgentType = new RowTypeInfo(
      Types.STRING, // trace_id
      Types.STRING, // data_version
      Types.STRING, // ts_created_at
      Types.STRING, // __raw_ingest_start_time__
      Types.STRING, // __uc_id_sha__
      Types.STRING, // Agent ID
      Types.STRING // Agent First Name
    )

    def agentMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      return Row.of(
        if (obj.has("City")) obj.get("City").asText() else "",
        if (obj.has("Office ID")) obj.get("Office ID").asText() else "",
        if (obj.has("Email")) obj.get("Email").asText() else "",
        if (obj.has("RENegotiation Exp")) obj.get("RENegotiation Exp").asText() else "",
        if (obj.has("NRDSID")) obj.get("NRDSID").asText() else "",
        if (obj.has("MLS Status")) obj.get("MLS Status").asText() else "",
        if (obj.has("Agent ID")) obj.get("Agent ID").asText() else ""
      )
    }

    val agentsStream = env.addSource(kafkaAgentsConsumer).map(x => agentMapper(x))(rowAgentType)

    val agentsTbl = tEnv.fromDataStream(agentsStream,
      'city,
      'office_id,
      'email,
      're_negotiation_exp,
      'nrdsid,
      'mls_status,
      'agent_id
    )
    tEnv.registerTable("agents_tbl", agentsTbl)

    //    val resAgents = tEnv.sqlQuery("SELECT * from agents_tbl")
    //    val aRow: DataStream[Row] = tEnv.toAppendStream[Row](resAgents)
    //    aRow.print()

    val rowOHType = new RowTypeInfo(
      Types.STRING, // trace_id
      Types.STRING, // data_version
      Types.STRING, // ts_created_at
      Types.STRING, // __raw_ingest_start_time__
      Types.STRING, // __uc_id_sha__
      Types.STRING, // Agent ID
      Types.STRING, // Agent First Name
      Types.STRING // Agent First Name
    )

    def oHMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      return Row.of(
        if (obj.has("City")) obj.get("City").asText() else "",
        if (obj.has("Open House Comments")) obj.get("Open House Comments").asText() else "",
        if (obj.has("Event End")) obj.get("Event End").asText() else "",
        if (obj.has("Street Address")) obj.get("Street Address").asText() else "",
        if (obj.has("Listing ID")) obj.get("Listing ID").asText() else "",
        if (obj.has("Office Primary Phone")) obj.get("Office Primary Phone").asText() else "",
        if (obj.has("Event Unique ID")) obj.get("Event Unique ID").asText() else "",
        if (obj.has("Listing Agent Id")) obj.get("Listing Agent Id").asText() else ""
      )
    }

    val ohStream = env.addSource(kafkaOHConsumer).map(x => oHMapper(x))(rowOHType)

    val ohTbl = tEnv.fromDataStream(ohStream,
      'oh_city,
      'oh_comments,
      'oh_event_end,
      'oh_street_address,
      'oh_listing_id,
      'oh_office_primary_phone,
      'oh_event_unique_id,
      'oh_listing_agent_id
    )
    tEnv.registerTable("oh_tbl", ohTbl)

    //    val resOH = tEnv.sqlQuery("SELECT * from oh_tbl")
    //    val ohRow: DataStream[Row] = tEnv.toAppendStream[Row](resOH)
    //    ohRow.print()


    //    val joinQuery =
    //      """
    //        | SELECT *
    //        | FROM listings_tbl
    //        | INNER JOIN agents_tbl
    //        | ON listings_tbl.l_agent_id = agents_tbl.agent_id
    //        | """.stripMargin
    //    val result = tEnv.sqlQuery(joinQuery)
    //    val row: DataStream[Row] = tEnv.toAppendStream[Row](result)
    //    println("### INNER JOIN result")
    //    row.print()

    //    val leftJoinQuery =
    //      """
    //        | SELECT *
    //        | FROM listings_tbl l
    //        | LEFT JOIN agents_tbl a
    //        | ON l.agent_id = a.agent_id
    //        | """.stripMargin
    //    val leftResult = tEnv.sqlQuery(leftJoinQuery)
    //    val leftJoinRow: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](leftResult)
    //    println("### LEFT JOIN result")
    //    leftJoinRow.print()



//        val leftJoinQuery2 =
//          """
//            | SELECT *
//            | FROM listings_tbl l
//            | LEFT JOIN agents_tbl aa ON l.l_agent_id = aa.agent_id
//            | LEFT JOIN agents_tbl ab ON l.l_colist_agent_id = ab.agent_id
//            | """.stripMargin
//        val leftResult2 = tEnv.sqlQuery(leftJoinQuery2)
//        val leftJoinRow2: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](leftResult2)
//        println("### LEFT JOIN2 result")
//        leftJoinRow2.print()

    val leftJoinQuery2 =
      """
        | SELECT *
        | FROM listings_tbl l
        | LEFT JOIN agents_tbl aa ON l.l_agent_id = aa.agent_id
        | LEFT JOIN agents_tbl ab ON l.l_colist_agent_id = ab.agent_id
        | LEFT JOIN oh_tbl AS oh ON l.listing_id = oh.oh_listing_id
        | """.stripMargin
    val leftResult2 = tEnv.sqlQuery(leftJoinQuery2)
    val leftJoinRow2: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](leftResult2)
    println("### LEFT JOIN2 result")
    leftJoinRow2.print()

//    val nestedJoinQuery =
//      """
//        | SELECT *
//        | FROM
//        | (
//        | SELECT *
//        | FROM listings_tbl l
//        | LEFT JOIN agents_tbl a
//        | ON l.l_agent_id = a.agent_id
//        | ) AS so
//        | LEFT JOIN oh_tbl AS oh
//        | ON so.listing_id = oh.oh_listing_id
//        | """.stripMargin
//    val nestedResult = tEnv.sqlQuery(nestedJoinQuery)
//    val nestedJoinRow: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](nestedResult)
//    println("### NESTED JOIN result")
//    nestedJoinRow.print()

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

