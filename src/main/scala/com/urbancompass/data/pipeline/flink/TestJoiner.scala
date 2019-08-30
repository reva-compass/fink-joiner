package com.urbancompass.data.pipeline.flink

import java.util
import java.util.Properties
import java.sql.Timestamp

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, Types}
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.table.sources.tsextractors.ExistingField

import scala.collection.mutable
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.io.DecoderFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sksamuel.avro4s.{AvroInputStream, AvroName, AvroNamespace, AvroSchema}

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
    val messageSchema = """
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
    //@AvroName("pipeline_message")
    //@AvroNamespace("pipeline")
    //case class kafkaData(trace_id: String, data_version: String, ts_created_at: String, payload: String)
    //val avroSchema = AvroSchema[kafkaData]
    //println("Avro Schema: " + avroSchema)

    /*
     Checking input parameters
     */
    val params = ParameterTool.fromArgs(args)
    val bootstrapServers = params.getRequired("bootstrap-server")
    val kafkaListingsTopic = params.getRequired("listings-topic")
    val kafkaAgentsTopic = params.getRequired("agents-topic")
    //val kafkaJoinedTopic = params.getRequired("sink-topic")

    /*
     Set up environment
     */
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)


    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, fsSettings)

    env.setStateBackend(new FsStateBackend("file:///Users/rkandoji/Documents/Software/flink-1.8.1/statebackend"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    env.getConfig.enableForceAvro()
    env.getConfig.disableForceKryo()

    /*
     Run on Mutable List
     */
    //runStaticDataStream()


    /*
     Set up Kafka details
     */
    val (consumerProps, producerProps) = getConsumerAndProducerProps(bootstrapServers)

    // Serializers used for reading topics
    val serdeSchema = new SimpleStringSchema
    val jsonDeserdeSchema = new JSONKeyValueDeserializationSchema(true)

    //val avroDeserdeSchema = AvroDeserializationSchema.forGeneric(avroSchema)
    //val avroDeserdeSchema = new MyAvroDeserializationSchema[kafkaData](classOf[kafkaData])
    //val deserializer = new KeyedDeserializationSchema[String](serdeSchema)

    //val binaryDeserdeSchema = new AbstractDeserializationSchema[Array[Byte]](){
    // override def deserialize(bytes: Array[Byte]): Array[Byte] = java.util.Base64.getDecoder.decode(bytes)
    //}
    //val binaryAvroDeserdeSchema = new BinaryAvroDeserializationSchema[GenericRecord](classOf[GenericRecord])

    /*
     Consumers
     */
    //val kafkaListingsConsumer = new FlinkKafkaConsumer(kafkaListingsTopic, binaryAvroDeserdeSchema, consumerProps)
    //val kafkaListingsConsumer = new FlinkKafkaConsumer(kafkaListingsTopic, avroDeserdeSchema, consumerProps)
    val kafkaListingsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaListingsTopic, jsonDeserdeSchema, consumerProps)
    kafkaListingsConsumer.setStartFromEarliest()
    //kafkaListingsConsumer.assignAscendingTimestamps(_.getCreationTime)
    //  kafkaListingsConsumer.assignTimestampsAndWatermarks(new CustomTimestampExtractor[ObjectNode]())

    val kafkaAgentsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaAgentsTopic, jsonDeserdeSchema, consumerProps)
    kafkaAgentsConsumer.setStartFromEarliest()
  //  kafkaAgentsConsumer.assignTimestampsAndWatermarks(new CustomTimestampExtractor[ObjectNode]())

    val rowListingType = new RowTypeInfo(
      Types.STRING, // trace_id
      Types.STRING, // data_version
      Types.STRING, // ts_created_at
      Types.STRING, // __raw_ingest_start_time__
      Types.STRING, // __uc_id_sha__
      Types.STRING, // Listing ID
      Types.STRING, // Agent ID
      Types.STRING, // Office ID
      Types.STRING, // payload
      Types.STRING, // payload
      Types.STRING, // Agent ID
      Types.STRING, // Office ID
      Types.STRING, // payload
      Types.STRING // payload
    )

    def listingMapper(record: ObjectNode): Row = {
      println("### listing object")
      println(record)
      val obj = record.get("value")
      return Row.of(
        obj.get("Earnest $ Payable To").asText(),
        obj.get("Status Change Date").asText(),
        obj.get("Inclusions").asText(),
        obj.get("County").asText(),
        obj.get("Agent ID").asText(),
        obj.get("Terms Offered").asText(),
        obj.get("Nbr of Acres"),
        obj.get("CoListingMemberUrl").asText(),
        obj.get("CoList Agent ID").asText(),
        obj.get("List Office Board Code").asText(),
        obj.get("LIST_207").asText()
      )
    }

    //    val rowCombinedImageType = new RowTypeInfo(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP)

   val listingsStream = env.addSource(kafkaListingsConsumer).map(x => listingMapper(x))(rowListingType)
    //listingsStream.assignAscendingTimestamps(_.getField(2).asInstanceOf[Long])
    val listingsTbl = tEnv.fromDataStream(listingsStream,
      'earnest_payable_to,
      'status_change_date,
      'inclusions,
      'county,
      'agent_id,
      'terms_offered,
      'nbr_of_acres,
      'colisting_member_url,
      'colist_agent_id,
      'list_office_board_code,
      'list_207

    )
    tEnv.registerTable("listings_tbl", listingsTbl)

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
      println("### agent record")
      println(record)
      val obj = record.get("value")
      return Row.of(
        obj.get("City").asText(),
        obj.get("Office ID").asText(),
        obj.get("Email").asText(),
        obj.get("RENegotiation Exp").asText(),
        obj.get("NRDSID").asText(),
        obj.get("MLS Status").asText(),
        obj.get("Agent ID").asText()
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


//    val result = tEnv.sqlQuery(
//      """SELECT l.earnest_payable_to,
//         l.status_change_date,
//         l.inclusions,
//         l.county,
//         l.agent_id,
//         l.terms_offered,
//         l.nbr_of_acres,
//         l.colisting_member_url,
//         l.colist_agent_id,
//         l.list_office_board_code,
//         l.list_207,
//         a.city,
//          a.office_id,
//          a.email,
//          a.re_negotiation_exp,
//          a.nrdsid,
//          a.mls_status,
//          a.agent_id
//FROM listings_tbl l
//JOIN agents_tbl a ON l.agent_id = a.agent_id""".stripMargin)
//
//    tEnv.registerTable("JoinResult", result)
//    tEnv.scan("JoinResult").toAppendStream[Row].print()

//    val dsRow: DataStream[Row] = tEnv.toAppendStream[Row](result)
//    dsRow.print()

    // Execute flow
    env.execute("Flink Joiner App")


      }



  private def getConsumerAndProducerProps(bootstrapServers: String): (Properties, Properties) = {

    // Consumer properties: put together the broker list and a unique group id
    val consumerProps = new Properties
    consumerProps.setProperty("bootstrap.servers", bootstrapServers)
    consumerProps.setProperty("group.id", s"flink-kafka-test-${System.currentTimeMillis}")

    // Producer properties: we just need the broker list
    val producerProps = new Properties
    producerProps.setProperty("bootstrap.servers", bootstrapServers)

    // Return the properties as a pair
    (consumerProps, producerProps)

  }
}

