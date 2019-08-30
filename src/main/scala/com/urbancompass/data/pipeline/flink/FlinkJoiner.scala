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

object FlinkJoiner {

  def main(args: Array[String]) {

//    val logger = LoggerFactory.getLogger(FlinkJoiner.getClass)
//    logger.info("### Hello from flink joiner ")

    println("### Entering Flink Joiner")
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
  //  val kafkaJoinedTopic = params.getRequired("sink-topic")

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
    kafkaListingsConsumer.assignTimestampsAndWatermarks(new CustomTimestampExtractor[ObjectNode]())

    val kafkaAgentsConsumer = new FlinkKafkaConsumer[ObjectNode](kafkaAgentsTopic, jsonDeserdeSchema, consumerProps)
    kafkaAgentsConsumer.setStartFromEarliest()
    kafkaAgentsConsumer.assignTimestampsAndWatermarks(new CustomTimestampExtractor[ObjectNode]())

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
      Types.LONG    // rowtime
    )

    def listingMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      val mapper = new ObjectMapper()
      val payload = mapper.readTree(obj.get("payload").asText())
      return Row.of(
        obj.get("trace_id").asText(),
        obj.get("data_version").asText(),
        obj.get("ts_created_at").asText(),
        payload.get("__raw_ingest_start_time__").asText(),
        payload.get("__uc_id_sha__").asText(),
        payload.get("Listing ID").asText(),
        payload.get("Agent ID").asText(),
        payload.get("Office ID").asText(),
        obj.get("payload").asText(),
        obj.get("ts_created_at")
      )
    }

    val rowAgentType = new RowTypeInfo(
      Types.STRING, // trace_id
      Types.STRING, // data_version
      Types.STRING, // ts_created_at
      Types.STRING, // __raw_ingest_start_time__
      Types.STRING, // __uc_id_sha__
      Types.STRING, // Agent ID
      Types.STRING, // Agent First Name
      Types.STRING, // Agent Last Name
      Types.STRING, // Agent Email
      Types.STRING, // Agent Timestamp
      Types.LONG    // rowtime
    )

    def agentMapper(record: ObjectNode): Row = {
      val obj = record.get("value")
      print(obj)
      val mapper = new ObjectMapper()
      val rawPayload = mapper.readTree(obj.get("payload").asText())
      return Row.of(
        obj.get("trace_id").asText(),
        obj.get("data_version").asText(),
        obj.get("ts_created_at").asText(),
        rawPayload.get("__raw_ingest_start_time__").asText(),
        rawPayload.get("__uc_id_sha__").asText(),
        rawPayload.get("ActiveAgent:Agent").get(0).get("Agent ID").asText(),
        rawPayload.get("ActiveAgent:Agent").get(0).get("First Name").asText(),
        rawPayload.get("ActiveAgent:Agent").get(0).get("Last Name").asText(),
        rawPayload.get("ActiveAgent:Agent").get(0).get("Email").asText(),
        rawPayload.get("ActiveAgent:Agent").get(0).get("Timestamp").asText(),
        obj.get("ts_created_at")
      )
    }


    //    val rowCombinedImageType = new RowTypeInfo(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP)

    val listingsStream = env.addSource(kafkaListingsConsumer).map(x => listingMapper(x))(rowListingType)
    //listingsStream.assignAscendingTimestamps(_.getField(2).asInstanceOf[Long])
    val listingsTbl = tEnv.fromDataStream(listingsStream,
      'trace_id,
      'data_version,
      'ts_created_at,
      '__raw_ingest_start_time__,
      '__uc_id_sha__,
      'listing_id,
      'agent_id,
      'office_id,
      'payload,
      'rowtime.rowtime
    )
    tEnv.registerTable("listings_tbl", listingsTbl)


    val agentsStream = env.addSource(kafkaAgentsConsumer).map(x => agentMapper(x))(rowAgentType)
    val agentsTbl = tEnv.fromDataStream(agentsStream,
      'trace_id,
      'data_version,
      'ts_created_at,
      '__raw_ingest_start_time__,
      '__uc_id_sha__,
      'agent_id,
      'agent_first_name,
      'agent_last_name,
      'agent_email,
      'agent_timestamp,
      'rowtime.rowtime
    )
    tEnv.registerTable("agents_tbl", agentsTbl)

    tEnv.registerFunction("JoinAgents",
      agentsTbl.createTemporalTableFunction('rowtime, 'agent_id))

    /*
     Query streams
     */
    val sqlQueryAgents = "select * from agents_tbl"
    tEnv.registerTable("AgentsResult", tEnv.sqlQuery(sqlQueryAgents))

    val sqlQueryListings = "select * from listings_tbl"
    tEnv.registerTable("ListingsResult", tEnv.sqlQuery(sqlQueryListings))

    /*
     Join streams
     */
    val sqlQuery =
      """
        |SELECT
        |  l.__uc_id_sha__, l.__raw_ingest_start_time__,
        |  l.listing_id, l.agent_id, l.ts_created_at, l.rowtime,
        |  a.agent_id, a.agent_first_name, a.agent_last_name, a.ts_created_at, a.rowtime
        |FROM
        |  listings_tbl AS l,
        |  LATERAL TABLE (JoinAgents(l.rowtime)) AS a
        |WHERE l.agent_id = a.agent_id
        |""".stripMargin
    tEnv.registerTable("JoinResult", tEnv.sqlQuery(sqlQuery))


    /*
     Output results to log
     */
    //listingsStream.print()
    //tEnv.scan("ListingsResult").toAppendStream[Row].print()
    //tEnv.scan("AgentsResult").toAppendStream[Row].print()

    tEnv.scan("JoinResult").toAppendStream[Row].print()

    // Execute flow
    env.execute("Flink Joiner App")


    /**
    def runStaticDataStream() = {

      val msToMins = 60000

      // Static data
      val listingData = new mutable.MutableList[(Long, String, Timestamp)]
      listingData.+=((1L, "listing 1.0", new Timestamp(1L*msToMins)))
      listingData.+=((2L, "listing 2.0", new Timestamp(1L*msToMins)))
      listingData.+=((3L, "listing 3.0", new Timestamp(1L*msToMins)))
      listingData.+=((4L, "listing 4.0", new Timestamp(1L*msToMins)))
      listingData.+=((2L, "listing 2.1", new Timestamp(3L*msToMins)))
      listingData.+=((1L, "listing 1.1", new Timestamp(4L*msToMins)))
      listingData.+=((2L, "listing 2.1", new Timestamp(10L*msToMins)))

      val imageData = new mutable.MutableList[(Long, String, Timestamp)]
      imageData.+=((1L, "Image 1.0", new Timestamp(1L*msToMins)))
      imageData.+=((1L, "Image 1.1", new Timestamp(2L*msToMins)))
      imageData.+=((1L, "Image 1.2", new Timestamp(3L*msToMins)))
      imageData.+=((2L, "Image 2.0", new Timestamp(3L*msToMins)))

      val openHouseData = new mutable.MutableList[(Long, String, Timestamp)]
      openHouseData.+=((1L, "OH 1.0", new Timestamp(1L*msToMins)))
      openHouseData.+=((1L, "OH 1.1", new Timestamp(2L*msToMins)))
      openHouseData.+=((2L, "OH 2.0", new Timestamp(2L*msToMins)))


      val listings = env
        .fromCollection(listingData)
        .assignTimestampsAndWatermarks(new TimestampExtractor[Long, String]())
        .toTable(tEnv, 'id, 'data, 'rowtime.rowtime)

      val images = env
        .fromCollection(imageData)
        .assignTimestampsAndWatermarks(new TimestampExtractor[Long, String]())
        .toTable(tEnv, 'id, 'data, 'rowtime.rowtime)

      val openHouses = env
        .fromCollection(openHouseData)
        .assignTimestampsAndWatermarks(new TimestampExtractor[Long, String]())
        .toTable(tEnv, 'id, 'data, 'rowtime.rowtime)

      tEnv.registerTable("listings", listings)
      tEnv.registerTable("images", images)
      tEnv.registerTable("open_houses", openHouses)

    }
     **/
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

class CustomTimestampExtractor[T1]
// How late can data be is passed in
  extends BoundedOutOfOrdernessTimestampExtractor[T1](Time.seconds(100))  {
  override def extractTimestamp(element: T1): Long = {
    val objectNode: ObjectNode = element.asInstanceOf[ObjectNode]
    return objectNode.get("value").get("ts_created_at").asInstanceOf[Long]
  }
}
