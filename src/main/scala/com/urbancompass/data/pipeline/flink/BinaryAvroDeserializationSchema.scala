//package com.urbancompass.data.pipeline.flink
//
//import org.apache.avro.generic.GenericDatumReader
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.io.BinaryDecoder
//import org.apache.avro.io.DatumReader
//import org.apache.avro.io.DecoderFactory
//import org.apache.avro.reflect.ReflectDatumReader
//import org.apache.avro.specific.SpecificDatumReader
//import org.apache.avro.specific.SpecificRecordBase
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.TypeExtractor
//import org.apache.flink.streaming.util.serialization.DeserializationSchema
//
//@SerialVersionUID(1L)
//class BinaryAvroDeserializationSchema[T](avroType: Class[T]) extends DeserializationSchema[T] {
//
//  @transient
//  private[this] lazy val typeInfo = TypeExtractor.getForClass(avroType)
//
//  @transient
//  private[this] lazy val reader: DatumReader[T] =
//    if (avroType == classOf[GenericRecord])
//      new GenericDatumReader[T]()
//    else if (classOf[SpecificRecordBase].isAssignableFrom(avroType))
//      new SpecificDatumReader[T](avroType)
//    else
//      new ReflectDatumReader[T](avroType)
//
//  @transient
//  private[this] var decoder: BinaryDecoder = null
//
//  override def deserialize(message: Array[Byte]): T = {
//    try {
//      this.decoder = DecoderFactory.get().binaryDecoder(java.util.Base64.getDecoder.decode(message), decoder)
//      reader.read(null.asInstanceOf[T], decoder)
//    }
//    //catch {
//    //  case _: Throwable => println("Exception while deserializing")
//    //case e: Exception => throw new RuntimeException(e)
//    //}
//  }
//
//  override def isEndOfStream(nextElement: T): Boolean = false
//
//  override def getProducedType(): TypeInformation[T] = typeInfo
//
//}