package com.urbancompass.data.pipeline.flink

import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.common.serialization._
import java.io.IOException

class MyAvroDeserializationSchema[T](val avroType: Class[T]) extends DeserializationSchema[T] {
  private var reader: DatumReader[T] = null
  private var decoder : BinaryDecoder = null

  def deserialize(message: Array[Byte]): T = {
    ensureInitialized()
    try {
      decoder = DecoderFactory.get.binaryDecoder(message, decoder)
      reader.read(null.asInstanceOf[T], decoder)
    }
    catch {
      case e: IOException => {
        throw new RuntimeException(e)
      }
    }
  }

  def isEndOfStream(nextElement: T): Boolean = false


  def getProducedType: TypeInformation[T] = TypeExtractor.getForClass(avroType)

  private def ensureInitialized() {
    if (reader == null) {
      if (classOf[SpecificRecordBase].isAssignableFrom(avroType)) {
        reader = new SpecificDatumReader[T](avroType)
      }
      else {
        reader = new ReflectDatumReader[T](avroType)
      }
    }
  }
}
