package com.flink.example.skyeye.serialize

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.flink.example.skyeye.modul.SkyeyeLogPushES
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.nio.charset.Charset



class SkyeyeSerializeSchema(topicName: String) extends KafkaSerializationSchema[SkyeyeLogPushES]{

  private lazy val UTF8_CHARSET = Charset.forName("UTF-8")
  override def serialize(e: SkyeyeLogPushES, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

    val skyeyeJson: String = JSON.toJSONString(e,SerializerFeature.EMPTY: _*)
    new ProducerRecord(topicName, (e.event_id + e.app_key).getBytes(UTF8_CHARSET), skyeyeJson.getBytes)
  }
}
