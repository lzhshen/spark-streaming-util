package io.github.shen.output

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.github.benfradet.spark.kafka010.writer._
import io.github.shen.utils._
import net.ceedubs.ficus.Ficus._

import scala.reflect.ClassTag

/**
  * Created by shen on 8/4/17.
  */
class KafkaOutputBeam[T: ClassTag](config: Config, ssc: StreamingContext) extends OutputBeam[T] {
  private val kafkaParams = config.as[Map[String, String]]("config.params")
  private val kafkaTopics = config.as[Set[String]]("config.topics")
  def write[K, V](dstream : DStream[T], transformFunc: T => ProducerRecord[K, V]) = {
    val p = Utils.map2Properties(kafkaParams)
    dstream.writeToKafka(p, transformFunc)
  }
}
