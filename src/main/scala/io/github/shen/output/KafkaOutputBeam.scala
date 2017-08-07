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
class KafkaOutputBeam(config: Config, ssc: StreamingContext) extends OutputBeam {
  val kafkaParams = config.as[Map[String, String]]("config.params")
  val kafkaTopics = config.as[Array[String]]("config.topics")
  val producerConfig = Utils.map2Properties(kafkaParams)

  //def write[K, V](dstream : DStream[T], transformFunc: T => ProducerRecord[K, V])
  /*
  def write[K, V](dstream : DStream[T]): Unit = {
    val p = Utils.map2Properties(kafkaParams)
    dstream.writeToKafka(p,
      s => new ProducerRecord[String, String](kafkaTopics(0), s.toString()))
  }*/
  def write[K, V](dstream : DStream[String]): Unit = {

  }
}
