package io.github.shen.output

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import io.github.shen.utils._
import io.github.shen.common._

/**
  * Created by shen on 8/4/17.
  */
class KafkaOutputBeam(beamConfig: BeamConfig, ssc: StreamingContext) extends OutputBeam {
  val producerConfig = Utils.map2Properties(beamConfig.params)
  val config: BeamConfig = beamConfig

  //def write[K, V](dstream : DStream[T], transformFunc: T => ProducerRecord[K, V])
  /*
  def write[K, V](dstream : DStream[T]): Unit = {
    val p = Utils.map2Properties(kafkaParams)
    dstream.writeToKafka(p,
      s => new ProducerRecord[String, String](kafkaTopics.get(0), s.toString()))
  }*/
  def write(dstream : DStream[String]): Unit = {

  }

  def write(anyRef: AnyRef): Unit = {

  }
}
