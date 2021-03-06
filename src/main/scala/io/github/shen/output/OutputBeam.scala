package io.github.shen.output

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by shen on 8/3/17.
  */
abstract class OutputBeam extends Serializable {
  def write[K, V](dstream : DStream[String]): Unit
  //def write[K, V](dstream : DStream[T], transformFunc: T => ProducerRecord[K, V]): Unit
}
