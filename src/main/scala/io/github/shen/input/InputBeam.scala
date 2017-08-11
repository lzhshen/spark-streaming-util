package io.github.shen.input

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
/**
  * Created by shen on 8/3/17.
  */
trait InputBeam {
  def read(): DStream[ConsumerRecord[String, String]]
}