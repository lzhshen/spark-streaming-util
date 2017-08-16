package io.github.shen.input

/**
  * Created by shen on 8/3/17.
  */
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import io.github.shen.common._

class KafkaInputBeam(beamConfig: BeamConfig, ssc: StreamingContext) extends InputBeam {
  private val preferredHosts = LocationStrategies.PreferConsistent
  private val consumerStrategy =
    ConsumerStrategies.Subscribe[String, String](beamConfig.topics.get, beamConfig.params)
  private val beam : DStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](ssc, preferredHosts, consumerStrategy)
  val config: BeamConfig = beamConfig

  def read() : DStream[ConsumerRecord[String, String]] = { this.beam }
}
