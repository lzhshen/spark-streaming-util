package io.github.shen.input

/**
  * Created by shen on 8/3/17.
  */
import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream


class KafkaInputBeam(config: Config, ssc: StreamingContext) extends InputBeam {
  private val kafkaParams = config.as[Map[String, String]]("config.params")
  private val kafkaTopics = config.as[Set[String]]("config.topics")
  private val preferredHosts = LocationStrategies.PreferConsistent
  private val consumerStrategy = ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaParams)
  private val beam : DStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](ssc, preferredHosts, consumerStrategy)

  def read() : DStream[ConsumerRecord[String, String]] = { this.beam }
}
