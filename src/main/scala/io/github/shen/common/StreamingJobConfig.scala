package io.github.shen.common

import com.typesafe.config.Config
import io.github.shen.utils.Utils
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.{Duration, Seconds}
import scala.concurrent.duration.FiniteDuration
/**
  * Created by shen on 8/7/17.
  */

class StreamingJobConfig(configFile: String) {
  val config = Utils.loadConf(configFile).getConfig("beam")
  val appConfigMap = config.as[Map[String, String]]("app")
  val sparkConfigMap = config.as[Map[String, String]]("spark")
  private val streamingConfig = config.getConfig("streaming")
  val inputConfigs = config.as[Array[Config]]("input")
  val outputConfigs = config.as[Array[Config]]("output")
  val batchDuration: Duration = Seconds(streamingConfig.as[FiniteDuration]("batchDuration").toSeconds)
  val windowDuration: Duration = Seconds(streamingConfig.as[FiniteDuration]("windowDuration").toSeconds)
  val slideDuration: Duration = Seconds(streamingConfig.as[FiniteDuration]("slideDuration").toSeconds)
  val checkpointDir: String = streamingConfig.getString("checkpointDir")
  val shutdownMarker: String = streamingConfig.getString("shutdownMarker")
  val appName: String = appConfigMap("name")
}

object KafkaConfig {
  def getKafkaParamsMap(cfg: Config): Map[String, String] = {
    cfg.as[Map[String, String]]("config.params")
  }

  def getKafkaTopicsArray(cfg: Config) = {
    cfg.as[Array[String]]("config.topics")
  }
}

