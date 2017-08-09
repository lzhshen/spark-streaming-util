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
  private val rootConfig : Config = Utils.loadConf(configFile)
  val appConfig = new AppConfig(rootConfig.getConfig("streamingJob.app"))
  val sparkConfig = new SparkConfig(rootConfig.getConfig("streamingJob.spark"))
  val streamingConfig: StreamingConfig = new StreamingConfig(rootConfig.getConfig("streamingJob.streaming"))
  val inputBeamConfigs: Array[BeamConfig] =
    for (config <- rootConfig.as[Array[Config]]("streamingJob.input")) yield {
      val beamConfig = new BeamConfig(config)
      beamConfig
  }
  val outputBeamConfigs: Array[BeamConfig] =
    for (config <- rootConfig.as[Array[Config]]("streamingJob.output")) yield {
      val beamConfig = new BeamConfig(config)
      beamConfig
    }
}

class AppConfig(config: Config) {
  config.resolve()
  val name = config.as[Option[String]]("name").getOrElse("defaultStreamingJobName")
  val params = config.as[Option[Map[String, String]]]("params").getOrElse(Map[String, String]())
}

class SparkConfig(config: Config) {
  config.resolve()
  val params = config.as[Option[Map[String, String]]]("params").getOrElse(Map[String, String]())
}

class BeamConfig(config: Config) {
  config.resolve()
  val topics: Array[String] = config.as[Array[String]]("config.topics")
  val params: Map[String, String] = config.as[Map[String, String]]("config.params")
}

class StreamingConfig(config: Config) {
    config.resolve()
    val batchDuration: Duration =
      Seconds(config.as[Option[FiniteDuration]]("batchDuration").getOrElse(FiniteDuration(5,"s")).toSeconds)
    val windowDuration: Duration =
      Seconds(config.as[Option[FiniteDuration]]("windowDuration").getOrElse(FiniteDuration(20,"s")).toSeconds)
    val slideDuration: Duration =
      Seconds(config.as[Option[FiniteDuration]]("slideDuration").getOrElse(FiniteDuration(10,"s")).toSeconds)
    val checkpointDir: String =
      config.as[Option[String]]("checkpointDir").getOrElse("/tmp/checkpoint")
    val shutdownMarker: String =
      config.as[Option[String]]("shutdownMarker").getOrElse("/tmp/shutdownMarker")
}
