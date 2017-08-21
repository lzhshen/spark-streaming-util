package io.github.shen.common

import com.typesafe.config.Config
import io.github.shen.utils.Utils
import net.ceedubs.ficus.Ficus._
/**
  * Created by shen on 8/7/17.
  */

class StreamingJobConfig(configFile: String) {
  private val rootConfig : Config = Utils.loadConf(configFile)
  val appConfig = new AppConfig(rootConfig.getConfig("streamingJob.app"))
  val sparkConfig = new SparkConfig(rootConfig.getConfig("streamingJob.spark"))
  val streamingConfig: StreamingConfig = new StreamingConfig(rootConfig.getConfig("streamingJob.streaming"))
  // "security" configuration is optional
  val securityConfig: Option[SecurityConfig] = {
    rootConfig.as[Option[Config]]("streamingJob.security") match {
      case Some(cfg) => Option(SecurityConfig(cfg))
      case None => None
    }
  }
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

