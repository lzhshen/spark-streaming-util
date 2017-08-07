package io.github.shen.streaming

import com.typesafe.config.Config
import org.apache.spark.streaming.{Duration, Seconds}
import net.ceedubs.ficus.Ficus._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by shen on 8/5/17.
  */
class StreamConfig(config: Config) {
  config.resolve()
  val batchDuration: Duration = Seconds(config.as[FiniteDuration]("batchDuration").toSeconds)
  val windowDuration: Duration = Seconds(config.as[FiniteDuration]("windowDuration").toSeconds)
  val slideDuration: Duration = Seconds(config.as[FiniteDuration]("slideDuration").toSeconds)
  val checkpointDir: String = config.getString("checkpointDir")
  val shutdownMarker: String = config.getString("shutdownMarker")
}

