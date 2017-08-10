package io.github.shen.common

import com.typesafe.config.Config
import org.apache.spark.streaming.{Duration, Seconds}
import net.ceedubs.ficus.Ficus._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by shen on 8/10/17.
  */

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
