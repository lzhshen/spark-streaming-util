package io.github.shen.common

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
/**
  * Created by shen on 8/10/17.
  */

class SparkConfig(config: Config) {
  config.resolve()
  val params = config.as[Option[Map[String, String]]]("params").getOrElse(Map[String, String]())
}

