package io.github.shen.common

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
/**
  * Created by shen on 8/10/17.
  */

class BeamConfig(config: Config) extends Serializable {
  config.resolve()
  val topics: Array[String] = config.as[Array[String]]("config.topics")
  val params: Map[String, String] = config.as[Map[String, String]]("config.params")
}
