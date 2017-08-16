package io.github.shen.common

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
/**
  * Created by shen on 8/10/17.
  */

class BeamConfig(config: Config) extends Serializable {
  config.resolve()
  val topics: Option[Array[String]] = config.as[Option[Array[String]]]("config.topics")
  val params: Map[String, String] = config.as[Map[String, String]]("config.params")
  val clsname: String = config.as[String]("class")
}
