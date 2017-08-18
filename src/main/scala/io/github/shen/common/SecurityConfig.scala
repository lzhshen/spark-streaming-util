package io.github.shen.common

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
/**
  * Created by shen on 8/18/17.
  */
/*
class SecurityConfig(config: Config) {
  config.resolve()
  val userPrinciple = config.as[String]("userPrinciple")
  val userKeytabPath = config.as[String]("userKeytabPath")
  val krb5ConfPath = config.as[String]("krb5ConfPath")
}*/

case class SecurityConfig(userPrinciple: String, userKeytabPath: String, krb5ConfPath: String)

object SecurityConfig {
  def apply(config: Config) = {
    config.resolve()
    val userPrinciple = config.as[String]("userPrinciple")
    val userKeytabPath = config.as[String]("userKeytabPath")
    val krb5ConfPath = config.as[String]("krb5ConfPath")
    new SecurityConfig(userPrinciple, userKeytabPath, krb5ConfPath)
  }
}
