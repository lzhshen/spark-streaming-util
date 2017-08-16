package io.github.shen.input

import io.github.shen.common.BeamConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by shen on 8/13/17.
  */
class HdfsInputBeam(beamConfig: BeamConfig, sparkContext: SparkContext) extends InputBeam {
  private val sc: SparkContext = sparkContext
  def read() : RDD[String] = {
    val format = beamConfig.params.getOrElse("format", "text")
    val path = beamConfig.params.getOrElse("path", "")
    val rdd = format match {
      case "text" => this.sc.textFile(path)
    }
    rdd
  }
}
