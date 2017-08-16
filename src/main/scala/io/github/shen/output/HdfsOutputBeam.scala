package io.github.shen.output

import io.github.shen.common.BeamConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by shen on 8/13/17.
  */
class HdfsOutputBeam(beamConfig: BeamConfig, sparkContext: SparkContext) extends OutputBeam {
  val config: BeamConfig = beamConfig
  val sc: SparkContext = sparkContext
  def write(anyRef: AnyRef): Unit = {

  }
  def write(rdd: RDD[String]): Unit = {

  }
}
