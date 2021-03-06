package io.github.shen.streaming

import io.github.shen.common.{BeamConfig, StreamingJobConfig}
import io.github.shen.input._
import io.github.shen.output._
import io.github.shen.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * Created by shen on 8/3/17.
  */
class StreamingJob(beamFileLocation: String) {
  val jobConfig = new StreamingJobConfig(beamFileLocation)

  val (sc, ssc) = this.setStreamingContext()

  val inputBeams: Array[InputBeam] =
    for (beamConfig: BeamConfig <- jobConfig.inputBeamConfigs) yield {
      /*
      val kafkaInputBeam = new KafkaInputBeam(beamConfig, this.ssc)
      kafkaInputBeam
       */
      val inputBeam =  Utils.newObjectFromClassName(beamConfig.clsname, beamConfig, this.ssc)
      inputBeam.asInstanceOf[InputBeam]
    }


  val outputBeams: Array[OutputBeam] = {
    for (beamConfig: BeamConfig <- jobConfig.outputBeamConfigs) yield {
      /*
      val kafkaOutputBeam = new KafkaOutputBeam(beamConfig, this.ssc)
      kafkaOutputBeam
      */
      val outputBeam =  Utils.newObjectFromClassName(beamConfig.clsname, beamConfig, this.ssc)
      outputBeam.asInstanceOf[OutputBeam]
    }
  }

  def startAndAwaitTermination () = {
    ssc.start()
    waitShutdownCommand(ssc, jobConfig.streamingConfig.shutdownMarker)
  }

  private def setStreamingContext() = {
    val appName = jobConfig.appConfig.name
    val conf = new SparkConf().setAppName(appName);
    jobConfig.sparkConfig.params.foreach { case (k, v) => conf.setIfMissing(k, v) }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, jobConfig.streamingConfig.batchDuration)
    ssc.checkpoint(jobConfig.streamingConfig.checkpointDir)
    (sc, ssc)
  }
/*
  private def setSparkSession(): SparkSession = {

    val appName = jobConfig.appConfig.name
    val conf = new SparkConf().setAppName(appName);
    jobConfig.sparkConfig.params.foreach { case (k, v) => conf.setIfMissing(k, v) }

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

  }
*/
  private def waitShutdownCommand(ssc: StreamingContext, shutdownMarker: String): Unit = {
    var stopFlag: Boolean = false
    val checkIntervalMillis: Long = 10000
    var isStopped = false
    val fs = FileSystem.get(new Configuration())
    while (!isStopped) {
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped) {
        LogHolder.log.info("ssc stopped")
      }

      if (!stopFlag) {
        stopFlag = fs.exists(new Path(shutdownMarker))
      } else {
        LogHolder.log.info("waiting ssc to stop")
      }

      if (!isStopped && stopFlag) {
        LogHolder.log.info("stopping ssc")
        ssc.stop(true, true)
        LogHolder.log.info("ssc stop called")
      }
    }
  }
}
