package io.github.shen.streaming

import io.github.shen.common.{BeamConfig, StreamingJobConfig}
import io.github.shen.input._
import io.github.shen.output._
import io.github.shen.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shen on 8/3/17.
  */
class StreamingJob(beamFileLocation: String) {
  val jobConfig = new StreamingJobConfig(beamFileLocation)

  val (sc, ssc) = this.setStreamingContext()

  val inputBeams: Array[InputBeam] =
    for (beamConfig: BeamConfig <- jobConfig.inputBeamConfigs) yield {
      val kafkaInputBeam = new KafkaInputBeam(beamConfig, this.ssc)
      kafkaInputBeam
    }


  val outputBeams: Array[OutputBeam] = {
    for (beamConfig: BeamConfig <- jobConfig.outputBeamConfigs) yield {
      // TODO:
      //val oclass = Class.forName(config.as[String]("outputClass")).getClass
      val kafkaOutputBeam = new KafkaOutputBeam(beamConfig, this.ssc)
      kafkaOutputBeam
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
