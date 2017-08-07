package io.github.shen.streaming

import com.typesafe.config.Config
import io.github.shen.common.StreamingJobConfig
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

  val inputBeams: Array[InputBeam] = {
    for (config <- jobConfig.inputConfigs) yield {
      val kafkaInputBeam = new KafkaInputBeam(config, this.ssc)
      kafkaInputBeam
    }
  }

  val outputBeams: Array[OutputBeam] = {
    for (config: Config <- jobConfig.outputConfigs) yield {
      // TODO:
      //val oclass = Class.forName(config.as[String]("outputClass")).getClass
      val kafkaOutputBeam = new KafkaOutputBeam(config, this.ssc)
      kafkaOutputBeam
    }
  }

  def startAndAwaitTermination () = {
    ssc.start()
    waitShutdownCommand(ssc, jobConfig.shutdownMarker)
  }

  private def setStreamingContext() = {
    val appName = jobConfig.appName
    val conf = new SparkConf().setAppName(appName);
    jobConfig.sparkConfigMap.foreach { case (k, v) => conf.setIfMissing(k, v) }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, jobConfig.batchDuration)
    ssc.checkpoint(jobConfig.checkpointDir)
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
