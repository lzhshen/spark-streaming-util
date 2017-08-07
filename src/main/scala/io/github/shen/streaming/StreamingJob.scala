package io.github.shen.streaming

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import io.github.shen.input._
import io.github.shen.output._
import io.github.shen.utils._

/**
  * Created by shen on 8/3/17.
  */
class StreamingJob(beamFileLocation: String) {
  private val config = Utils.loadConf(beamFileLocation).getConfig("beam")
  private val appConfigMap = config.as[Map[String, String]]("app")
  private val sparkConfigMap = config.as[Map[String, String]]("spark")
  private val streamingConfig = config.getConfig("streaming")
  private val inputConfigs = config.as[Array[Config]]("input")
  private val outputConfigs = config.as[Array[Config]]("output")
  val streamConfig: StreamConfig = new StreamConfig(streamingConfig)

  val (sc, ssc) = this.setStreamingContext()

  val inputBeams: Array[InputBeam] = {
    for (config <- inputConfigs) yield {
      val kafkaInputBeam = new KafkaInputBeam(config, this.ssc)
      kafkaInputBeam
    }
  }

  val outputBeams: Array[OutputBeam] = {
    for (config: Config <- outputConfigs) yield {
      // TODO:
      //val oclass = Class.forName(config.as[String]("outputClass")).getClass
      val kafkaOutputBeam = new KafkaOutputBeam(config, this.ssc)
      kafkaOutputBeam
    }
  }

  def startAndAwaitTermination () = {
    ssc.start()
    waitShutdownCommand(ssc, streamConfig.shutdownMarker)
  }

  private def setStreamingContext() = {
    val appName = appConfigMap("name")
    val conf = new SparkConf().setAppName(appName);
    sparkConfigMap.foreach { case (k, v) => conf.setIfMissing(k, v) }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, streamConfig.batchDuration)
    ssc.checkpoint(streamConfig.checkpointDir)
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
