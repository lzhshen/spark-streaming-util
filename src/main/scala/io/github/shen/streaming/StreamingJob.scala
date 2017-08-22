package io.github.shen.streaming

import io.github.shen.common.{BeamConfig, StreamingJobConfig}
import io.github.shen.input._
import io.github.shen.output._
import io.github.shen.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import com.huawei.hadoop.security._

/**
  * Created by shen on 8/3/17.
  */
class StreamingJob(beamFileLocation: String) {
  val jobConfig = new StreamingJobConfig(beamFileLocation)

  this.login()
  val (sc, ssc) = this.setStreamingContext()

  val inputBeams: Array[InputBeam] =
    for (beamConfig: BeamConfig <- jobConfig.inputBeamConfigs) yield {
      val clsname = beamConfig.clsname
      val inputBeam = clsname match {
        case "io.github.shen.input.KafkaInputBeam" =>
          Utils.newObjectFromClassName(clsname, beamConfig, this.ssc).asInstanceOf[KafkaInputBeam]
        case "io.github.shen.input.HdfsInputBeam" =>
          Utils.newObjectFromClassName(clsname, beamConfig, this.sc).asInstanceOf[HdfsInputBeam]
      }
      inputBeam
    }

  val outputBeams: Array[OutputBeam] = {
    for (beamConfig: BeamConfig <- jobConfig.outputBeamConfigs) yield {
      /*
      val outputBeam =  Utils.newObjectFromClassName(beamConfig.clsname, beamConfig, this.ssc)
      outputBeam.asInstanceOf[OutputBeam]
      */
      val clsname = beamConfig.clsname
      val outputBeam = clsname match {
        case "io.github.shen.output.KafkaOutputBeam" =>
          Utils.newObjectFromClassName(clsname, beamConfig, this.ssc).asInstanceOf[KafkaOutputBeam]
        case "io.github.shen.output.HdfsOutputBeam" =>
          Utils.newObjectFromClassName(clsname, beamConfig, this.sc).asInstanceOf[HdfsOutputBeam]
      }
      outputBeam
    }
  }

  def startAndAwaitTermination () = {
    ssc.start()
    waitShutdownCommand(ssc, jobConfig.streamingConfig.shutdownMarker)
  }

  private def login() = {
    jobConfig.securityConfig match {
      case Some(config) => {
        val hadoopConf: Configuration = new Configuration()
        LogHolder.log.info(s"Login Information, userPrincipal: ${config.userPrinciple}; userKeytabPath: ${config.userKeytabPath}, krb5ConfPath: ${config.krb5ConfPath}")
        LoginUtil.login(config.userPrinciple, config.userKeytabPath, config.krb5ConfPath, hadoopConf)
      }
      case None => {
        LogHolder.log.info("Ignore login as there is no 'security' info in configuration file!")
      }
    }
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
