package io.github.shen.common

import org.apache.spark.streaming.Seconds

/**
  * Created by shen on 8/5/17.
  */

class StreamingJobConfigSpecs extends UnitSpec {
  val path = getClass.getResource("/exampleBeam.conf").getPath
  val jobConfig = new StreamingJobConfig(path)
  /*
    "application name" should "be exist" in {
      val appName = jobConfig.appName
      assert(appName == "KafkaLogCountExample")
    }

    "batch duration" should "be correct" in {
      assert(jobConfig.batchDuration == Seconds(10))
    }

    "windows duration" should "be correct" in {
      assert(jobConfig.windowDuration == Seconds(20))
    }

    "slide duration" should "be correct" in {
      assert(jobConfig.slideDuration == Seconds(5))
    }

    "checkpoint directory" should "be correct" in {
      assert(jobConfig.checkpointDir == "/tmp/checkpoint/")
    }

    "shutdown marker file" should "be correct" in {
      assert(jobConfig.shutdownMarker == "/tmp/stopMarker")
    }*/
}
