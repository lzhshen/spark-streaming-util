package io.github.shen.common

import org.apache.spark.streaming.Seconds
/**
  * Created by shen on 8/5/17.
  */

class StreamingJobConfigSpecs extends UnitSpec {
  val path = getClass.getResource("/streamingJobFull.conf").getPath
  val jobConfig = new StreamingJobConfig(path)
  "valid application configuration" should "have correct application name" in {
      val appName = jobConfig.appConfig.name
      assert(appName == "KafkaLogCountExample")
  }
  it should "have application parameters" in {
    val appParams = jobConfig.appConfig.params
    assert(appParams == Map[String, String]("param_key_01" -> "param_value_01"))
  }

  "valid streaming configuration" should "have batch duration" in {
    assert(jobConfig.streamingConfig.batchDuration == Seconds(10))
  }
  it should "have correct window duration" in {
    assert(jobConfig.streamingConfig.windowDuration == Seconds(20))
  }
  it should "have correct slide duration" in {
    assert(jobConfig.streamingConfig.slideDuration == Seconds(5))
  }
  it should "have checkpoint directory" in {
    assert(jobConfig.streamingConfig.checkpointDir == "/tmp/checkpoint/")
  }
  it should "have shutdown marker file " in {
    assert(jobConfig.streamingConfig.shutdownMarker == "/tmp/stopMarker")
  }

  "valid spark configuration" should "have correct parameters" in {
    assert(jobConfig.sparkConfig.params == Map[String, String](
      "spark.master" -> "local[*]",
      "spark.app.name" -> "KafkaLogCountExample",
      "spark.streaming.stopGracefullyOnShutdown" -> "true",
      "spark.streaming.receiver.writeAheadLog.enable" -> "false",
      "spark.streaming.driver.writeAheadLog.allowBatching" -> "false"))
  }

  "valid input beam" should "have correct topics" in {
    assert(jobConfig.inputBeamConfigs(0).topics(0) == "input1")
    assert(jobConfig.inputBeamConfigs(0).topics(1) == "input2")
  }
  it should "have correct parameters" in {
    assert(jobConfig.inputBeamConfigs(0).params == Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "example",
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "latest"
    ))
  }

  "valid output beam" should "have correct topics" in {
    assert(jobConfig.outputBeamConfigs(0).topics(0) == "output")
  }
  it should "have correct parameters" in {
    assert(jobConfig.outputBeamConfigs(0).params == Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "acks" -> "all"
    ))
  }

  val path2 = getClass.getResource("/streamingJobPartial.conf").getPath
  val jobConfig2 = new StreamingJobConfig(path2)
  "streaming configuration" should "have default batch duration" in {
    assert(jobConfig2.streamingConfig.batchDuration == Seconds(5))
  }
  it should "have default window duration" in {
    assert(jobConfig2.streamingConfig.windowDuration == Seconds(20))
  }
  it should "have default slide duration" in {
    assert(jobConfig2.streamingConfig.slideDuration == Seconds(10))
  }
  it should "have default checkpoint directory" in {
    assert(jobConfig2.streamingConfig.checkpointDir == "/tmp/checkpoint")
  }
  it should "have default shutdown marker file " in {
    assert(jobConfig2.streamingConfig.shutdownMarker == "/tmp/shutdownMarker")
  }
}
