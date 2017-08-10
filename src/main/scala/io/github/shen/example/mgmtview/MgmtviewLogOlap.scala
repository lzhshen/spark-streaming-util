package io.github.shen.example.mgmtview

import com.github.benfradet.spark.kafka010.writer._
import io.github.shen.output.KafkaOutputBeam
import io.github.shen.streaming.StreamingJob
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.storage.StorageLevel

/**
  * Created by shen on 8/10/17.
  */
object MgmtviewLogOlap {
  def main(args: Array[String]): Unit = {
    // Initialize streaming job
    val path = if (args.length == 0) {
      getClass.getResource("/mgmtviewLogOlap-dev.conf").getPath
    } else {
      args(0)
    }
    val streamingJob = new StreamingJob(path)

    // read orgnization map file and broadcast it
    val orgFile = streamingJob.jobConfig.appConfig.params.getOrElse("orgMapFile", "/tmp/orgMapFile.csv")
    val orgMap = streamingJob.sc.textFile(orgFile).collect().drop(1).map(OrgLogParser.parse).map(o => (o.InsID, o)).toMap
    val orgMapBC = streamingJob.sc.broadcast(orgMap)

    // read data from kafka topcis
    val lines = streamingJob.inputBeams(0).read().map(_.value())
    val docs = ServiceLogParser.parse(streamingJob.ssc, lines, orgMapBC)
    docs.persist(StorageLevel.MEMORY_ONLY_SER)

    // write data back to kafka topics
    val kafkaOutputBeam = streamingJob.outputBeams(0).asInstanceOf[KafkaOutputBeam]
    docs.writeToKafka(
      kafkaOutputBeam.producerConfig,
      s => new ProducerRecord[String, String](kafkaOutputBeam.config.topics(0), s.toString())
    )

    // start streaming job
    streamingJob.startAndAwaitTermination()
  }
}
