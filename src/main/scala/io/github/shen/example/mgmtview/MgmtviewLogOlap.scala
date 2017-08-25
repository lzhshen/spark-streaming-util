package io.github.shen.example.mgmtview

import com.github.benfradet.spark.kafka010.writer._
import io.github.shen.input.{HdfsInputBeam, KafkaInputBeam}
import io.github.shen.output.KafkaOutputBeam
import io.github.shen.streaming.StreamingJob
import io.github.shen.utils._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.parsing.json.JSON

/**
  * Created by shen on 8/10/17.
  */
object MgmtviewLogOlap {
  def main(args: Array[String]): Unit = {
    val path = if (args.length == 0) {
      //getClass.getResource("/mgmtviewLogOlap2-dev.conf").getPath
      getClass.getResource("/mgmtviewLogOlap2-security.conf").getPath
    } else {
      args(0)
    }

    // Initialize streaming job
    val streamingJob = new StreamingJob(path)

    // read orgnization map file and broadcast it
    val hdfsInputBeam = streamingJob.inputBeams(1).asInstanceOf[HdfsInputBeam]
    val orgMap = hdfsInputBeam.read().collect().drop(1)
      .map(OrgInfoRec(_))
      .filter(_.nonEmpty)
      .map(o => (o.get.InsID, o))
      .toMap
    val orgMapBC = streamingJob.sc.broadcast(orgMap)

    // read data from kafka topcis and apply transformations
    val kafkaInputBeam = streamingJob.inputBeams(0).asInstanceOf[KafkaInputBeam]
    val lines = kafkaInputBeam.read().map(_.value())
    val docs = lines
      .transform(extractMessageFromFilebeatLog)
      .transform(toMgmtviewLogRec)
      .transform(joinWithOrgMap(orgMapBC))
      .transform(toJsonString)

    docs.persist(StorageLevel.MEMORY_ONLY_SER)

    // write data back to kafka topics
    val kafkaOutputBeam = streamingJob.outputBeams(0).asInstanceOf[KafkaOutputBeam]
    docs.writeToKafka(
      kafkaOutputBeam.producerConfig,
      s => new ProducerRecord[String, String](kafkaOutputBeam.config.topics.get(0), s.toString())
    )

    // start streaming job
    streamingJob.startAndAwaitTermination()
  }

  val extractMessageFromFilebeatLog = (jsonLines: RDD[String]) => {
    jsonLines.map(line => {
      val result = JSON.parseFull(line)
      val msg = result match {
        case Some(map) => {
          val m = map.asInstanceOf[Map[String, String]]
          m.getOrElse("message", "")
        }
        case _ => ""
      }
      // print malformed data to log
      if (msg == null || msg.length == 0) {
        LogHolder.log.info(s"Invalid json format! Data: $line")
      }
      msg
    }).filter(s => s != null && s.length > 0)
  }

  val toMgmtviewLogRec = (lines : RDD[String]) => {
    lines.map(MgmtviewLogRec(_)).filter(_.nonEmpty)
  }

  val joinWithOrgMap =
    (orgMap: Broadcast[Map[String, Option[OrgInfoRec]]]) => (mgmtviewLogRecs: RDD[Option[MgmtviewLogRec]]) =>
    {
      mgmtviewLogRecs.map(rec => {
        MgmtviewLogRecWithOrg(rec.get, orgMap.value.getOrElse(rec.get.insId, OrgInfoRec()).get)
      })
    }

  val toJsonString = (mgmtviewLogRecs: RDD[MgmtviewLogRecWithOrg]) => {
    mgmtviewLogRecs.map(rec => {
      val m: Map[String, Any] = Utils.ccToMap(rec.mgmtLog) ++ Utils.ccToMap(rec.org)
      scala.util.parsing.json.JSONObject(m).toString()
    })
  }
}
