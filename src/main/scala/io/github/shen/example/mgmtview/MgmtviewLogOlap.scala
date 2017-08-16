package io.github.shen.example.mgmtview

import com.github.benfradet.spark.kafka010.writer._
import io.github.shen.input.{HdfsInputBeam, KafkaInputBeam}
import io.github.shen.output.KafkaOutputBeam
import io.github.shen.streaming.StreamingJob
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
      getClass.getResource("/mgmtviewLogOlap2-dev.conf").getPath
    } else {
      args(0)
    }

    // Initialize streaming job
    val streamingJob = new StreamingJob(path)

    // read orgnization map file and broadcast it
    val hdfsInputBeam = streamingJob.inputBeams(1).asInstanceOf[HdfsInputBeam]
    val orgMap = hdfsInputBeam.read().collect().drop(1).map(OrgInfoRec(_)).map(o => (o.InsID, o)).toMap
    val orgMapBC = streamingJob.sc.broadcast(orgMap)

    // read data from kafka topcis and apply transformations
    val kafkaInputBeam = streamingJob.inputBeams(0).asInstanceOf[KafkaInputBeam]
    val lines = kafkaInputBeam.read().map(_.value())
    val docs = lines
      .transform(extractMessageFromFilebeatLog)
      .map(MgmtviewLogRec(_))
      .transform(joinWithOrgMap(orgMapBC))
      .transform(convertToJsonString)

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
      msg
    })
  }

  val joinWithOrgMap =
    (orgMap: Broadcast[Map[String, OrgInfoRec]]) => (mgmtviewLogRecs: RDD[MgmtviewLogRec]) =>
    {
      mgmtviewLogRecs.map(rec => {
        MgmtviewLogRecWithOrg(rec, orgMap.value.getOrElse(rec.insId, OrgInfoRec()))
      })
    }

  val convertToJsonString = (mgmtviewLogRecs: RDD[MgmtviewLogRecWithOrg]) => {
    mgmtviewLogRecs.map(rec => {
      val m: Map[String, String] = Map("instandId" -> rec.mgmtLog.insId,
                                       "Inst_Chn_ShrtNm" -> rec.org.Inst_Chn_ShrtNm)
      scala.util.parsing.json.JSONObject(m).toString()
    })
  }
}
