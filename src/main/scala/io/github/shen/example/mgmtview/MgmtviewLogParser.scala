package io.github.shen.example.mgmtview

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by shen on 8/10/17.
  */
object MgmtviewLogParser {

  val fields: List[String] = List("timestamp", "loglevel", "reqTimeStamp",
    "uuid", "traceId", "txCodeDetails", "compId", "insId", "userId",
    "loginName", "clientInfo", "txCostTime", "dataFrom", "rowCount",
    "errCode", "errMsg")
  val defaultOrgRec = OrgInfoRec("0", "0", "0", "0", "0", "0",
    "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0") // TODO:
  var orgMap: Map[String, OrgInfoRec] = Map()

  def parse(lines: DStream[String],
            orgMapBC: Broadcast[Map[String, OrgInfoRec]]): DStream[String] = {
    orgMap = orgMapBC.value
    val docs = lines.transform(extractMessageField).transform(map2String)
    docs
  }

  // for UT only
  def setOrgMap(m: Map[String, OrgInfoRec]): Unit = {
    orgMap = m
  }

  val extractMessageField = (lines: RDD[String]) =>
    lines.map(line => {
      val rec = new FilebeatLogRecord(line, fields)
      val m = rec.parse("\\|")
      val orgInfo = orgMap.getOrElse(m("insId"), defaultOrgRec)
      /*
      val logWithOrg = mutable.Map.empty[String, String]
      logWithOrg ++= m.toList
      logWithOrg += ("id" -> orgInfo.id)*/
      // cut off milliseconds part of timestamp if "timestamp" field exist
      val n = collection.mutable.Map(m.toSeq: _*)
      if (n.contains("timestamp")) n("timestamp") = n("timestamp").split(",")(0)
      n += ("InsID" -> orgInfo.InsID)
      n += ("Inst_Chn_ShrtNm" -> orgInfo.Inst_Chn_ShrtNm)
      n += ("Inst_Hier_Code"-> orgInfo.Inst_Hier_Code)
      n += ("Blng_Lvl7_Inst_ID" -> orgInfo.Blng_Lvl7_Inst_ID)
      n += ("Blng_Lvl7_Inst_Nm" -> orgInfo.Blng_Lvl7_Inst_Nm)
      n += ("Blng_Lvl6_Inst_ID" -> orgInfo.Blng_Lvl6_Inst_ID)
      n += ("Blng_Lvl6_Inst_Nm" -> orgInfo.Blng_Lvl6_Inst_Nm)
      n += ("Blng_Lvl5_Inst_ID" -> orgInfo.Blng_Lvl5_Inst_ID)
      n += ("Blng_Lvl5_Inst_Nm" -> orgInfo.Blng_Lvl5_Inst_Nm)
      n += ("Blng_Lvl4_Inst_ID" -> orgInfo.Blng_Lvl4_Inst_ID)
      n += ("Blng_Lvl4_Inst_Nm" -> orgInfo.Blng_Lvl4_Inst_Nm)
      n += ("Blng_Lvl3_Inst_ID" -> orgInfo.Blng_Lvl3_Inst_ID)
      n += ("Blng_Lvl3_Inst_Nm" -> orgInfo.Blng_Lvl3_Inst_Nm)
      n += ("Blng_Lvl2_InsID" -> orgInfo.Blng_Lvl2_InsID)
      n += ("Blng_Lvl2_Inst_Nm" -> orgInfo.Blng_Lvl2_Inst_Nm)
      n += ("Blng_Lv11_InsID" -> orgInfo.Blng_Lv11_InsID)
      n += ("Blng_Lvl1_Inst_Nm" -> orgInfo.Blng_Lvl1_Inst_Nm)
      n.toMap
    })

  val map2String = (maps: RDD[Map[String, String]]) =>
    maps.map(m => {
      val doc = scala.util.parsing.json.JSONObject(m).toString()
      doc
    })

  /*
  def orgInfoToMap(orgInfo: OrgInfoRec): Map[String, Any] = {
    val fieldNames = orgInfo.getClass.getDeclaredFields.map(_.getName)
    val vals = OrgInfoRec.unapply(orgInfo).get.iterator.toSeq
    fieldNames.zip(vals).toMap
  }*/

}

