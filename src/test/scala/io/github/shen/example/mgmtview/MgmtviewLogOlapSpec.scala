package io.github.shen.example.mgmtview

/**
  * Created by shen on 8/10/17.
  */
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.mkuthan.spark.SparkStreamingSpec

object MgmtviewLogOlapSpec {
  val insID = "230291138"
  val orgString = """230291138,哈尔滨先锋路分理处客户部,4,230291138,哈尔滨先锋路分理处客户部,
                    |230291138,哈尔滨先锋路分理处客户部,230291138,哈尔滨先锋路分理处客户部,
                    |230291138,哈尔滨先锋路分理处客户部,230100397,哈尔滨先锋路分理处,
                    |230000000,建行黑龙江省分行,111111111,中国建设银行,2015/9/26,2999/12/31,
                    |,,0001/1/1,2015/9/26,dim_new_gen_ccb_inst_hier_c_a0300.pl,,0,2999/12/31""".stripMargin.replaceAll("\n", "")

  val inputDoc: String = """{"@timestamp":"2017-06-27T07:07:14.320Z",
    "beat":{"hostname":"W112PCO3UM12","name":"W112PCO3UM12", "version":"5.4.2"},
    "input_type":"log",
    "message":"2017-06-27 14:14:04,557|INFO |1498544044408|516067101|4600954220170627141138|AP121A001@TEST001002|AP121_test|230291138|1|||149|saiku|2|||",
    "offset":138,
    "source":"/home/ap/p12/shen/data/0.1og",
    "type":"log"
  }"""

  val msgString = "2017-06-27 14:14:04,557|INFO |1498544044408|516067101|4600954220170627141138|AP121A001@TEST001002|AP121_test|230291138|1|||149|saiku|2|||"

  val outputMap: Map[String, String] = Map("insId"->"230291138", "timestamp"->"2017-06-27 14:14:04",
    "rowCount"->"2", "uuid"->"516067101", "errMsg"->"", "clientInfo"->"",
    "txCodeDetails"->"AP121A001@TEST001002", "errCode"->"", "txCostTime"->"149",
    "reqTimeStamp"->"1498544044408", "compId"->"AP121_test", "dataFrom"->"saiku",
    "userId"->"1", "loginName"->"", "traceId"->"4600954220170627141138",
    "loglevel"->"INFO ",
    "InsID"->"230291138", "Inst_Chn_ShrtNm"->"哈尔滨先锋路分理处客户部", "Inst_Hier_Code"->"4",
    "Blng_Lvl7_Inst_ID"->"230291138", "Blng_Lvl7_Inst_Nm"->"哈尔滨先锋路分理处客户部",
    "Blng_Lvl6_Inst_ID"->"230291138", "Blng_Lvl6_Inst_Nm"->"哈尔滨先锋路分理处客户部",
    "Blng_Lvl5_Inst_ID"->"230291138", "Blng_Lvl5_Inst_Nm"->"哈尔滨先锋路分理处客户部",
    "Blng_Lvl4_Inst_ID"->"230291138", "Blng_Lvl4_Inst_Nm"->"哈尔滨先锋路分理处客户部",
    "Blng_Lvl3_Inst_ID"->"230100397", "Blng_Lvl3_Inst_Nm"->"哈尔滨先锋路分理处",
    "Blng_Lvl2_InsID"->"230000000", "Blng_Lvl2_Inst_Nm"->"建行黑龙江省分行",
    "Blng_Lv11_InsID"->"111111111", "Blng_Lvl1_Inst_Nm"->"中国建设银行")

  val orgInfoRec2 = OrgInfoRec(
    "230291138",
    "哈尔滨先锋路分理处客户部",
    "4",
    "230291138",
    "哈尔滨先锋路分理处客户部",
    "230291138",
    "哈尔滨先锋路分理处客户部",
    "230291138",
    "哈尔滨先锋路分理处客户部",
    "230291138",
    "哈尔滨先锋路分理处客户部",
    "230100397",
    "哈尔滨先锋路分理处",
    "230000000",
    "建行黑龙江省分行",
    "111111111",
    "中国建设银行",
    "2015/9/26",
    "2999/12/31")

  val mgmtviewLogRec2 = MgmtviewLogRec(
    "2017-06-27 14:14:04,557",
    "INFO ",
    "1498544044408",
    "516067101",
    "4600954220170627141138",
    "AP121A001@TEST001002",
    "AP121_test",
    "230291138",
    "1",
    "",
    "",
    "149",
    "saiku",
    "2",
    "",
    "")
}

class MgmtviewLogOlapSpec extends FlatSpec with GivenWhenThen with Matchers with Eventually with SparkStreamingSpec {

  import MgmtviewLogOlapSpec._
  import MgmtviewLogOlap._
  val orgInfoRec = OrgInfoRec(orgString)
  val mgmtviewLogRec = MgmtviewLogRec(msgString)
  val mgmtviewLogRecWithOrg = MgmtviewLogRecWithOrg(mgmtviewLogRec, orgInfoRec)

  "Valid Orgnization String" should "be parsed correctly" in {
    orgInfoRec shouldBe orgInfoRec2
  }

  "'messsage' field in filebeat log" should "extracted properly"  in {
    val lines = Seq(inputDoc)
    val msgLines = extractMessageFromFilebeatLog(sc.parallelize(lines)).collect()
    msgLines shouldBe Array(msgString)
  }

  "Valid MgmtviewLogRec string" should "be parsed properly" in {
    mgmtviewLogRec shouldBe mgmtviewLogRec2
  }

  "MgmtviewLogRec " should "join OrgInfoRec properly" in {
    val orgMap = Map(insID -> orgInfoRec)
    val orgMapBC = sc.broadcast(orgMap)
    val mgmtviewLogRecRDD = sc.parallelize(Seq(mgmtviewLogRec))
    val retArray = joinWithOrgMap(orgMapBC)(mgmtviewLogRecRDD).collect()
    retArray shouldBe Array(mgmtviewLogRecWithOrg)
  }
}