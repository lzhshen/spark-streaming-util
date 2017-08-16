package io.github.shen.example.mgmtview

/**
  * Created by shen on 8/14/17.
  */
case class MgmtviewLogRec(timestamp: String,
                          loglevel: String,
                          reqTimeStamp: String,
                          uuid: String,
                          traceId: String,
                          txCodeDetails: String,
                          compId: String,
                          insId: String,
                          userId: String,
                          loginName: String,
                          clientInfo: String,
                          txCostTime: String,
                          dataFrom: String,
                          rowCount: String,
                          errCode: String,
                          errMsg: String)

object MgmtviewLogRec {
  def apply(line: String) = {
    val m = line.split("\\|", -1)
    val rec = new MgmtviewLogRec(m(0), m(1),m(2),m(3),m(4),m(5), m(6),m(7),m(8),m(9),m(10), m(11),m(12),m(13),m(14),m(15))
    rec
  }
}
