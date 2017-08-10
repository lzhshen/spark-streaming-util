package io.github.shen.example.mgmtview

/**
  * Created by shen on 8/10/17.
  */


object OrgLogParser {
  def parse(line: String): OrgInfoRec = {
    val m = line.split("\\,")
    val orgInfoRec = new OrgInfoRec(m(0), m(1),m(2),m(3),m(4),m(5),
      m(6),m(7),m(8),m(9),m(10),
      m(11),m(12),m(13),m(14),m(15),m(16),m(17),m(18))
    orgInfoRec
  }
}