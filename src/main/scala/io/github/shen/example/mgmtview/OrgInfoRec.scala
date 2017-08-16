package io.github.shen.example.mgmtview

/**
  * Created by shen on 8/10/17.
  */

case class OrgInfoRec (InsID: String,
                       Inst_Chn_ShrtNm: String,
                       Inst_Hier_Code: String,
                       Blng_Lvl7_Inst_ID: String,
                       Blng_Lvl7_Inst_Nm: String,
                       Blng_Lvl6_Inst_ID: String,
                       Blng_Lvl6_Inst_Nm: String,
                       Blng_Lvl5_Inst_ID: String,
                       Blng_Lvl5_Inst_Nm: String,
                       Blng_Lvl4_Inst_ID: String,
                       Blng_Lvl4_Inst_Nm: String,
                       Blng_Lvl3_Inst_ID: String,
                       Blng_Lvl3_Inst_Nm: String,
                       Blng_Lvl2_InsID: String,
                       Blng_Lvl2_Inst_Nm: String,
                       Blng_Lv11_InsID: String,
                       Blng_Lvl1_Inst_Nm: String,
                       StDt: String,
                       EdDt: String)

object OrgInfoRec {
  def apply(line: String) = {
    val m = line.split("\\,", -1)
    new OrgInfoRec(m(0), m(1),m(2),m(3),m(4),m(5), m(6),m(7),m(8),m(9),m(10), m(11),m(12),m(13),m(14),m(15),m(16),m(17),m(18))
  }

  def apply(): OrgInfoRec = new OrgInfoRec("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
}