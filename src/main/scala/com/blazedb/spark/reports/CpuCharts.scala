package com.blazedb.spark.reports

import com.blazedb.spark.reports.ReportsDataPrep.{ColInfo, MetaInfo, formatTstamp}

class CpuCharts extends ChartingApp {

  val cpuMetaInfo = MetaInfo(
    "sample line",
    ".*Completed $COLUMN_GROUPS",
    Seq(ColInfo("tstamp", "Tstamp", classOf[String], """[\d]{4}-[\d]{6})""", formatTstamp),
      ColInfo("tname", "TestName"),
      ColInfo("loops", "Loops", classOf[Int], """[\d]+)Loops""", (x) => (x.toInt / 1000).toString),
      ColInfo("cores", "Cores", classOf[Int], """[\d]+)cores""", (x) => (x.toInt / 1000).toString),
      ColInfo("mem", "MB", classOf[Int], """[\d]+)mb""", (x) => (x.toInt / 1000).toString),
      ColInfo("inrecs", "InRecords", classOf[Int], """[\d]+)recs""", (x) => (x.toInt / 1000).toString),
      ColInfo("nparts", "Partitions", classOf[Int], """[\d]+)parts"""),
      ColInfo("xform", "Xform"),
      ColInfo("action", "Action"),
      ColInfo("duration", "Duration", classOf[Double], """- duration=(?<duration>[\\d]+\\.[\\d]+) seconds""", (x) => x.toString),
      ColInfo("outrecs", "OutRecords", classOf[Int], """count=[\d]+).*""", (x) => (x.toInt / 1000).toString)
    ),
    "$tname $xform on $tstamp Parts=$nparts",
    "$tname $tstamp $nparts $xform $action",
    "loops", "duration"
  )

  override def getMeta() = cpuMetaInfo

}
