package com.blazedb.spark.reports

import com.blazedb.spark.reports.ReportsDataPrep.{LLine, MetaInfo, ColInfo, formatTstamp}

class MemCharts extends ChartingApp {

  // /Users/steboesch/sparkperf/0922-044958/Mem/20rowlen/1000000recs/32parts/4Loops/cached/GroupByKey/Collect/Loop3.log
  val memMetaInfo = MetaInfo(
    "<08:18:07> Completed 0922-081645 Mem 1000rowlen 1000000recs 32parts 4Loops cached GroupByKey Collect Loop0 - duration=19.766 seconds count=724860",
    ".*Completed $COLUMN_GROUPS.*",
    Seq(ColInfo("tstamp", "Tstamp", classOf[String], """(?<$NAME>[\d]{4}-[\d]{6})""", formatTstamp),
      ColInfo("tname", "TestName"),
      ColInfo("rowlen", "RowLen", classOf[Int], """(?<$NAME>[\d]+)rowlen"""),
      ColInfo("inrecs", "InRecords", classOf[Int], """(?<$NAME>[\d]+)recs""", (x) => (x.toInt / 1000).toString),
      ColInfo("nparts", "Partitions", classOf[Int], """(?<$NAME>[\d]+)parts"""),
      ColInfo("loops", "Loops", classOf[Int], """(?<$NAME>[\d]+)Loops""", (x) => (x.toInt / 1000).toString),
      ColInfo("cached", "Cached"),
      ColInfo("xform", "Xform"),
      ColInfo("action", "Action"),
      ColInfo("loopnum", "LoopNum", classOf[Int], """Loop(?<$NAME>[\d]+)"""),
      ColInfo("duration", "Duration", classOf[Double], """- duration=(?<$NAME>[\d]+\.[\d]+) seconds""", (x) => x.toString),
      ColInfo("outrecs", "OutRecords", classOf[Int], """count=(?<$NAME>[\d]+)""", (x) => (x.toInt / 1000).toString)
    ),
    "$tname $xform on $tstamp Parts=$nparts",
      "$tname $tstamp $nparts $xform $action $inrecs-recs loop$loopnum",
//    (line: LLine) => {
//      val pat = "$tname $tstamp $nparts $xform $action recs=$inrecs_thousands $isCached"
//      val inrecsK = (line.valsMap("inrecs").cval.asInstanceOf[Int] / 1000).toInt
//      val isCached = line.valsMap("loopnum").cval.asInstanceOf[Int] > 0
//      val m = line.valsMap.map { case (k, v) => (k, v.cval.toString) }
//      val m2 = m ++ Map("inrecs_thousands" -> inrecsK, "iscached" -> isCached)
//      val out = LLine.applyVals(pat, m2)
//      out
//    },
      "rowlen", "duration"
  )

  override def getMeta() = memMetaInfo

}

/*
  val coreMetaInfo = MetaInfo(
    "sample line",
    ".*Completed $COLUMN_GROUPS",
    Seq(ColInfo("tstamp", "Tstamp", classOf[String], """[\d]{4}-[\d]{6})""", formatTstamp),
    ColInfo("tname", "TestName"),
    ColInfo("inrecs", "InRecords", classOf[Int], """[\d]+)recs""", (x) => (x.toInt / 1000).toString),
    ColInfo("nparts", "Partitions", classOf[Int], """[\d]+)parts"""),
    ColInfo("nskew", "Skew", classOf[Int], """[\d]+)skew"""),
    ColInfo("native", "Ignite"),
    ColInfo("xform", "Xform"),
    ColInfo("action", "Action"),
    ColInfo("duration", "Duration", classOf[Int], """[\d]+)""", (x) => (x.toInt / 1000).toString),
    ColInfo("outrecs", "OutRecords", classOf[Int], """[\d]+)recs""", (x) => (x.toInt / 1000).toString)
  ),
    "$tname $xform on $formatT($tstamp) Partitions=$nparts Skew=$nskew",
    "$tname $tstamp $nparts $nskew $xform $action $native",
      "inrecs", "duration"
  )

  val sqlMetaInfo = MetaInfo(
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
      "inrecs", "duration"
  )

 */