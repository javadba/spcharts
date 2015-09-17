/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blazedb.spark.reports

import java.io.File
import java.lang.{Double => JDouble}
import javafx.collections.FXCollections
import javafx.scene.chart.XYChart
import javafx.scene.chart.XYChart._

import scala.collection.SortedMap

/**
 * ReportsDataPrep
 *
 * Takes a single command line argument: the path to the Yardstick-Spark 
 * Logs directory - which will be under /tmp/yslogs/<timestamp>
 *
 * Generates K Charts where K is the number of distinct Testcases in the CoreBattery
 * Each Chart has four series: The TestCase x (IgniteRDD|NativeRdd, Count|CountByKey)
 *
 * In addition there is a "Summary" Page containing miniaturized copies of ALL the other 
 * charts.
 *
 * There are two significant components to this program: 
 *   Data processing via grabData and formatData
 *   Charting: via the ChartingApp JavaFx application
 */
object ReportsDataPrep {

  import collection.mutable

  case class SeriesEntry(groupName: String, inputLine: LLine, native: String, nskew: Int, action: String, nrecs: Int, duration: Int) {
    def seriesName() = s"$nskew $action $native"

    def name() = s"$groupName ${seriesName()}"
  }

  type SeriesValSeq = Seq[SeriesEntry]

  type SeriesInst = mutable.HashMap[String, SeriesValSeq]

  def pr(msg: String) = println(msg)

  case class LLine(tstamp: String, tname: String, nrecs: Int, nparts: Int, nskew: Int,
    native: String, xform: String, action: String, duration: Int, count: Int) {
    def key = s"$tstamp $tname $nparts $nskew $xform $action $native"

    def seriesKey = s"$xform-$action-$native"

    def csvHeader =
      s"TestName,Tstamp,Partitions,SkewFactor,Transform,Action,Native,InputRecords,OutputRecords,Duration"

    def toCsv =
      s"$tname,$tstamp,$nparts,$nskew,$xform,$action,$native,$nrecs,$count,$duration"
  }

  object LLine {
    def apply(line: String) = {
      //      val line = "<09:18:00><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native AggregateByKey/Count - duration=14895 millis count=993127"
      //      pr(line)
      val regex = """.*Completed (?<tstamp>[\d]{4}-[\d]{6})/(?<tname>[\w]+) (?<nrecs>[\d]+)[\w]+ (?<nparts>[\d]+)[\w]+ (?<nskew>[\d]+)[\w]+ (?<native>[\w]+) (?<xform>[\w]+)[ /](?<action>[\w]+) - duration=(?<duration>[\d]+) millis count=(?<count>[\d]+).*""".r
      val regex(tstamp, tname, nrecs, nparts, nskew, native, xform, action, duration, count) = line
      new LLine(tstamp, tname, nrecs.toInt / 1000, nparts.toInt, nskew.toInt, native, xform, action, duration.toInt, count.toInt)
    }

  }

  def grabData(baseDir: String) = {
    def getFiles(baseDir: String, filter: (File) => Boolean): Seq[File] = {
      val out: Seq[Seq[File]] = for (f <- new File(baseDir).listFiles.filter(filter))
        yield {
//          pr(s"${f.getAbsolutePath}")
          f match {
            case _ if f.isDirectory => getFiles(f.getAbsolutePath, filter)
            case _ if f.isFile => Seq(f)
            case _ => throw new IllegalArgumentException(s"Unrecognized file type ${f.getClass.getName}")
          }
        }
      out.flatten
    }
    val basef = new File(baseDir)
    val files = getFiles(baseDir, (path) =>
      path.isDirectory || path.getAbsolutePath.endsWith(".log"))
    val lines = files.map { f =>
      pr(s"processing ${f.getAbsolutePath}")
      val info = scala.io.Source.fromFile(f.getAbsolutePath)
        .getLines.filter(l => l.contains("Completed")).toList
      if (!info.isEmpty) Some(LLine(info.head)) else None
    }.flatten
    pr(s"Number of lines: ${lines.length}")
    lines
  }

  import FxDataUtils._

  import scala.collection.JavaConverters._

  object FxDataUtils {
    type Coords = (JDouble, JDouble, JDouble, JDouble)

    def minMax(data: Seq[XYChart.Data[JDouble, JDouble]]): Coords = {
      data.foldLeft[Coords]((Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue)) { case (l, d) => {
        (if (l._1 < d.getXValue.doubleValue)
          l._1
        else
          d.getXValue.doubleValue,
          if (l._2 > d.getXValue.doubleValue)
            l._2
          else
            d.getXValue.doubleValue,
          if (l._3 < d.getYValue.doubleValue) l._3 else d.getYValue.doubleValue,
          if (l._4 > d.getYValue.doubleValue) l._4 else d.getYValue.doubleValue
          )
      }
      }
    }
  }

  def formatData(lines: Seq[ReportsDataPrep.LLine]): (String,MapSeriesMap) = {
    def formatT(ts: String) = s"${ts.slice(0, 2)}/${ts.slice(2, 4)} ${ts.slice(5, 7)}:${ts.slice(7, 9)}:${ts.slice(9, 11)}"
    def key1(l: LLine) = s"${l.tname} ${l.xform} on ${formatT(l.tstamp)} Partitions=${l.nparts} Skew=${l.nskew}"
    def key2(l: LLine) = s"${l.tname} ${l.tstamp} ${l.nparts} ${l.nskew} ${l.xform} ${l.action} ${l.native}"
    val seriesCollsGrouping = lines.groupBy(key1)
    val sortedSeriesCollsGrouping = SortedMap(seriesCollsGrouping.toList: _*)
    val seriesCollsMap = sortedSeriesCollsGrouping.map { case (k, groupseq) =>
      (k, {
        val unsorted = groupseq.groupBy(key2).map { case (k, ls) => (k, ls.sortBy(_.nrecs)) }
        SortedMap(unsorted.toList: _*)
      }
        )
    }
    val csvData = prepareCsvData(seriesCollsMap)
    val seriesMap = seriesCollsMap.map { case (cgroupkey, cmap) =>
      (cgroupkey, genSeries(cgroupkey, cmap, SeriesPerChart))
    }
    (csvData, SortedMap(seriesMap.toList: _*))
  }

  def prepareCsvData(seriesCollsMap: SortedMap[String,
      SortedMap[String, Seq[ReportsDataPrep.LLine]]]) = {
    val csvHeader = seriesCollsMap.values.head.values.head.head.csvHeader
    val csvLines = seriesCollsMap.mapValues {  case smap =>
      val smapseq = smap.mapValues { case serseq =>
        serseq.map(_.toCsv)
      }.values.toList.flatten
        smapseq
    }.values.flatten.toList
    val out = s"${csvHeader}\n${csvLines.mkString("\n")}"
    out
  }

  def writeToCsv(lines: Seq[LLine], fileName: String) = {
    val csvLines = lines.map(_.toCsv)
    val out = s"${lines(0).csvHeader}\n${csvLines.mkString("\n")}"
    scala.tools.nsc.io.File(fileName).writeAll(out)
    out
  }

  val SeriesPerChart = 5

  type SeriesTup = (Series[JDouble, JDouble], Coords)
  type SeriesSeq = Seq[SeriesTup]
  type SeriesMap = SortedMap[String, SeriesTup]
  type MapSeriesMap = SortedMap[String, SeriesMap]

  def genSeries(seriesTag: String, seriesMap: SortedMap[String, Seq[LLine]], maxSeries: Int):
  SeriesMap = {
    if (seriesMap.size > maxSeries) {
      throw new IllegalArgumentException(s"Can not fit > $maxSeries series in a single chart")
    }
    val seriesTups = seriesMap.map { case (sname, serval) =>
      (sname, serval.map(l => double2Double(l.nrecs.toDouble))
        .zip(serval.map(l => double2Double(l.duration.toDouble))))
    }
    val seriesData = seriesTups.map { case (sname, sersSeq) =>
      (sname, sersSeq.map { case (x, y) => new XYChart.Data(x, y) })
    }
    val obsdata = seriesData.map { case (sname, data) =>
      (sname, (new Series(sname.substring(sname.substring(0, sname.lastIndexOf(" ")).lastIndexOf(" ") + 1),
        FXCollections.observableList(data.asJava)), minMax(data)))
    }
    SortedMap(obsdata.toList: _*)
  }

}

