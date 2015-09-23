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
import scala.util.matching.Regex

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
 * Data processing via grabData and formatData
 * Charting: via the ChartingApp JavaFx application
 */
object ReportsDataPrep {

  import collection.mutable

  type ColValT = ColVal[_]

  def pr(msg: String) = println(msg)

  case class ColInfo(name: String, csvName: String, ctype: Class[_] = classOf[String], regex: String = """(?<$NAME>[\w]+)""",
                     xform: (String) => String = identity)

  import reflect.runtime.universe._

  sealed abstract class ColVal[T: TypeTag](val meta: ColInfo, val cval: T) {
    def dtype() = cval.getClass
    override def toString() = { cval.toString }
  }

  class StrColVal(cinfo: ColInfo, cv: String) extends ColVal[String](cinfo, cv)

  class IntColVal(cinfo: ColInfo, cv: Int) extends ColVal[Int](cinfo, cv)

  class DoubleColVal(cinfo: ColInfo, cv: Double) extends ColVal[Double](cinfo, cv)

  val CI = ColInfo

  case class MetaInfo(sampleLine: String, basePat: String, allCols: Seq[ColInfo],
                      key1Pat: String, key2Pat: String,
                      xaxisField: String, yaxisField: String = "duration") {
    val colsMap = Map(allCols.map(c => (c.name, c)): _*)
    val csvHeader = allCols.map(_.csvName).mkString(",")
//    val systemCols = Array("tstamp", "tname", "duration", "outrecs")
//    val userCols = allCols.filter(c => !systemCols.contains(c.name.toLowerCase))
    val regex = {
      val groups = allCols.map {
        _.name
      }
      val mmap = allCols.map { ci =>
        ci.regex.replace("$NAME",ci.name)
      }.mkString(" ")

      //  .*Completed (?<tstamp>[\d]{4}-[\d]{6})/(?<tname>[\w]+) (?<inrecs>[\d]+)recs (?<nparts>[\d]+)parts (?<nskew>[\d]+)skew (?<native>[\w]+) (?<xform>[\w]+) (?<action>[\w]+) - duration=(?<duration>[\d]+) millis count=(?<outrecs>[\d]+).*
//      new Regex( s""".*Completed (?<tstamp>[\\d]{4}-[\\d]{6})/(?<tname>[\\w]+) $mmap - duration=(?<duration>[\\d]+\\.[\\d]+) seconds count=(?<outrecs>[\\d]+).*""", groups: _*)
//       s""".*Completed $COLUMN_GROUPS""", groups: _*)
      val r = basePat.replace("$COLUMN_GROUPS", mmap)
      new Regex( r, groups: _*)
    }

  }

  case class DataMap(dlist: SortedMap[String, ColValT])

  case class LLine(meta: MetaInfo, fields: Seq[Any]) {
    val valsMap = fields.map {
      case f: StrColVal => (f.meta.name, f)
      case f: IntColVal => (f.meta.name, f)
      case f: DoubleColVal => (f.meta.name, f)
      case a => throw new UnsupportedOperationException(s"unsupported LLine datatype: ${a.toString}")
    }.toMap
    assert(valsMap.keySet == meta.colsMap.keySet, s"Received a line with deficient fields: "
      + s"${valsMap.keySet} required=${meta.colsMap.keySet}")

    def applyVals(pat: String) = {

      val outres = valsMap.keys.foldLeft(pat) { case (str, k) => {
        val regex = ("\\$" + k).r
        val replaced = regex.replaceAllIn(str, valsMap(k).cval.toString)
        replaced
      }
      }
      outres
    }

    val key = applyVals(meta.key1Pat)
    val seriesKey = applyVals(meta.key2Pat)

    def toCsv = meta.allCols.map(ci => valsMap(ci.name).cval).mkString(",")
  }

  object LLine {
    def apply(meta: MetaInfo, line: String) = {
      val rmatch = meta.regex.findFirstMatchIn(line)
      val retval = if (rmatch.isDefined) {
        val m = rmatch.get
        new LLine(meta,
          meta.allCols.map { case ci =>
            ci.ctype match {
              case q if q == classOf[String] => new StrColVal(ci, m.group(ci.name))
              case q if q == classOf[Int] => new IntColVal(ci, m.group(ci.name).toInt)
              case q if q == classOf[Double] => new DoubleColVal(ci, m.group(ci.name).toDouble)
              case _ => throw new UnsupportedOperationException(s"unsupported LLine datatype: ${ci.ctype.getName}")
            }
          }
        )
      } else {
        throw new IllegalArgumentException(s"Unable to parse line $line")
      }
      retval
    }
    def applyVals(pat: String, vmap: Map[String, _]) = {

      val outres = vmap.keys.foldLeft(pat) { case (str, k) => {
        val regex = ("\\$" + k).r
        val replaced = regex.replaceAllIn(str, vmap(k).toString)
        replaced
      }
      }
      outres
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
    val lines = files.flatMap { f =>
      pr(s"processing ${f.getAbsolutePath}")
      val info = scala.io.Source.fromFile(f.getAbsolutePath)
        .getLines.filter(l => l.contains("Completed")).toList
      info.headOption
    }
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

  def formatTstamp(ts: String) = s"${ts.slice(0, 2)}/${ts.slice(2, 4)} ${ts.slice(5, 7)}:${ts.slice(7, 9)}:${ts.slice(9, 11)}"

  def formatData(meta: MetaInfo, strLines: Seq[String]): (String, MapSeriesMap) = {
    val lines = strLines.map(LLine(meta, _))
    val seriesCollsGrouping = lines.groupBy(_.key)
    val sortedSeriesCollsGrouping = SortedMap(seriesCollsGrouping.toList: _*)
    val seriesCollsMap = sortedSeriesCollsGrouping.map { case (k, groupseq) =>
      (k, {
        val unsorted = groupseq.groupBy(_.seriesKey).map
        { case (k, ls) => (k, ls.sortBy(_.valsMap(meta.xaxisField).cval.asInstanceOf[Int])) }
        SortedMap(unsorted.toList: _*)
      }
        )
    }
    val csvData = prepareCsvData(meta, seriesCollsMap)
    val seriesMap = seriesCollsMap.map { case (cgroupkey, cmap) =>
      (cgroupkey, genSeries(meta, cgroupkey, cmap, SeriesPerChart))
    }
    (csvData, SortedMap(seriesMap.toList: _*))
  }

  def prepareCsvData(meta: MetaInfo, seriesCollsMap: SortedMap[String,
    SortedMap[String, Seq[ReportsDataPrep.LLine]]]) = {
    val csvHeader = meta.csvHeader
    val csvLines = seriesCollsMap.mapValues { case smap =>
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
    val out = s"${lines(0).meta.csvHeader}\n${csvLines.mkString("\n")}"
    scala.tools.nsc.io.File(fileName).writeAll(out)
    out
  }

  val SeriesPerChart = 8

  type SeriesTup = (Series[JDouble, JDouble], Coords)
  type SeriesSeq = Seq[SeriesTup]
  type SeriesMap = SortedMap[String, SeriesTup]
  type MapSeriesMap = SortedMap[String, SeriesMap]

  def genSeries(meta: MetaInfo, seriesTag: String, seriesMap: SortedMap[String, Seq[LLine]], maxSeries: Int):
  SeriesMap = {
    if (seriesMap.size > maxSeries) {
      throw new IllegalArgumentException(s"Can not fit > $maxSeries series in a single chart")
    }
    val seriesTups = seriesMap.map { case (sname, serval) =>
//      (sname, serval.map(l => double2Double(l.valsMap(meta.xaxisField).cval.asInstanceOf[Int].toDouble))
//        .zip(serval.map(l => double2Double(l.valsMap(meta.yaxisField).cval.asInstanceOf[Double]))))
      (sname, serval.map{case l =>
        val myv = l.valsMap(meta.xaxisField).cval.toString
        val xform = meta.colsMap(meta.xaxisField).xform(myv)
        println(s"myv=$myv xform=$xform")
        double2Double(meta.colsMap(meta.xaxisField).xform(l.valsMap(meta.xaxisField).cval.toString).toInt)}
            .zip(serval.map(l => double2Double(l.valsMap(meta.yaxisField).cval.asInstanceOf[Double])))
    )}
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

