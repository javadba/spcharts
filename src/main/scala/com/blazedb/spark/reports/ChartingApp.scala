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
import java.util.Date
import javafx.application.{Application, Platform}
import javafx.embed.swing.SwingFXUtils
import javafx.geometry.Insets
import javafx.scene.Scene
import javafx.scene.chart.LineChart
import javafx.scene.chart.XYChart._
import javafx.scene.control.Label
import javafx.scene.layout.GridPane
import javafx.stage.Stage
import javax.imageio.ImageIO

import ReportsDataPrep.FxDataUtils.Coords
import ReportsDataPrep.{MapSeriesMap, _}

import scala.collection.JavaConverters._

/**
 * ChartingApp
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
object ChartingApp {


  val Title = "IgniteRDD Runtime Performance Compared with Native Spark RDD Three Workers AWS c3.2xlarge Cluster"
  val useLogAxis = true

  class ChartingApp extends Application {

    val YardstickCss = "org/yardstick/spark/reports/ys-charts.css"
    var dataPath: String = _
    var reportsPath: String = _

    def getData(dataPath: String): (String, MapSeriesMap) = {
      val lines = grabData(dataPath)

      val (csvData, overSeriesMap) = ReportsDataPrep.formatData(coreMetaInfo, lines)
      (csvData, overSeriesMap)
    }

    override def start(mainStage: Stage): Unit = {
      val params = getParameters()
      val parameters = params.getRaw().asScala
      dataPath = parameters(0)
      mainStage.setTitle(Title)

      reportsPath = s"$dataPath/reports"
      new File(reportsPath).mkdirs()
      val (csvData, seriesMap) = getData(dataPath)
      val seriesMap2 = getData(dataPath)._2
      tools.nsc.io.File(s"$reportsPath/parsed.csv").writeAll(csvData)

      val ScreenWidth = 1920
      val ScreenHeight = 1200
      val SingleScreenWidth = 800
      val SingleScreenHeight = 600
      val DisplayW = ScreenWidth - 120
      val DefaultDisplayH = ScreenHeight - 100
      val MinChartH = 400
      val MaxCols = 3
      val nCharts = seriesMap.size
      val nCols = math.min(MaxCols, nCharts)
      val nRows = math.ceil(nCharts / nCols)
      val DisplayH = if (nRows <= 3) DefaultDisplayH else (MinChartH + 10) * nRows
      val displaySize = (DisplayW /* / nCols */ , DisplayH, nRows)
      val singleDisplaySize = (SingleScreenWidth, SingleScreenHeight, nRows)

      def createChart(ix: Int, chartTitle: String, seriesMap: SeriesMap) = {
        val mm = seriesMap.values.foldLeft(
          new Coords(Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue)) {
          case (c, (series, s)) =>
            new Coords(math.min(c._1, s._1), math.max(c._2, s._2), math.min(c._3, s._3),
              math.max(c._4, s._4))
        }
        val (xAxis, yAxis) = (new LogAxis(math.max(1, mm._1), mm._2),
          new LogAxis(math.max(1, mm._3), mm._4))

        xAxis.setTickLabelRotation(20)
        xAxis.setLabel("Number of Records (Thousands)")
        yAxis.setLabel("Elapsed Time (msecs) (Lower is Better)")
        val chart = new LineChart[JDouble, JDouble](xAxis, yAxis)
        chart.setTitle(s"Ignite-on-Spark Performance - $chartTitle")
        chart.setTitle(s"$chartTitle")
        val l = chart.lookup(".chart-title").asInstanceOf[Label]
        l.setWrapText(true)

        for ((sname, (series, mm)) <- seriesMap) {
          chart.getData().add(series.asInstanceOf[Series[JDouble, JDouble]])
        }
        chart.setStyle("-fx-background-color: white;")
        chart
      }
      var firstStage: Stage = null
      val tpane = new GridPane
      tpane.setHgap(10)
      tpane.setVgap(10)
      tpane.setPadding(new Insets(0, 0, 0, 10))
      var mainScene: Scene = null
      for ((ix, (sname, series)) <- (0 until seriesMap.size).zip(seriesMap)) {
        val lineChart = createChart(ix, sname, series)
        if (ix == 0) {
          tpane.add(lineChart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
          mainScene = new Scene(tpane, displaySize._1, displaySize._2)
          mainScene.getStylesheets.add(YardstickCss)
          mainStage.setScene(mainScene)
          mainStage.sizeToScene();
          mainStage.show()
        }
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            if (ix > 0) {
              tpane.add(lineChart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
            }
            if (ix == seriesMap.size - 1) {
              val fname = s"$reportsPath/summaryChart.png"
              imageTasksPool.submit(mainScene, fname)

            }
          }
        })
      }

      for ((ix, (sname, series)) <- (0 until seriesMap2.size).zip(seriesMap2)) {
        val lineChart = createChart(ix, sname, series)
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            val tpane1 = new GridPane
            val stage = new Stage
            tpane1.setHgap(10)
            tpane1.setVgap(10)
            tpane1.setPadding(new Insets(0, 0, 0, 10))
            tpane1.add(lineChart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
            val mainScene1 = new Scene(tpane1, singleDisplaySize._1, singleDisplaySize._2)
            mainScene1.getStylesheets.add(YardstickCss)
            stage.setScene(mainScene1)
            stage.sizeToScene();
            stage.show()

            val fname = s"$reportsPath/${
              sname.replace(" ", "_").replace("/", "-")
                .replace(":", "-")
            }.png"

            imageTasksPool.submit(mainScene1, fname)

          }
        })
      }

    }

    val imageTasksPool = FxOfflineThreadPool[Boolean]("ImagesPool") {
      (scene: Scene, fileName: String) => {
        Thread.sleep(50)
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            val fname = s"$reportsPath/summaryChart.png"
            val snapShot = scene.snapshot(null)
            pr(s"${new Date().toString} Saving image to ${fileName}")
            if (!ImageIO.write(SwingFXUtils.fromFXImage(snapShot, null),
              "png", new File(fileName))) {
              throw new IllegalArgumentException(s"Failed to write image $fileName")
            }
          }
        })
        true
      }
    }
  }

  def saveUsingJavaFx(args: Array[String]): Unit = {

    Platform.setImplicitExit(false)
    pr("Saving with JavaFX")
    Application.launch(classOf[ChartingApp], args: _*)
    pr("launched")
  }

  def main(args: Array[String]) {
    saveUsingJavaFx(args)
  }

}

