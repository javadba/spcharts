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

import java.awt.Transparency
import java.awt.image.BufferedImage
import java.io.File
import java.lang.{Double => JDouble}
import java.util.Date
import java.util.concurrent.CountDownLatch
import javafx.animation.PauseTransition
import javafx.application.{Application, Platform}
import javafx.concurrent.Task
import javafx.embed.swing.SwingFXUtils
import javafx.event.{EventHandler, ActionEvent}
import javafx.geometry.Insets
import javafx.scene.image.{Image, WritableImage}
import javafx.scene.paint.Color
import javafx.scene.{Node, SnapshotParameters, Scene}
import javafx.scene.chart.LineChart
import javafx.scene.chart.XYChart._
import javafx.scene.control.Label
import javafx.scene.layout.{BorderPane, GridPane}
import javafx.stage.Stage
import javafx.util.Duration
import javax.imageio.ImageIO
import ReportsDataPrep.FxDataUtils.Coords

import collection.JavaConverters._

import ReportsDataPrep.{MapSeriesMap, _}
object ChartingAppExperimental {


  val Title = "IgniteRDD Runtime Performance Compared with Native Spark RDD Three Workers AWS c3.2xlarge Cluster"
  val useLogAxis = true

  class ChartingAppExperimental extends Application {

    val YardstickCss = "/org/yardstick/spark/util/ys-charts.css"
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
        l.setWrapText(true);

        for ((sname, (series, mm)) <- seriesMap) {
          chart.getData().add(series.asInstanceOf[Series[JDouble, JDouble]])
        }
        chart.setStyle("-fx-background-color: white;")
        chart
      }
      var firstStage: Stage = null
      val tpane = new GridPane
      val tpane2 = new GridPane
      tpane.setHgap(10)
      tpane.setVgap(10)
      tpane.setPadding(new Insets(0, 0, 0, 10))
      var mainScene: Scene = null
      var mainScene2: Scene = null
      var mainStage2: Stage = null
      for ((ix, (sname, series)) <- (0 until seriesMap.size).zip(seriesMap)) {
        val chart = createChart(ix, sname, series)
        if (ix == 0) {
          tpane.add(chart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
          mainScene = new Scene(tpane, displaySize._1, displaySize._2)
          mainScene.getStylesheets.add(YardstickCss)
          mainStage.setScene(mainScene)
          mainStage.show()
        }
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            if (ix > 0) {
              tpane.add(chart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
            }
//            if (ix == seriesMap.size - 1) {
//              val fname = s"$reportsPath/summaryChart.jpg"
//              imageTasksPool.submit(mainScene, chart, tpane, fname)
//
//            }
          }
        })
      }
      for ((ix, (sname, series)) <- (0 until seriesMap.size).zip(seriesMap)) {
        val chart = createChart(ix, sname, series)
//        if (ix == 0) {
//          tpane2.add(chart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
//          mainScene2 = new Scene(tpane, displaySize._1, displaySize._2)
//          mainScene2.getStylesheets.add(YardstickCss)
//          mainStage2.setScene(mainScene2)
//          mainStage2.show()
//        }
        javafx.application.Platform.runLater(new Runnable() {
          val ix2 = ix
          override def run() = {
            if (ix2 > 0) {
              tpane2.add(chart, math.floor(ix2 / MaxCols).toInt, ix2 % MaxCols)
            }
            if (ix2 == seriesMap.size - 1) {
              val fname = s"$reportsPath/summaryChart.jpg"
              imageTasksPool.submit(mainScene2, chart, tpane2, fname)

            }
          }
        })
      }
      for ((ix, (sname, series)) <- (0 until seriesMap2.size).zip(seriesMap2)) {
        val chart = createChart(ix, sname, series)
        val chart2 = createChart(ix, sname, series)
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            val stage = new Stage
            val borderPane = new BorderPane
            borderPane.getChildren.add(chart)
            val scene = new Scene(borderPane, singleDisplaySize._1, singleDisplaySize._2)
            stage.setScene(scene)
            scene.getStylesheets.add(YardstickCss)
            stage.show()
            val borderPane2 = new BorderPane
            borderPane2.getChildren.add(chart2)
            val fname = s"$reportsPath/${sname.replace(" ", "_").replace("/", "-").replace(":", "-")}.jpg"
            imageTasksPool.submit(scene, chart2, borderPane2, fname)
          }
        })
      }

    }
    val imageTasksPool = FxOfflineThreadPoolExperimental[Boolean]("ImagesPool") {
      (scene, chart, pane, fileName) => {
        Thread.sleep(5000)
        val latch = new CountDownLatch(1)
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            val snaScene = new Scene(pane)
//            val snaScene = new Scene(chart.getParent)
            val fname = s"$reportsPath/summaryChart.jpg"

            val p = new SnapshotParameters()
            p.setFill(Color.WHITE)
//            val snapShot = scene.snapshot(null)
//            val snapShot = chart.snapshot(p,null)
//            pane.snapshot(
//            new Callback[SnapshotResult, Void]() {
//              override def call(result: SnapshotResult): Void = {
                pr(s"${new Date().toString} Saving image to ${fileName}")
//                saveSnapshot(chart, fileName)

                val pt = new PauseTransition(Duration.seconds(5));
                pt.setOnFinished(new EventHandler[ActionEvent]() {
                  override def handle(event: ActionEvent): Unit = {
                    saveSnapshot(chart, fileName);
                  }
                })
                pt.play();

//                if (!ImageIO.write(SwingFXUtils.fromFXImage(result.getImage, null),
//                  "jpg", new File(fileName))) {
//                  throw new IllegalArgumentException(s"Failed to write image $fileName")
//                }
//                latch.countDown
//                null
//              }
//            },p,null)
          }
        })
        println("Waiting for latch ..")
        latch.await
        println("Latch completed ..")
        true
      }
    }
  }

  def createImage(node: Node): Image = {

        var wi: WritableImage = null

        val parameters = new SnapshotParameters();
        parameters.setFill(Color.WHITE);

        val imageWidth = node.getBoundsInLocal().getWidth();
        val imageHeight = node.getBoundsInLocal().getHeight();

        wi = new WritableImage(imageWidth.toInt, imageHeight.toInt);
        node.snapshot(parameters, wi);

        wi;
    }

    def saveSnapshot(node: Node, fileName: String) {

        val image = createImage(node);

        // save image !!! has bug because of transparency (use approach below) !!!
        // ImageIO.write(SwingFXUtils.fromFXImage( selectedImage.getImage(), null), "jpg", file);

        // save image (without alpha)
        val bufImageARGB = SwingFXUtils.fromFXImage(image, null);
        val bufImageRGB = new BufferedImage(bufImageARGB.getWidth(), bufImageARGB.getHeight(),
          Transparency.OPAQUE);

        val graphics = bufImageRGB.createGraphics();
        graphics.drawImage(bufImageARGB, 0, 0, null);

        ImageIO.write(bufImageRGB, "jpg", new File(fileName));

        graphics.dispose();

        System.out.println( "Image saved: " + fileName);

    }
  def saveUsingJavaFx(args: Array[String]): Unit = {

    Platform.setImplicitExit(false)
    pr("Saving with JavaFX")
    Application.launch(classOf[ChartingAppExperimental], args: _*)
    pr("launched")
  }

  def main(args: Array[String]) {
    saveUsingJavaFx(args)
  }

}
