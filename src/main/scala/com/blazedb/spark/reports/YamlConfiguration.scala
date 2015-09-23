package com.blazedb.spark.reports

import java.io.FileInputStream

import org.yaml.snakeyaml.Yaml

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

/**
 * class YamlConfiguration
 *
 */
case class YamlConfiguration(ymlFile: String) {
  val yaml = new Yaml

  import collection.JavaConverters._
  import collection.mutable

  val yamlConf = {
    val iy = (for (y <- yaml.loadAll(new FileInputStream(ymlFile)).iterator.asScala) yield y).toList
    val omap = new mutable.HashMap[String, String]()
    iy.foreach { o =>
      o match {
        case _ if o.isInstanceOf[java.util.LinkedHashMap[_, _]] => {
          val sm = o.asInstanceOf[java.util.LinkedHashMap[String, String]].asScala
          omap.++=(sm.iterator)
        }
        case _ => println("o type is %s".format(o.getClass.getName))
      }
    }
    println(s"omap: ${omap}")
    collection.immutable.HashMap[String,String](omap.iterator.toSeq:_*)
  }

  def getConfiguration() = {
    yamlConf
  }

  override def toString() = {
    getConfiguration.mkString(",")
  }

  def apply(key: String) = {
    yamlConf.get(key)
  }

  def apply(key: String, default: String) = {
    yamlConf.getOrElse(key, default)
  }

}

object YamlConfiguration {

  def main(args: Array[String]) {
    val f = java.io.File.createTempFile("yaml-test", null)
    val s = """
abcdef: |
      abc
      def
      g hi hi again
AnotherKey:
 -key value 1
 -key value 2
            """
    tools.nsc.io.File(f).writeAll(s)
    val y = new YamlConfiguration(f.getAbsolutePath)
    println(y)
  }
}
