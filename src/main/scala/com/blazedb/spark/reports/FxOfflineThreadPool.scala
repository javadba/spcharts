package com.blazedb.spark.reports

import java.util.concurrent._
import javafx.scene.Scene

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

import reflect.runtime.universe._
class FxOfflineThreadPool[T: TypeTag](threadGroupName: String)(block: (Scene, String) => T)  {

  import reflect.runtime.universe._
  class CallableToRunnable[T: TypeTag](c: Callable[T]) extends Runnable with Callable[T] {
    var ret: T = _
    override def run(): Unit = {
      ret = c.call
    }
    override def call(): T = c.call()
  }

  val factory = new ThreadFactory() {
    override def newThread(c: Runnable) = {
      val t = new Thread(c)
      t.setName(threadGroupName)
      t.setDaemon(true)
      t
    }
  }
  val pool =  Executors.newSingleThreadExecutor(factory)
  def submit(scene: Scene, fileName: String): T = {
    block(scene, fileName)
  }
  def submit(task: Callable[T]): T = {
    val r = new CallableToRunnable(task)
    val future = pool.submit(r.asInstanceOf[Callable[T]]).asInstanceOf[Future[T]]
    val res = future.get
    res
  }
}
/**
 * FxOfflineThreadpool
 *
 */
object FxOfflineThreadPool {

  def apply[T: TypeTag](name: String)(block: (Scene, String) => T) =
      new FxOfflineThreadPool(name)(block)

}



