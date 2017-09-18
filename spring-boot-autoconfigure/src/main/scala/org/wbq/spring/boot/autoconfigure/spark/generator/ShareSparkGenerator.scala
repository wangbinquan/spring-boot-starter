/*
 *
 * Copyright BinquanWang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.wbq.spring.boot.autoconfigure.spark.generator

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.wbq.spring.boot.autoconfigure.properties.SparkProperties
import org.wbq.spring.boot.autoconfigure.spark.generator.SparkInstanceStatus.SparkInstanceStatus
import org.wbq.spring.boot.autoconfigure.spark.proxy.{SparkContextProxy, SparkContextStopCallback}

class ShareSparkGenerator(sparkProperties: SparkProperties) extends SparkGenerator {
  private val sparkConf = {
    val conf = new SparkConf()
    conf.setMaster(sparkProperties.getMaster).setAppName(sparkProperties.getAppName)
    import scala.collection.JavaConversions._
    for (entry <- sparkProperties.getOtherSparkConf.entrySet) {
      conf.set(entry.getKey, entry.getValue)
    }
    conf
  }
  var sparkInstanceHolder: SparkInstanceHolder = _
  override def getSparkSession = {
    synchronized{
      if(sparkInstanceHolder == null){
        sparkInstanceHolder = new SparkInstanceHolder(sparkConf)
        sparkInstanceHolder.getSparkSession()
      }
      else {
        val sparkSession = sparkInstanceHolder.getSparkSession()
        if (sparkSession == null){
          while(sparkInstanceHolder.getState() != SparkInstanceStatus.Closed){
            Thread.sleep(1000)
          }
          sparkInstanceHolder = new SparkInstanceHolder(sparkConf)
          sparkInstanceHolder.getSparkSession()
        }
        else sparkSession
      }
    }
  }

}

case class SparkInstanceHolder(sparkConf: SparkConf){
  private var state: SparkInstanceStatus = SparkInstanceStatus.Initializing
  private val refNum: AtomicInteger = new AtomicInteger(0)
  private  val sparkContextStopCallback: SparkContextStopCallback = new SparkContextStopCallback {
    override def onSparkContextStop(): Boolean = {
      synchronized {
        if (refNum.decrementAndGet() <= 0) {
          state = SparkInstanceStatus.Closing
          true
        }
        else false
      }
    }

    override def afterSparkContextStop(): Unit = {
      state = SparkInstanceStatus.Closed
    }
  }

  private val sparkContext = new SparkContextProxy(sparkConf, sparkContextStopCallback)
  val sparkSession: SparkSession = SparkSessionBuilder.build(sparkConf, sparkContext)
  state = SparkInstanceStatus.Active

  def getState() = {
    synchronized {
      this.state
    }
  }
  def getSparkSession() =
    synchronized {
      if(this.state == SparkInstanceStatus.Active){
        refNum.incrementAndGet()
        this.sparkSession
      } else null
    }
}

object SparkInstanceHolder {

}

object SparkInstanceStatus extends Enumeration{
  type SparkInstanceStatus = Value
  val Initializing, Active, Closing, Closed = Value
}
