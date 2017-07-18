/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.examples.join

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, List => JList, TreeMap => TMap}

import org.apache.flink.api.common.functions.{JoinFunction, MultiPartitioner}
import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.scala.examples.join.ThetaJoinDemo.JoinerType.JoinerType
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.Random

object ThetaJoinDemo {

  def formatTime(time: Long): String = {
    if (null == time) {
      return "null"
    }
    val f: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss SSS")
    f.format(new Date(time))
  }

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(2000)
    val orderA: DataStream[Order] = env.createInput(new InfiniteSource(2000)).setParallelism(1)
      .assignTimestampsAndWatermarks(new WatermarksAssigner(2000))

    val orderB: DataStream[Order] = env.createInput(new InfiniteSource(1000)).setParallelism(1)
      .assignTimestampsAndWatermarks(new WatermarksAssigner(2000))

    val connectedStream = orderA.connect(orderB).process(new JoinMerge[Order, Order]())

    val leftResult = connectedStream
      .multicast(new JoinPartitioner[Order, Order](JoinerType.LEFT))
      .process(new Joiner[Order, Order, Order2](
        new SimpleJoinFunction(JoinerType.LEFT), JoinerType.LEFT, 10000, 5000, 0)).setParallelism(4)

    val rightResult = connectedStream
      .multicast(new JoinPartitioner[Order, Order](JoinerType.RIGHT))
      .process(new Joiner[Order, Order, Order2](
        new SimpleJoinFunction(JoinerType.RIGHT), JoinerType.RIGHT, 10000, 5000, 0)).setParallelism(4)

    leftResult.connect(rightResult).process(new CoProcessFunction[Order2, Order2, Order2] {
      override def processElement1(value: Order2, ctx: CoProcessFunction[Order2, Order2, Order2]#Context, out:
      Collector[Order2]): Unit = {
        out.collect(value)
      }

      override def processElement2(value: Order2, ctx: CoProcessFunction[Order2, Order2, Order2]#Context, out:
      Collector[Order2]): Unit = {
        out.collect(value)
      }
    }).print()
    println(env.getExecutionPlan)
    env.execute()
  }

  case class Order(user: Long, product: String, amount: Int, ts: Long)

  case class Order2(user1: Long, user2: Long, product1: String, product2: String, amount1: Int, amount2: Int,
                    deltaT: Long, joinerType: JoinerType)


  /**
    * InfiniteSource
    *
    * @param interval
    */
  class InfiniteSource (interval: Long) extends GenericInputFormat[Order] {
    var a: Long = 0
    val b: Seq[String] = Seq("beer", "diaper", "rubber")
    var c: Int = 0

    override def reachedEnd(): Boolean = {
      false
    }

    override def nextRecord(reuse: Order): Order = {
      Thread.sleep(interval)
      a += 1
      c += 1
      Order(a, b(Random.nextInt(3)), c, System.currentTimeMillis() - Random.nextInt(2000))
    }
  }

  /**
    * SimpleJoinFunction*
    * @param joinerType
    */
  class SimpleJoinFunction (joinerType:JoinerType) extends JoinFunction[Order, Order, Order2] {
    override def join(left: Order, right: Order): Order2 = {
      if (left.product.equals(right.product)){
        Order2(left.user, right.user, left.product, right.product, left.amount, right.amount,
          left.ts - right.ts, joinerType);
      } else {
        null
      }
    }
  }

  /**
    * WatermarksAssigner
    *
    * @param interval
    */
  class WatermarksAssigner(interval: Long) extends AssignerWithPunctuatedWatermarks[Order] {
    var lastWatermarks: Long = 0

    override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
      element.ts
    }

    override def checkAndGetNextWatermark(lastElement: Order, extractedTimestamp: Long): Watermark = {
      if (extractedTimestamp >= lastWatermarks + interval) {
        new Watermark(lastWatermarks + ((extractedTimestamp - lastWatermarks) / interval) * interval)
      } else {
        null
      }
    }
  }

  /**
    * JoinMerge
    *
    * @tparam L
    * @tparam R
    */
  class JoinMerge[L, R] extends CoProcessFunction[L, R, Either[L, R]] {
    override def processElement1(value: L, ctx: CoProcessFunction[L, R, Either[L, R]]#Context,
                                 out: Collector[Either[L, R]]): Unit = {
      out.collect(Left(value))
    }

    override def processElement2(value: R, ctx: CoProcessFunction[L, R, Either[L, R]]#Context,
                                 out: Collector[Either[L, R]]): Unit = {
      out.collect(Right(value))
    }
  }

  /**
    * JoinPartitioner
    *
    * @tparam L
    * @tparam R
    */
  class JoinPartitioner[L, R] (joinerType: JoinerType) extends MultiPartitioner[Either[L, R]] {
    var targets: Array[Int] = null

    override def partition(record: Either[L, R], numPartitions: Int): Array[Int] = {
      //TODO we need a more complicated hybrid partitioning mechanism here
      if (joinerType == JoinerType.LEFT) {
        if (record.isLeft) {
          if (!(null != targets && targets.length == numPartitions)) {
            targets = Array.range(0, numPartitions)
          }
          return targets
        } else {
          Array(Random.nextInt(numPartitions))
        }
      } else {
        if (record.isRight) {
          if (!(null != targets && targets.length == numPartitions)) {
            targets = Array.range(0, numPartitions)
          }
          return targets
        } else {
          Array(Random.nextInt(numPartitions))
        }
      }
    }
  }

  object JoinerType extends Enumeration {
    type JoinerType = Value
    val LEFT, RIGHT = Value
  }

  /**
    * Joiner
    * @param joinFunction
    * @param joinerType
    * @param windowSize
    * @param lateness
    * @param rOffset
    * @tparam L
    * @tparam R
    * @tparam O
    */
  class Joiner[L, R, O](val joinFunction: JoinFunction[L, R, O],
                        val joinerType: JoinerType,
                        val windowSize: Long,
                        val lateness: Long,
                        val rOffset: Long)
    extends ProcessFunction[Either[L, R], O] {

    var leftCache = new TMap[Long, JList[L]]
    val rightCache = new TMap[Long, JList[R]]

    val leftBuffer = new TMap[Long, JList[L]]
    val rightBuffer = new TMap[Long, JList[R]]

    var result: O = _
    var lastWatermark: Long = 0
    var currentWatermark: Long = 0
    var time: Long = _

    def addToCache[T](value: T, time: Long, cache: TMap[Long, util.List[T]]): Unit = {
      if (!cache.containsKey(time)) {
        cache.put(time, new util.LinkedList[T]())
      }
      cache.get(time).add(value)
    }

    def triggerJoin(value: Either[L, R], time: Long, out: Collector[O]): Unit = {
      joinerType match {
        case JoinerType.LEFT => {
          if (value.isRight) {
            val right = value.right.get
            addToCache[R](right, time, rightBuffer)
            for (leftList: util.List[L] <- leftCache.tailMap(time - windowSize).values()) {
              for (left <- leftList) {
                doJoin(left, right, out, false)
              }
            }
          } else if (value.isLeft) {
            val left = value.left.get
            addToCache[L](value.left.get, time, leftCache)
            for (rightList: util.List[R] <- rightBuffer.subMap(time, time + windowSize).values()) {
              for (right <- rightList) {
                doJoin(left, right, out, true)
              }
            }
          }
        }
        case JoinerType.RIGHT => {
          if (value.isLeft) {
            val left = value.left.get
            addToCache[L](left, time, leftBuffer)
            for (rightList: util.List[R] <- rightCache.tailMap(time - windowSize).values()) {
              for (right <- rightList) {
                doJoin(left, right, out, false)
              }
            }
          } else if (value.isRight) {
            val right = value.right.get
            addToCache[R](value.right.get, time, rightCache)
            for (leftList: util.List[L] <- leftBuffer.subMap(time, time + windowSize).values()) {
              for (left <- leftList) {
                doJoin(left, right, out, true)
              }
            }
          }
        }
      }
    }

    def triggerClean(watermark: Long): Unit = {
      joinerType match {
        case JoinerType.LEFT => {
          rightBuffer.headMap(watermark - lateness).clear()
          leftCache.headMap(watermark - windowSize - lateness, false).clear()
        }
        case JoinerType.RIGHT => {
          leftBuffer.headMap(watermark - lateness).clear()
          rightCache.headMap(watermark - windowSize - lateness, false).clear()
        }
      }
    }

    def doJoin(left: L, right: R, out: Collector[O], outOfOrder: Boolean): Unit = {
      result = joinFunction.join(left, right)
      if (null != result) {
        out.collect(result)
        if (outOfOrder) {
          println("OutOfOrder Join: " + result)
        }
      }
    }

    override def processElement(value: Either[L, R],
                                ctx: ProcessFunction[Either[L, R], O]#Context,
                                out: Collector[O]): Unit = {
      time = ctx.timestamp()
      currentWatermark = ctx.timerService().currentWatermark()

      triggerJoin(value, time, out)
      if (lastWatermark < ctx.timerService().currentWatermark()) {
        triggerClean(time)
      }
      lastWatermark = currentWatermark
    }
  }
}
