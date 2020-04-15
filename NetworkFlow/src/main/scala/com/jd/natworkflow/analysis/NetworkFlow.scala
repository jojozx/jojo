package com.jd.natworkflow.analysis


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, metgod: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\workspac\\UserBehaviorAnalysis\\NetworkFlow\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyy:HH:mm:ss")
        val timeStamp = simpleDateFormat.parse(dataArray(2).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timeStamp, dataArray(4).trim, dataArray(5).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      }).keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new windowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(10))
    println(dataStream)
    env.execute("Network Flow")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = {
    0L
  }

  override def add(in: ApacheLogEvent, acc: Long): Long = {
    acc + 1
  }

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = {
    acc + acc1
  }
}

class windowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlstate: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlstate.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
    val iter = urlstate.get().iterator()
    while (iter.hasNext) {
      allUrlViews += iter.next()
    }
    urlstate.clear()
    val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topSize)
    val result: StringBuilder = new StringBuilder()
    result.append("时间,").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrlViews.indices) {
      val currentUrlVieW = sortedUrlViews(i)
      result.append("NO: ", i).append("\n")
      result.append("url:", currentUrlVieW.url).append("十分钟内访问量: ", currentUrlVieW.count)

    }
    result.append("=========================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}