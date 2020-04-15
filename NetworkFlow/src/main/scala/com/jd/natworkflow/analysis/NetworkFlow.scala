package com.jd.natworkflow.analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
case class ApacheLogEvent(ip:String ,userId :String ,eventTime:Long,metgod : String ,url : String)
case class UrlViewCount(url:String,windowEnd:Long ,count:Long)
object NetworkFlow {
  def main(args: Array[String]): Unit = {
   val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile("")

    env.execute("Network Flow")
  }

}
