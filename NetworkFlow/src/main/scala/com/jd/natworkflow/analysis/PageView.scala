package com.jd.natworkflow.analysis

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


//case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, metgod: String, url: String)
//
//case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource=getClass.getResource("/apache.log")
    val dataStream=env.readTextFile(resource.getPath)
      .map(data=>{
        val spl=data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyy:HH:mm:ss")
        val timeStamp = simpleDateFormat.parse(spl(2).trim).getTime
        ApacheLogEvent(spl(0).trim,spl(1).trim,timeStamp,spl(4).trim,spl(5).trim)
      })

  }
}
