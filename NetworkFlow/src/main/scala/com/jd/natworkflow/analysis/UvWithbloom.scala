package com.jd.natworkflow.analysis

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timeStamp: Long)

//窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
case class UvCount(windowEnd: Long, count: Long)

object UvWithbloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/apache.log")
    val dataStream = env.readTextFile("D:\\workspac\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      }).assignAscendingTimestamps(_.timeStamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("dummtKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(3))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloon())

    dataStream.print()
    env.execute("uv with bolean job")
  }
}





class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

class Bloom (size: Long) extends Serializable {
  private val cap = if (size > 0) size else 1 << 27

  def hash( value:String , seed :Int):Long={
    var result:Long = 0L
  for(i<- 0 until  value.length ){
   result=result*seed+value.charAt(i)
  }
    result & (cap-1)
  }
}
class UvCountWithBloon() extends ProcessWindowFunction[(String ,Long),UvCount,String,TimeWindow]{
  //redis
  lazy val jedis=new Jedis("",6379)
  lazy val bloom=new Bloom(1<<92)

  override def process(key: String, context: ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]#Context, iterable: lang.Iterable[(String, Long)], collector: Collector[UvCount]): Unit = {
    val storeKey=context.window.getEnd.toString
    var Count =0L
    var jedisKey:String=jedis.hget("count",storeKey)
    if(jedisKey!=null){
         Count =jedisKey.toLong
    }
    val
  }
}