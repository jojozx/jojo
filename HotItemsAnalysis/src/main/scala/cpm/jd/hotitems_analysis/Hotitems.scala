package cpm.jd.hotitems_analysis




import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.api.windowing.time.Time

case  class UserBehavior(userId : Long,item :Long ,categoryId :Long,hehavior:String ,timeSTAMP:Long )
case class ItemViewCount(itemId: Long,windowEnd:Long ,count:Long)

object Hotitems {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//
//    val dataStream = env.readTextFile("D:\\workspac\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
//    val UserBeHaviorStream = dataStream.map(data => {
//      val dataArray = data.split(",")
//
//     val aaa= UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt,
//        dataArray(3).trim(), dataArray(4).toLong)
//      aaa
//    })
//    UserBeHaviorStream.keyBy(_.behavior).filter(_.behavior=="pv").assignAscendingTimestamps(_.timestamp * 1000L)
//        .timeWindowAll(Time.minutes(60),Time.minutes(5))
//
//    dataStream.print()
//    UserBeHaviorStream.print()
//    env.execute("a1")
  }

}
