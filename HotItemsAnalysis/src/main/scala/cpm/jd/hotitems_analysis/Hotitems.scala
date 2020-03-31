package cpm.jd.hotitems_analysis

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
case  class UserBehavior(UserId :Long ,itemId :Long ,categoryId:Integer ,behavior:String,timestamp :Long )
case class ItemViewCount(itemId :Long ,windowEnd:Long ,Count :Long)
object Hotitems {
  def main(args: Array[String]): Unit = {
   val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream =env.readTextFile("D:\\workspac\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
     val UserBeHaviorStream=dataStream.map(data =>{
       val dataArray=data.split(",")
       UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,
         dataArray(3).trim(),dataArray(4).toLong)
     })
    UserBeHaviorStream.assignAscendingTimestamps(_.timestamp*1000L)
    dataStream.print()
    env.execute("a1")
  }

}
