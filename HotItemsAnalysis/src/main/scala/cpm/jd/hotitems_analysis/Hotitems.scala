package cpm.jd.hotitems_analysis


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, hehavior: String, timeStamp: Long)
//窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties =new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //   kafka数据源
// val dataStream=  env
//   .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
    val dataStream = env.readTextFile("D:\\workspac\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        .map(data => {
          val arrayStream = data.split(",")
          UserBehavior(arrayStream(0).trim.toLong, arrayStream(1).trim.toLong, arrayStream(2).trim.toInt, arrayStream(3).trim, arrayStream(4).trim.toLong)
        }).assignAscendingTimestamps(_.timeStamp )
    dataStream.print()
      val processedStream=dataStream.filter(_.hehavior == "pv").keyBy(_.itemId)
        .timeWindow(Time.seconds(2),Time.seconds(1))
        .aggregate(new CountAgg(),new WindowResult())
        .keyBy(_.windowEnd)
        .process(new TopNHotItem(3) )
      processedStream.print()
      env.execute("hot items job")


//    val properties = new Properties()
//   kafkaSink原
//   val StreamOutKafka =processedStream.addSink(new FlinkKafkaProducer[String]("hotit", new SimpleStringSchema(),properties))



  }

}
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
//自定义预集合函数求平均数

class AvergeAgg()extends AggregateFunction[UserBehavior,(Long ,Int),Double] {
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timeStamp,acc._2+1)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = ((acc._1+acc1._1),(acc._2+acc1._2))
}
class WindowResult() extends WindowFunction[Long ,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    val itemId:Long=key.asInstanceOf[Tuple1[Long]].f0
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}
class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount]= null

  override def open(parameters: Configuration): Unit = {
     itemState=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]) )
  }
  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd+100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }

    val sortedItem=allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()
    val result:StringBuilder=new StringBuilder()
    result.append("时间").append(new Timestamp(timestamp-1)).append("\n")
    for(i <- sortedItem.indices){
      val curroutItem=sortedItem(i)
      result.append("No").append(i+1).append(":")
      .append("商品ID=").append(curroutItem.itemId)
        .append("  浏览量=").append(curroutItem.count).append("\n")
    }
    result.append("============")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}