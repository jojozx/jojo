package com.jd.natworkflow.analysis

import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

case class MarketingUserBehavior(UserId:String ,behavior:String ,channel:String ,timestamp: Long)

object  AppmarketingByChannel{
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.)

  }
}
