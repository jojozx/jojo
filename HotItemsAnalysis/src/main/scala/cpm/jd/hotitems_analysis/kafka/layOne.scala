package cpm.jd.hotitems_analysis.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object layOne {
 def kafak (): Unit ={
   val env =StreamExecutionEnvironment.getExecutionEnvironment
   env.setParallelism(1)
    val properties=new Properties()
   properties.setProperty("bootstrap.servers","localhoset:9092")
   properties.setProperty( "group.id","consumer-group")
   properties.setProperty("key.deserializer","org.apache.kafak.common.serialization.StringDeserializer")
   properties.setProperty("value.deserializer","org.apache.kafak.common.serialization.StringDeserializer")
   properties.setProperty("auto.offset.reset","latest")
   val dataStream=env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
     .map(
     data =>{
       val dataArray=data.split(",")
     }
   )
 }
}
