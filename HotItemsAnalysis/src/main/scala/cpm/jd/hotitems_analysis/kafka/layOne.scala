package cpm.jd.hotitems_analysis.kafka

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}


case class asd(as: String, sas: Int, timestamp: Long)

object layOne {
  def kafak(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhoset:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafak.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafak.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitem", new SimpleStringSchema(), properties))

    //    stream.addSink(new FlinkKafkaConsumer[String]("as",new SimpleStringSchema(),properties))
    stream.addSink(new FlinkKafkaProducer[String]("qw", new SimpleStringSchema(), properties))

    env.execute("Hot Items Job")
  }
}

