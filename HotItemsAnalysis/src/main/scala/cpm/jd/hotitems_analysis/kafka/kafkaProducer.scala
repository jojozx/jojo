package cpm.jd.hotitems_analysis.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafkaProducerTest {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")

  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    //    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)
    val bufferedSource = io.Source.fromFile("D:\\workspac\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
     producer.close()
  }
}
