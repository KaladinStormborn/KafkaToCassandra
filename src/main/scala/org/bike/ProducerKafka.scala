package org.bike

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source

object ProducerKafka extends App {
  val SERIALIZER = (new StringSerializer).getClass.getCanonicalName

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER)

  val producer = new KafkaProducer[String, String](props)
  val topic = "trip-events-topic"

  val source = Source.fromFile("citibike-tripdata.csv")

  try {
    // drop the header and send each line of the file every 10 seconds
    for (line <- source.getLines().drop(1)) {

      val record = new ProducerRecord[String, String](topic, line)
      Thread.sleep(10000)
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
    source.close()
  }
}

