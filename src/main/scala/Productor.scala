import java.util.Properties
import org.apache.kafka.clients.producer._

object Productor extends App {
  org.apache.log4j.BasicConfigurator.configure()
  val  props = new Properties()
  props.put("bootstrap.servers", "51.255.74.114:42111") //Produzco directamente a kafka, no a zookeeper, si se puede en un futuro intento cambiarlo
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //Para pasar la Key
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //Para pasar el value

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test2" //El topic que vamos a usar

   //Bucle de prueba para enviar siempre los mismos datos
    val record = new ProducerRecord(TOPIC, "key", "{\"idTracker\":\"09013f4b164366c73d57f9f1e43c4a33\",\"ip\":\"::ffff:159.147.124.253\",\"url\":\"http://www.clinicalondres.es/tratamientos-tienes-40-anos-promociones-clinicalondres\",\"parametros\":{\"referer\":\"\",\"evar7\":\"\",\"evar39\":\"\",\"evar49\":\"\"},\"decay\":1513211004544,\"useragent\":\"Chrome\",\"os\":\"Windows\",\"dispositivo\":\"PC\",\"language\":\"es-ES\"}")
    val record2 = new ProducerRecord(TOPIC, "key", "{\"idTracker\":\"1198f97c106e576491f06d5beb3581bc\",\"ip\":\"::ffff:201.204.94.191\",\"url\":\"http://www.clinicalondres.es/testimonios\",\"parametros\":{\"referer\":\"https://www.google.com/\",\"evar7\":\"\",\"evar39\":\"\",\"evar49\":\"\"},\"decay\":1513214546032,\"useragent\":\"Chrome\",\"os\":\"Android\",\"dispositivo\":\"Movil\",\"language\":\"es-419\"}")

    Thread.sleep(5000)
    producer.send(record)
    producer.send(record2)
    //println("key"+" hello")


  producer.close()
}