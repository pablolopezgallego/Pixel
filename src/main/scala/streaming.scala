import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object streaming extends Serializable {
  //implicit val formats = DefaultFormats
  //case class adn(idTracker: String, decay: String, url: String, ip: String, useragent: String, os: String, dispositivo: String, language: String)

  //  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]) {
    /** EL código de spark conf para hacer el streaming */

    val conf = new SparkConf().setAppName("HBaseStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      conf.setMaster("local[*]")
    }

    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    /** KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "localhost:21000",
      "group.id" -> "kafka-example",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    val rutaTax = args(0) //URL a identificador de taxonomía
    //val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = args(1) //Campos que contiene el Json recibido
    val topic = args(2) //Topic Kafka text
    val tabla = args(3) //HBase tabla 'usuarios'
    val columnFamily = args(4) //HBase column family 'adn'
    /*
    val rutaTax ="file:///Pablo/Taxonomias.csv" //URL a identificador de taxonomía
    //val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///Pablo/dictVarSanitas.txt" //Campos que contiene el Json recibido
    val topic = "test" //Topic Kafka text
    val tabla = "usuarios" //HBase tabla 'usuarios'
    val columnFamily = "adn" //HBase column family 'adn'
    */
    val tax = sc.textFile(rutaTax)
    val taxFM = tax.map(x => (x.split(";")(0), x.split(";")(1)))
    val camposGeneralesRDD = sc.textFile(camposGenerales)
    val mapaVarGen: Array[String] = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "")).collect
    //val camposTaxRDD = sc.textFile(camposTax)
    //val mapaTax = camposTaxRDD.map(x => (x.split(";")(0), 0)).collect()

    /** Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map(topic -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    /** Trasformamos la linea de entrada en un Json */
    val traficoRDD: DStream[(String, (String, Array[String]))] = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), (compact(parse(x) \ "idTracker").replaceAll("\"", "") + compact(parse(x) \ "decay").replaceAll("\"", ""), Array(compact(parse(x) \ "ip").replaceAll("\"", ""), compact(parse(x) \ "useragent").replaceAll("\"", ""), compact(parse(x) \ "os").replaceAll("\"", ""), compact(parse(x) \ "dispositivo").replaceAll("\"", ""), compact(parse(x) \ "language").replaceAll("\"", "")))))
    val traficoTax: DStream[(String, Array[String], Array[String])] = traficoRDD.transform(rdd => rdd.join(taxFM).map(x => (x._2._1._1, Array(x._2._2.toString).union(x._2._1._2), Array("idTax" + x._2._2.toString).union(mapaVarGen))))

    /** Llamamos al Put de HBase */
    putHBase(traficoTax)

    def putHBase(partitions: DStream[(String, Array[String], Array[String])]) {
      /** Inicializamos los valores de HBase */
      sc.setLogLevel("ERROR")
      val config = new HBaseConfiguration()
      val hbaseContext = new HBaseContext(sc, config)
      ssc.checkpoint("/tmp/spark_checkpoint")

      /** Hacemos el Put */
      partitions.hbaseBulkPut(
        hbaseContext,
        TableName.valueOf(tabla),
        (row) => {
          //POR CADA COLUMNA REALIZO UN PUT
          val put = new Put(Bytes.toBytes(row._1)) //idTracker
          for (x <- 0 until row._2.length) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(row._3(x)), Bytes.toBytes(row._2(x))) //colFamily,col,timestamp,value
          }
          put
        })
      /*
      partitions.hbaseBulkPut(
        hbaseContext,
        TableName.valueOf("usuarios"),
        (row) => {
          val inc = new Put(Bytes.toBytes(row._1.toString)) //idTracker
          inc.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("idTax"), Bytes.toBytes("1")) //colFamily,col,timestamp,value
          inc
        })
      */
    }
    ssc.start()
    ssc.awaitTermination()
  }
}



