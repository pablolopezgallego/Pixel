import java.text.SimpleDateFormat
import java.util.HashMap
import org.junit.rules.TemporaryFolder
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.exceptions._
import org.apache.hadoop.hbase.CellComparator
import org.apache.hadoop.hbase.client.{ConnectionFactory, _}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
//import org.apache.hadoop.hbase.{HConstants, CellUtil, HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object streaming extends Serializable {
  implicit val formats = DefaultFormats
  var HBASE_DB_HOST: String = null
  var HBASE_TABLE: String = null
  var HBASE_COLUMN_FAMILY: String = null

  case class adn(idTracker: String, decay: String, url: String, ip: String, useragent: String, os: String, dispositivo: String, language: String)

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

    /** Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map("test" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

      val rutaTax = "file:///Pablo/Taxonomias.csv" //URL a identificador
      //val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
      val camposGenerales = "file:///Pablo/dictVarSanitas.txt" //Campos que contiene el Json recibido
      //val rutaTrafico = args(3)    No se usa, la ruta es lo que leemos de kafka, seguramente sea el topic
      //val destino = args(4)  Es HBase, aún desconocemos que meteremos al final, seguramente el HBase colum adn y tabla
      val tax = sc.textFile(rutaTax)
      val taxFM = tax.map(x => (x.split(";")(0), x.split(";")(1)))
      //val camposTaxRDD = sc.textFile(camposTax)
      val camposGeneralesRDD = sc.textFile(camposGenerales)
      //val mapaTax = camposTaxRDD.map(x => (x.split(";")(0), 0)).collect()
      val mapaVarGen: Array[String] = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "")).collect
      //lines.print()

      /** Trasformamos la linea de entrada en un Json */
      //val traficoRDD = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), Array(for (y<-mapaVarGen) compact(parse(x) \ y).toString.replaceAll("\"", ""))))
      val traficoRDD: DStream[(String, (String, Array[String]))] = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), (compact(parse(x) \ "idTracker").replaceAll("\"", "") + compact(parse(x) \ "decay").replaceAll("\"", ""), Array(compact(parse(x) \ "ip").replaceAll("\"", ""), compact(parse(x) \ "useragent").replaceAll("\"", ""), compact(parse(x) \ "os").replaceAll("\"", ""), compact(parse(x) \ "dispositivo").replaceAll("\"", ""), compact(parse(x) \ "language").replaceAll("\"", "")))))
      val traficoTax: DStream[(String, Array[String], Array[String])] = traficoRDD.transform(rdd => rdd.join(taxFM).map(x => (x._2._1._1, Array(x._2._2.toString).union(x._2._1._2), Array("idTax"+x._2._2.toString).union(mapaVarGen))))
      //val traficoFinal: DStream[(String, Array[String], Array[String])] = traficoTax.map(x => (x._1, x._2, Array("idTax").union(mapaVarGen)))

    putHBase(traficoTax)

      /** Inicializamos los valores de HBase */
      HBASE_DB_HOST = "127.0.0.1"
      HBASE_TABLE = "usuarios"
      HBASE_COLUMN_FAMILY = "adn"
      //    ssc.checkpoint("/tmp/spark_checkpoint")
      //traficoTax.foreachRDD { rdd => rdd.foreachPartition(x => putHBase(x, mapaVarGen)) }
      //traficoTax.foreachRDD(aux => {for (x <- aux.collect) putHBase(x, mapaVarGen)})

    //def putHBase(partitions: Iterator[(Array[String])], mapaVarGen: (Array[String])) {
    def putHBase(partitions: DStream[(String, Array[String], Array[String])]) {
      sc.setLogLevel("ERROR")
      val config = new HBaseConfiguration()
      val hbaseContext = new HBaseContext(sc, config)
      ssc.checkpoint("/tmp/spark_checkpoint")
      //      if (!partitions.isEmpty) {

      partitions.hbaseBulkPut(
        hbaseContext,
        TableName.valueOf("usuarios"),
        (row) => {
          //POR CADA COLUMNA REALIZO UN PUT
          val put = new Put(Bytes.toBytes(row._1)) //idTracker
          for (x <- 0 until row._2.length) {
            put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes(row._3(x)), Bytes.toBytes(row._2(x))) //colFamily,col,timestamp,value
          }
          put
          /*
        val inc = new Increment(Bytes.toBytes(row(1).toString)) //idTracker
        inc.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("idTax" + row(0).toString), 1) //colFamily,col,timestamp,value
        table.increment(inc)
        */
        })
    }
    //    }
    ssc.start()
    ssc.awaitTermination()
  }
}

streaming.main(null)



