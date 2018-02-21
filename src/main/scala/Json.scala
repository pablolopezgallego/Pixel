import java.text.SimpleDateFormat
import java.util.HashMap
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
//import org.apache.hadoop.hbase.{HConstants, CellUtil, HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkContext, Logging}
import org.junit.rules.TemporaryFolder
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.exceptions._
import org.apache.hadoop.hbase.CellComparator
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, _}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
    /*
            val conf = new SparkConf().setAppName("HBaseStream")
            if (sys.env("ENTORNO") == "DESARROLLO") {
              conf.setMaster("local[*]")
            }
    */
    val ssc = new StreamingContext(sc, Seconds(1))
    //val sc = ssc.sparkContext

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
    //    val traficoRDD = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), Array(for (y<-mapaVarGen) compact(parse(x) \ y).toString.replaceAll("\"", ""))))
    //val traficoRDD: DStream[(String, Array[String])] = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), Array(compact(parse(x) \ "idTracker"), compact(parse(x) \ "decay"), compact(parse(x) \ "ip"), compact(parse(x) \ "useragent"), compact(parse(x) \ "os"), compact(parse(x) \ "dispositivo"), compact(parse(x) \ "language"))))
    val traficoRD: DStream[Array[String]] = lines.map(x =>Array(compact(parse(x) \ "idTracker"), compact(parse(x) \ "decay"), compact(parse(x) \ "ip"), compact(parse(x) \ "useragent"), compact(parse(x) \ "os"), compact(parse(x) \ "dispositivo"), compact(parse(x) \ "language")))

    //traficoRDD.foreachRDD(x=>x.foreach(y=>print("Hola"+y._2)))
    //val traficoTax: DStream[Array[String]] = traficoRDD.transform(rdd => rdd.join(taxFM).map(x => Array(x._2._2.toString).union(x._2._1))) //.union(x._2._2.toString)))
    //traficoTax.print()

    /** Inicializamos los valores de HBase */
    HBASE_DB_HOST = "127.0.0.1"
    HBASE_TABLE = "usuarios"
    HBASE_COLUMN_FAMILY = "adn"
    //    ssc.checkpoint("/tmp/spark_checkpoint")
    //traficoTax.foreachRDD { rdd => rdd.foreachPartition(x => putHBase(x, mapaVarGen)) }
    //traficoTax.foreachRDD(aux => {for (x <- aux.collect) putHBase(x, mapaVarGen)})
    putHBase(traficoRD, mapaVarGen)

    //def putHBase(partitions: Iterator[(Array[String])], mapaVarGen: (Array[String])) {
    def putHBase(partitions: DStream[(Array[String])], mapaVarGen: (Array[String])) {
      sc.setLogLevel("ERROR")
      val config = new HBaseConfiguration()
      val hbaseContext = new HBaseContext(sc, config)
      ssc.checkpoint("/tmp/spark_checkpoint")
      //      if (!partitions.isEmpty) {
      partitions.hbaseBulkPut(
        hbaseContext,
        TableName.valueOf("b"),
        (row) => {
          //.next()
          //POR CADA COLUMNA REALIZO UN PUT
          val put = new Put(Bytes.toBytes("b")) //idTracker
          put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("b")) //colFamily,col,timestamp,value
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


import java.util.HashMap
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HConstants, CellUtil, HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkContext, Logging}
import org.junit.rules.TemporaryFolder
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.exceptions._
import org.apache.hadoop.hbase.CellComparator

sc.setLogLevel("ERROR")
val config = new HBaseConfiguration()
val hbaseContext = new HBaseContext(sc, config)
val ssc = new StreamingContext(sc, Seconds(10))
ssc.checkpoint("/tmp/spark_checkpoint")


val lines = KafkaUtils.createStream(ssc, "localhost:21000", "grupo", Map(("test",1))).map(_._2)
val csv = lines.map(x=>x.split(","))
//144,ALICANTE,20170117,4.8,7.2
csv.hbaseBulkPut(
  hbaseContext,
  TableName.valueOf("meteoHBase"),
  (putRecord) => {
    val put = new Put(Bytes.toBytes(putRecord(0)))
    put.addColumn( Bytes.toBytes("datos"),  Bytes.toBytes("provincia"), Bytes.toBytes(putRecord(1)))
    put.addColumn( Bytes.toBytes("datos"),  Bytes.toBytes("fecha"), Bytes.toBytes(putRecord(2)))
    put.addColumn( Bytes.toBytes("datos"),  Bytes.toBytes("precipitacion"), Bytes.toBytes(putRecord(3)))
    put.addColumn( Bytes.toBytes("datos"),  Bytes.toBytes("temperatura"), Bytes.toBytes(putRecord(4)))
    put
  })
ssc.start()
ssc.awaitTermination()
