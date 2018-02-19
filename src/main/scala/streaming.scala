import java.text.SimpleDateFormat
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, _}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object streaming {

  implicit val formats = DefaultFormats

  case class adn(idTracker: String, decay: String, url: String, ip:String, useragent: String, os: String, dispositivo: String, language: String)

  case class evar7(evar7: String)

  //  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]) {

    /** EL código de spark conf para hacer el streaming */
        val conf = new SparkConf().setAppName("HBaseStream")
        if (sys.env("ENTORNO") == "DESARROLLO") {
          conf.setMaster("local[*]")
        }

    val ssc = new StreamingContext(conf, Seconds(5))

    /* KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "localhost:21000",
      "group.id" -> "kafka-example",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /* Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map("test" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    /* Creamos el sparkContext para usar Json */
        val sc = ssc.sparkContext

    val rutaTax = "file:///Pablo/Taxonomias.csv" //URL a identificador
    val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///Pablo/dictVarSanitas.txt" //Campos que contiene el Json recibido
    //    val rutaTrafico = args(3)    No se usa, la ruta es lo que leemos de kafka, seguramente sea el topic
    //    val destino = args(4)  Es HBase, aún desconocemos que meteremos al final, seguramente el HBase colum adn y tabla

    val tax = sc.textFile(rutaTax, 1)
    val taxFM = tax.map(x => (x.split(";")(0), x.split(";")(1)))
    val camposTaxRDD = sc.textFile(camposTax, 1)
    val camposGeneralesRDD = sc.textFile(camposGenerales, 1)

    val mapaTax = camposTaxRDD.map(x => (x.split(";")(0), 0)).collect()
    val mapaVarGen = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "")).collect()

    lines.print()

    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        /*Traducimos el Json a RDD */
        val traficoRDD = rdd.collect()

        var aux: Array[String] = null
        for (i <- traficoRDD) {
          aux = parser(i)
          aux.foreach(println)
          putHBase(aux)
        }
        /*
                var traficoTax = traficoRDD(x => Array(x._2._1(0).toString(), "idTax" + x._2._2, x._2._1(2).toString, x._2._1(3).toString, x._2._1(4).toString, x._2._1(5).toString, x._2._1(6).toString, x._2._1(7).toString)) //(idTracker,TimeStamp,idTax,Url...etc)

                println(traficoTax.collect())
                traficoTax.foreachPartition(putHBase)
                */
      }
    }

    def parser(json: String): Array[String] = {
      val parsedJson = parse(json)
      val result = parsedJson.extract[adn]
      //val parametros = parsedJson.extract[parametros]

      return Array(result.idTracker, result.decay, result.url, result.ip, result.useragent, result.os, result.dispositivo, result.language)
    }

    def putHBase(partitions: Array[String]): Unit = {
      var row = partitions //Array[Strings]

      val config = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(sc, config)
      ssc.checkpoint("/tmp/spark_checkpoint")

      val APP_NAME: String = "SparkHbaseJob"
      var HBASE_DB_HOST: String = null
      var HBASE_TABLE: String = null
      var HBASE_COLUMN_FAMILY: String = null

      HBASE_DB_HOST = "127.0.0.1"
      HBASE_TABLE = "usuarios"
      HBASE_COLUMN_FAMILY = "adn"

      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE)
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf(Bytes.toBytes(HBASE_TABLE)))

      //POR CADA COLUMNA REALIZO UN PUT
      val put = new Put(Bytes.toBytes(row(0).toString)) //idTracker
      for (j <- 0 until mapaVarGen.length) {
        put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes(""+mapaVarGen(j)), Bytes.toBytes(row(j+3))) //colFamily,col,timestamp,value
        table.put(put)
      }
      val inc = new Increment(Bytes.toBytes(row(0).toString)) //idTracker
      inc.addColumn(Bytes.toBytes("adn"), Bytes.toBytes(row(2)), 1) //colFamily,col,timestamp,value
      table.increment(inc)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

streaming.main(null)



