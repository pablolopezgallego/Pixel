import java.net.InetSocketAddress
import java.text.SimpleDateFormat

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName.valueOf
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object streaming {
  //case class adn(idTracker: String, decay: String, url: String, ip: String, useragent: String, os: String, dispositivo: String, language: String)
  //org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]) {
    /** EL código de spark conf para hacer el streaming */

    val conf = new SparkConf().setAppName("HBaseStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      conf.setMaster("local[*]")
    }
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    val rutaTax = "file:///C:/Users/plopez/Desktop/Taxonomias.csv" //URL a identificador de taxonomía
    //val camposTax = "file:///C:/Users/plopez/Desktop/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///C:/Users/plopez/Desktop/dictVarSanitas.txt" //Campos que contiene el Json recibido
    val topic = "test2" //Topic Kafka text
    val tabla = "usuarios" //HBase tabla 'usuarios'
    val columnFamily = "adn" //HBase column family 'adn'
    /*
    val ssc = new StreamingContext(sc, Seconds(1))
    val rutaTax = "file:///Pablo/Taxonomias.csv" //URL a identificador de taxonomía
    //val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///Pablo/dictVarSanitas.txt" //Campos que contiene el Json recibido
    val topic = "cliente2" //Topic Kafka text
    val tabla = "usuarios" //HBase tabla 'usuarios'
    val columnFamily = "adn" //HBase column family 'adn'
*/
    /*
    val rutaTax = args(0) //URL a identificador de taxonomía
    //val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = args(1) //Campos que contiene el Json recibido
    val topic = args(2) //Topic Kafka text
    val tabla = args(3) //HBase tabla 'usuarios'
    val columnFamily = args(4) //HBase column family 'adn'
    */
    /** KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "51.255.74.114:21000",
      "group.id" -> "kafka-example",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /** Tratamos los ficheros de datos */
    val tax = sc.textFile(rutaTax)
    val taxFM: RDD[(String, String)] = tax.map(x => (x.split(";")(0), x.split(";")(1))) //Taxonomías, un url corresponde a un int
    val camposGeneralesRDD = sc.textFile(camposGenerales)
    val mapaVarGen: Array[String] = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "")).collect //Columnas del Json que debemos usar
    //val camposTaxRDD = sc.textFile(camposTax)
    //val mapaTax = camposTaxRDD.map(x => (x.split(";")(0), 0)).collect()

    /** Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map(topic -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)
    lines.print()

    /** Trasformamos la linea de entrada que es un Json */
    //val dateFormat =  new SimpleDateFormat("yyyyMMdd")
    // + dateFormat.format(compact(parse(x) \ "decay").replaceAll("\"", "")).toString
    val traficoRDD = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), (compact(parse(x) \ "idTracker").replaceAll("\"", ""), Array(compact(parse(x) \ "ip").replaceAll("\"", ""), compact(parse(x) \ "useragent").replaceAll("\"", ""), compact(parse(x) \ "os").replaceAll("\"", ""), compact(parse(x) \ "dispositivo").replaceAll("\"", ""), compact(parse(x) \ "language").replaceAll("\"", "")))))
    val traficoTax: DStream[(String, Array[String], Array[String])] = traficoRDD.transform(rdd => rdd.join(taxFM).map(x => (x._2._1._1, Array(x._2._2.toString).union(x._2._1._2), Array("idTax" + x._2._2.toString).union(mapaVarGen))))
    val traficoIdTracker = traficoTax.map(x => Bytes.toBytes(x._1))
    /** Inicializamos los valores para llamar a HBase */
    sc.setLogLevel("ERROR")
    val config = new HBaseConfiguration()
    val hbaseContext = new HBaseContext(sc, config)
    ssc.checkpoint("/tmp/spark_checkpoint")

    /** Variables para obtener datos de las funciones a las qiue llamamos */
    var dataGet: DStream[String] = null
    var dataRecomendacion: DStream[(String, Int)] = null
    // val r = scala.util.Random No lo usamos porque no es paralelizable
    dataGet = getData(traficoIdTracker)
    //dataRecomendacion = recomendacion(traficoTax, dataGet)
    //putDataPixel(traficoTax)
    //putDataRecomendacion(dataRecomendacion)

    /** Hacemos el get y la recomendación, devolvemos idTracker, int */
    def getData(rowStream: DStream[Array[Byte]]): DStream[String] = {
      val dataGet2 =
        rowStream.transform(rdd => {
          val getRdd = hbaseContext.bulkGet[Array[Byte], String](
            TableName.valueOf("operacional" + tabla),
            2, // *************************************************************Estudiar más adelante cual debería ser el tamaño del bach
            rdd,
            record => {
              //System.out.println("making Get")
              new Get(record)
            },
            (result: Result) => {
              //println(result)
              val it = result.listCells().iterator()
              val b = new StringBuilder
              b.append(Bytes.toString(result.getRow) + "|")

              while (it.hasNext) {
                val cell = it.next()
                val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                if (q.equals("counter")) {
                  b.append(q + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ";")
                } else {
                  b.append(q + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ";")
                }
              }
              b.toString()
            })
          //getRdd.collect().foreach(v => println(v))
          getRdd
        })
      dataGet2.print()
      dataGet2
    }

    /** Generamos la recomendación */
    def recomendacion(dataTransaccioal: DStream[(String, Array[String], Array[String])], dataOperacional: DStream[String]): DStream[(String, Int)] = {

      /** Aquí se implementará la recomendación
        *
        * Falta modelo
        *
        */

      dataTransaccioal.map(x => (x._1, 5))
    }

    /** Hacemos el Put de Pixel */
    def putDataPixel(dataStream: DStream[(String, Array[String], Array[String])]) {
      dataStream.hbaseBulkPut(
        hbaseContext,
        valueOf("transaccional" + tabla),
        (row) => {
          //POR CADA COLUMNA REALIZO UN PUT
          val put = new Put(Bytes.toBytes(row._1)) //idTracker
          for (x <- 0 until row._2.length) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(row._3(x)), Bytes.toBytes(row._2(x))) //colFamily,col,timestamp,value
          }
          put
        })
    }

    /** Hacemos el Put de la recomendación */
    def putDataRecomendacion(dataStream: DStream[(String, Int)]) {
      dataStream.hbaseBulkPut(
        hbaseContext,
        valueOf("transaccional" + tabla),
        (row) => {
          //POR CADA COLUMNA REALIZO UN PUT
          val put = new Put(Bytes.toBytes(row._1)) //idTracker
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("recomendacion"), Bytes.toBytes(row._2.toString)) //colFamily,col,timestamp,value
          put
        })
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
//streaming.main(null)


