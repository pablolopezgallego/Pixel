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

object streaming{
  //case class adn(idTracker: String, decay: String, url: String, ip: String, useragent: String, os: String, dispositivo: String, language: String)
  def main(args: Array[String]) {
    /** EL código de spark conf para hacer el streaming */
    val conf = new SparkConf().setAppName("HBaseStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      conf.setMaster("local[*]")
    }
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    val rutaTax = args(0) //URL a identificador de taxonomía
    val camposGenerales = args(1) //Campos que contiene el Json recibido
    val topic = args(2) //Topic Kafka text
    val grupo = topic+"Streaming"
    val tabla = args(3) //HBase tabla 'usuarios'
    val columnFamily = args(4) //HBase column family 'adn'

    /** KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "localhost:21000",
      "group.id" -> grupo,
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /** Tratamos los ficheros de datos */
    val tax = sc.textFile(rutaTax, 1)
    val taxFM: RDD[(String, String)] = tax.map(x => (x.split(";")(0), x.split(";")(1))) //Taxonomías, un url corresponde a un int
    val camposGeneralesRDD = sc.textFile(camposGenerales, 1)
    val mapaVarGen: Array[String] = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "")).collect //Columnas del Json que debemos usar

    /** Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map(topic -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)
    lines.print()

    /** Trasformamos la linea de entrada que es un Json */
    val traficoRDD = lines.map(x => (compact(parse(x) \ "url").replaceAll("\"", ""), (compact(parse(x) \ "idTracker").replaceAll("\"", ""), Array(compact(parse(x) \ "ip").replaceAll("\"", ""), compact(parse(x) \ "useragent").replaceAll("\"", ""), compact(parse(x) \ "os").replaceAll("\"", ""), compact(parse(x) \ "dispositivo").replaceAll("\"", ""), compact(parse(x) \ "language").replaceAll("\"", "")))))
    val traficoTax: DStream[(String, Array[String], Array[String])] = traficoRDD.transform(rdd => rdd.join(taxFM).map(x => (x._2._1._1, Array(x._2._2.toString).union(x._2._1._2), Array("idTax" + x._2._2.toString).union(mapaVarGen))))
    val traficoIdTracker = traficoTax.map(x => Bytes.toBytes(x._1))

    /** Inicializamos los valores para llamar a HBase */
    sc.setLogLevel("ERROR")
    val config = new HBaseConfiguration()
    val hbaseContext = new HBaseContext(sc, config)
    ssc.checkpoint("/tmp/spark_checkpoint")

    /** Variables para obtener datos de las funciones a las qiue llamamos */
    var dataGet: DStream[Array[(String, String)]] = null
    var dataRecomendacion: DStream[(String, Int)] = null
    dataGet = getData(traficoIdTracker)
    dataGet.foreachRDD(x=>{ x.foreachPartition(y=>{ if(y.hasNext) y.next.foreach(println)})})
    dataRecomendacion = recomendacion(traficoTax, dataGet)
    putDataPixel(traficoTax)
    putDataRecomendacion(dataRecomendacion)

    /** Hacemos el get y la recomendación, devolvemos idTracker, int */
    def getData(rowStream: DStream[Array[Byte]]): DStream[Array[(String, String)]] = {
      val dataGet2 =
        rowStream.transform(rdd => {
          val getRdd = hbaseContext.bulkGet[Array[Byte], Array[(String, String)]](
            TableName.valueOf("operacional" + tabla),
            2, // *************************************************************Estudiar más adelante cual debería ser el tamaño del bach
            rdd,
            record => {
              new Get(record)
            },
            (result: Result) => {
              val it = result.listCells().iterator()
              var b: Array[(String, String)] = Array(("idTracker", "Bytes.toString(result.getRow)"))
              //print("Esto es b" + b.foreach(print))

              while (it.hasNext) {
                val cell = it.next()
                val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                if (q.equals("counter")) {
                  b = b.union(Array((q, Bytes.toLong(CellUtil.cloneValue(cell)).toString())))
                } else {
                  b = b.union(Array((q, Bytes.toString(CellUtil.cloneValue(cell)))))
                }
              }
              b
            })

          getRdd
        })
      dataGet2
    }

    /** Generamos la recomendación */
    def recomendacion(dataTransaccioal: DStream[(String, Array[String], Array[String])], dataOperacional: DStream[Array[(String, String)]]): DStream[(String, Int)] = {

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
          //INSERTO CADA COLUMNA Y REALIZO UN ÚNICO PUT
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
          val put = new Put(Bytes.toBytes(row._1)) //idTracker
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("recomendacion"), Bytes.toBytes(row._2.toString)) //colFamily,col,timestamp,value
          put
        })
    }
    ssc.start()
    ssc.awaitTermination()
  }
}


