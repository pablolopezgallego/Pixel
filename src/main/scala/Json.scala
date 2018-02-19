
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext

object Json {
  //  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]) {

    /** EL código de spark conf para hacer el streaming */

    val conf = new SparkConf().setAppName("HBaseStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      conf.setMaster("local[*]")
    }

    val ssc = new StreamingContext(conf, Seconds(10))

    /* KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "localhost:21000",
      "group.id" -> "pablo",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /* Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map("test" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    /* Creamos el SqlContext para usar Json */
        val sc = ssc.sparkContext
    val SqlContext = new org.apache.spark.sql.SQLContext(sc)

    /*Importamos taxonomías
    val rutaTax = "file:///C:/Users/plopez/Desktop/Taxonomias.csv" //URL a identificador
    val camposTax = "file:///C:/Users/plopez/Desktop/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///C:/Users/plopez/Desktop/dictVarSanitas.txt" //Campos que contiene el Json recibido
*/
    /* Versión en el servidor, en un futuro se pasará como argumentos */
    val rutaTax = "file:///Pablo/Taxonomias.csv" //URL a identificador
    val camposTax = "file:///Pablo/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///Pablo/dictVarSanitas.txt" //Campos que contiene el Json recibido
    //val rutaTrafico = args(3)    No se usa, la ruta es lo que leemos de kafka, seguramente sea el topic
    //val destino = args(4)  Es HBase, aún desconocemos que meteremos al final, seguramente el HBase column adn y tabla

    val tax = sc.textFile(rutaTax, 1)
    val taxFM = tax.map(x => (x.split(";")(0), x.split(";")(1)))
    val camposTaxRDD = sc.textFile(camposTax, 1)
    val camposGeneralesRDD = sc.textFile(camposGenerales, 1)

    val mapaTax = camposTaxRDD.map(x => (x.split(";")(0), 0)).collect()
    val mapaVarGen = camposGeneralesRDD.map(x => x.replaceAll("parametros.", "").collect

    //mapaTax.foreach(println)

    sc.setLogLevel("ERROR")
    val config = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, config)
    ssc.checkpoint("/tmp/spark_checkpoint")

//    lines.print()
//    val trafico = lines.flatMap(k=>k.slice(k.lastIndexOf("idTracker")+1,k.indexOf("url")-2))
//    trafico.foreachRDD(k=>println(k.collect()(0) + k.collect()(1) ))

    lines.foreachRDD { k =>
      if (k.count() > 0) {
        /*Traducimos el Json a RDD */
        val traficoRDD = SqlContext.read.json(k).selectExpr(List("idTracker", "decay", "url") ++ camposGeneralesRDD.collect(): _*).rdd.keyBy(t => if (t.getAs[String]("url").indexOf('?') > 0) t.getAs[String]("url").substring(0, t.getAs[String]("url").indexOf('?')) else t.getAs[String]("url"))
        var traficoTax = traficoRDD.join(taxFM).map(x => (x._2._1.getAs("idTracker").toString(), x._2))

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
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes(HBASE_TABLE))) //nombre tabla

        var csv = traficoTax.map(l => (l._2._1, l._2._2))
        //traficoTax.collect().foreach(l => println { "traficoTax: " + l})
        //traficoRDD.collect().foreach(l => println {"traficoRDD: " + l})
        csv.collect().foreach(l => println {"csv: " + l})

        csv.collect().foreach( aux => {
        //            for (aux<-csv){
            val p = new Put(Bytes.toBytes(aux._1.getAs("idTracker").toString()))
            for ((key,v) <- mapaVarGen) {
              p.addColumn( Bytes.toBytes("adn"),  Bytes.toBytes(""+key), aux._1.getAs("decay"), Bytes.toBytes(aux._1.getAs(""+key).toString))
              table.put(p)
            }
            val inc = new Increment(Bytes.toBytes(aux._1.getAs("idTracker").toString())) //key
            inc.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("idTax"+aux._2.toString), 1) //ColFamily,col,incremento
            table.increment(inc)
          })

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
Json.main(null)

