import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.SparkConf

object HBase {
  org.apache.log4j.BasicConfigurator.configure()
  val APP_NAME: String = "SparkHbaseJob"
  var HBASE_DB_HOST: String = null
  var HBASE_TABLE: String = null
  var HBASE_COLUMN_FAMILY: String = null

  def main(args: Array[String]) {
    val confi = new SparkConf().setAppName("HBaseNoStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      confi.setMaster("local[*]")
    }

    HBASE_DB_HOST = "127.0.0.1"
    HBASE_TABLE = "b"
    HBASE_COLUMN_FAMILY = "b"

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE)

    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(HBASE_TABLE)))

    val put = new Put(Bytes.toBytes("b"))
    put.addColumn( Bytes.toBytes("b"),  Bytes.toBytes("b"), Bytes.toBytes("f"))
    table.put(put)
  }
}