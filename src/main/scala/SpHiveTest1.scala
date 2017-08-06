import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


case class Record(key: Int, value: String)

object SpHiveApp {
  def main(args: Array[String]) {

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession
    .builder()
    .appName("SpHiveApp")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

//sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
//sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sql("SELECT * FROM twext").show()
// +---+-------+
// |key|  value|
// +---+-------+
// |238|val_238|
// | 86| val_86|
// |311|val_311|
// ...

// Aggregation queries are also supported.
sql("SELECT COUNT(*) FROM twext").show()
// +--------+
// |count(1)|
// +--------+
// |    500 |
// +--------+

// The results of SQL queries are themselves DataFrames and support all normal functions.
val sqlDF = sql("SELECT id_str, text FROM twext limit 10")

// The items in DataFrames are of type Row, which allows you to access each column by ordinal.
val stringsDS = sqlDF.map {
  case Row(id_str: String, text: String) => s"Key: $id_str, Value: $text"
}
stringsDS.show()
// +--------------------+
// |               value|
// +--------------------+
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// ...

// You can also use DataFrames to create temporary views within a SparkSession.
val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
recordsDF.createOrReplaceTempView("records")

// Queries can then join DataFrame data with data stored in Hive.
sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
// +---+------+---+------+
// |key| value|key| value|
// +---+------+---+------+
// |  2| val_2|  2| val_2|
// |  4| val_4|  4| val_4|
// |  5| val_5|  5| val_5|
// ...
  }
}

