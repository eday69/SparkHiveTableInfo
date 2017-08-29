import java.io.File
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SpHiveApp {

  def main(args: Array[String]) {

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  import sqlContext.implicits._

  // The results of SQL queries are themselves DataFrames and support all normal functions.
  val sqlDF = sqlContext.sql("SELECT id_str, text FROM twext limit 10")
  sqlDF.show()
  }
}

