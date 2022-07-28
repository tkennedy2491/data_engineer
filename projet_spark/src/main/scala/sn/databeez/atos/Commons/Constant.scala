package sn.databeez.atos.Commons

import org.apache.spark.sql.SparkSession

object Constant {

  val spark = new SparkSession.Builder()
    .appName("GROUPE2")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
}
