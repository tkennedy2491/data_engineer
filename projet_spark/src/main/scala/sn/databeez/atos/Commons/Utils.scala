package sn.databeez.atos.Commons

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import sn.databeez.atos.Commons.Constant._

object Utils {
  def readCsv(filepath:String,sep:String,head:Boolean,format:String): DataFrame ={
     spark.read.format(format)
          .option("header", head)
          .option("delimiter",sep)
          .load(filepath)
  }
  def readCsvFull(filepath:String,sep:String,head:Boolean,format:String): DataFrame ={
    spark.read.format(format)
      .option("header", head)
      .option("delimiter",sep)
      .option("ignoreTrailingWhiteSpace",true)
      .option("ignoreLeadingWhiteSpace",true)
      .option("quote","\"")
      .load(filepath).na.drop().dropDuplicates()
  }

  def saveAsTable(df: DataFrame, dataBaseName: String, tableName: String, path: String, mode: String, format: String): Unit = {
    df.write
      .format(s"$format") // format = parquet | orc …
      .mode(s"$mode") // mode = append | overwrite | ignore | errorifexists
      .option("path",s"$path")
      .saveAsTable(s"$dataBaseName.$tableName")
  }




}
