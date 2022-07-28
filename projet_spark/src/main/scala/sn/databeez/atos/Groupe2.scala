package sn.databeez.atos

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import sn.databeez.atos.Commons.Utils.{readCsv, saveAsTable}

object Groupe2 {
  def main(args: Array[String]): Unit = {
    val CONF_FILE_PATH = args(0)
    val conf : Config = ConfigFactory.parseFile(new File(s"$CONF_FILE_PATH"))
    //val conf: Config = ConfigFactory.parseFile(new File("C:\\Users\\a853183\\Documents\\PROJETSPARK\\EXAMENGROUPE2\\src\\main\\resources\\variant-covid.json"))
    //val conf: Config = ConfigFactory.parseFile(new File("/home/databeez_atos/ressources/variant-covid.json"))
    /** let's get the value from conf file */
    val FLUX_PAYS = conf.getString("FLUX_METADATA.FLUX_PAYS")
    val FLUX_NAME = conf.getString("FLUX_METADATA.FLUX_NAME")
    val HDFS_SOURCE_PATH = conf.getString("FLUX_METADATA.HDFS_SOURCE_PATH")
    val FILE_DELIMITER = conf.getString("FLUX_METADATA.FILE_DELIMITER")
    val FLUX_COLUMNS = conf.getString("FLUX_METADATA.FLUX_COLUMNS")
    val entete = FLUX_COLUMNS.split(",") // pour recuperer l'entete sous forme de liste
    // avec spark.read lire le repertoire contenant la donnée et utilser la
    var covid = readCsv(HDFS_SOURCE_PATH, FILE_DELIMITER, true, "csv").toDF(entete: _*)
    covid.show(5)
    /** Database metadata */
    val DATABASE_NAME = conf.getString("DATABASE_METADATA.DATABASE_NAME")
    val TABLE_NAME = conf.getString("DATABASE_METADATA.TABLE_NAME")
    val TABLE_PATH = conf.getString("DATABASE_METADATA.TABLE_PATH")
    val SAVE_DATABASE_FORMAT = conf.getString("DATABASE_METADATA.SAVE_DATABASE_FORMAT")
    val SAVE_DATABASE_MODE = conf.getString("DATABASE_METADATA.SAVE_DATABASE_MODE")
    saveAsTable(covid, DATABASE_NAME, TABLE_NAME, TABLE_PATH, SAVE_DATABASE_MODE, SAVE_DATABASE_FORMAT)
  }
}
