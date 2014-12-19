package utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

case class WordCount(word: String, count: Int)

object SparkSQL {

  def runQuery (query:String, day:String,datetime:String) {
    val logFile = "s3n://usmc-log-parsed/"+day+"/"+datetime+".json.gz" // Should be some file on your system
    val driverHost = "localhost"
    val conf = new SparkConf(false) // skip loading external settings
      .setMaster("local[4]") // run locally with enough threads
      .setAppName("firstSparkApp")
      .set("spark.logConf", "true")
      .set("spark.driver.host", s"$driverHost")
    val sc = new SparkContext(conf)
    val logData = sc.jsonFile(logFile)

    val sTable = "log_test"
    val sTableComodin = "%TABLE%"
    logData.regiterTempTable(sTable)

    
    val sqlContext = new SQLContext(sc)
    
    val result = sqlContext.sql(query.replace(sTableComodin,sTable))
    println("Total Rows : "+result.count() )
    
  }

}