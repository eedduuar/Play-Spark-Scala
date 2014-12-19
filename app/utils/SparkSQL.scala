package utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

case class WordCount(word: String, count: Int)

object SparkSQL {

  def runQuery (query:String, day:String,datetime:String) {
     println("Starting query ...... " )
    val logFile = "s3n://usmc-log-parsed/"+day+"/"+datetime+".json.gz" // Should be some file on your system
    println("s3 file:"+logFile)
    val driverHost = "10.0.3.50"
    val conf = new SparkConf(true) // skip loading external settings
      .setMaster("spark://10.0.3.50:7077") // run locally with enough threads
      .setAppName("SparkPlay")
      .set("spark.logConf", "true")
      .set("spark.driver.host", s"$driverHost")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    val logData = sqlContext.jsonFile(logFile)

    val sTable = "log_test"
    val sTableComodin = "%TABLE%"
    logData.registerTempTable(sTable)

    
    
    
    val result = sqlContext.sql(query.replace(sTableComodin,sTable))
    println("Total Rows : "+result.count() )
    
  }

}
