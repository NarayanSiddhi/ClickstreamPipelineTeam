package utils

import com.typesafe.config.ConfigFactory
import service.FileReader
import org.apache.spark.sql._

object sparksession {
  def sparkSession():SparkSession= {
    val sparkConf = FileReader.readConfig()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = ConfigFactory.load("application.conf").getString("spark.logLevel")
    spark.sparkContext.setLogLevel(logLevel)
  spark
  }
}
