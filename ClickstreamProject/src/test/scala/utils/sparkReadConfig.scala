package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import service.FileReader

object sparkReadConfig {
  def sparkSession():SparkSession= {
    // Creating a SparkSession
    val sparkConf = readConfig()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = ConfigFactory.load("test_application.conf").getString("spark.logLevel")
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  def readConfig(): SparkConf = {
    // Reading the configuration file for the test cases
    val config = ConfigFactory.load("test_application.conf")
    val sparkAppName = config.getString("spark.appName")
    val sparkMaster = config.getString("spark.master")
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  def readTestDataframe(): (DataFrame,DataFrame)={
    // Reading the test or sample dataframes
    val spark = sparkSession()

    val inputPath_clickstream = ConfigFactory.load("test_application.conf").getString("input.sample_path1")
    val inputPath_itemset = ConfigFactory.load("test_application.conf").getString("input.sample_path2")

    val clickstream_test_DF = FileReader.readDataFrame(spark, inputPath_clickstream)
    val itemset_test_DF = FileReader.readDataFrame(spark, inputPath_itemset)

    (clickstream_test_DF,itemset_test_DF)
  }
}
