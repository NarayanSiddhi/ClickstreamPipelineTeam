package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import utils.sparksession

object FileReader{

  //method to read a input file from a folder
  def readConfig(): SparkConf = {
    val config = ConfigFactory.load("application.conf")
    val sparkAppName = config.getString("spark.appName")
    val sparkMaster = config.getString("spark.master")
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  def readDataFrame(): (DataFrame,DataFrame) = {
//    spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)

    val spark=sparksession.sparkSession()

    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")

    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath1)
    val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath2)

    (df1,df2)
  }
}
