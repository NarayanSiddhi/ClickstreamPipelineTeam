package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object FileReader{

  //method to read a input file from a folder
  def readConfig(): SparkConf = {
    val config = ConfigFactory.load("application.conf")
    val sparkAppName = config.getString("spark.appName")
    val sparkMaster = config.getString("spark.master")
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  def readDataFrame(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
  }

//  def main(args: Array[String]) {
//    val spark = SparkSession.builder.master("local[*]").appName("FileReader").getOrCreate()
//    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//    import spark.implicits._
//    import spark.sql
//
//    spark.stop()
//  }
}
