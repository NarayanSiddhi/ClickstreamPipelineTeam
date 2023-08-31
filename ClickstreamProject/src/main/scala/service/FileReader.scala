package service

import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object FileReader extends Logging {
  //method to read the input csv file
  def readDataFrame(spark:SparkSession, inputpath:String): DataFrame= {
    try {
      // creating a dataframe that is reading the file
      val dataframe = spark.read.option("header", "true").option("inferSchema", "true").csv(inputpath)
      dataframe
    } catch {
      case e: Exception =>
        logInfo("An error occurred during reading the files",e)
        // returning null dataframe if the file is not read as the data is not present or the input file path is incorrect
        null
    }
  }
}
