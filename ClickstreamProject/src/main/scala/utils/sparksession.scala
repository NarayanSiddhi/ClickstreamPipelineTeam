package utils

import com.typesafe.config.Config
import constants.ApplicationConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object sparksession extends Logging {
  def sparkSession(sparkConf:SparkConf, conf:Config, appConstants: ApplicationConstants):SparkSession= {
    // creates a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = conf.getString(appConstants.SPARK_LOGLEVEL)
    spark.sparkContext.setLogLevel(logLevel)
    logInfo(s"This is $spark")

    spark
  }
}
