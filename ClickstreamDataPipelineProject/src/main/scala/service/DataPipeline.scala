package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._

object DataPipeline {
  def dataPipeline():(DataFrame,DataFrame)= {

    //execute the pipeline
    val sparkConf = FileReader.readConfig()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = ConfigFactory.load("application.conf").getString("spark.logLevel")
    spark.sparkContext.setLogLevel(logLevel)

    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")

    val df1 = FileReader.readDataFrame(spark, inputPath1)
    val df2 = FileReader.readDataFrame(spark, inputPath2)

    //spark.stop()
    (df1,df2)
  }
}
