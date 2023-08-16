package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import transform.ConvertToLowercase
import utils.sparksession

object DataPipeline {
  def dataPipeline(): Unit = {

    //execute the pipeline
//    val sparkConf = FileReader.readConfig()
    //seperate utils package
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    val logLevel = ConfigFactory.load("application.conf").getString("spark.logLevel")
//    spark.sparkContext.setLogLevel(logLevel)

    //    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
    //    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")
    //
    //    val df1 = FileReader.readDataFrame(spark, inputPath1)
    //    val df2 = FileReader.readDataFrame(spark, inputPath2)

    //sparksession.sparkSession()

    FileWriter.fileWriter()
    //spark.stop()

  }
}


//git ignore file

//for every class in main scala there should be a corresponding test class and test methods in test/scala


//give meaningful names to dataframes