import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import service.DataPipeline

object ClickstreamPipeLine {
//  //to read application config file
//  def readConfig(): SparkConf = {
//    val config = ConfigFactory.load("application.conf")
//    val sparkAppName = config.getString("spark.appName")
//    val sparkMaster = config.getString("spark.master")
//    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
//  }
  def main(args: Array[String]): Unit = {
    try
    { // main class where the flow of the code starts

      // call DataPipeline object
      DataPipeline.dataPipeline()
    }
    catch {
      case e: Exception=>
        DataPipeline.logger.error("An error occured due to failure of main function ClickstreamPipeline.",e)
    }
  }


}

// add exception handling
// catch file reader print log message