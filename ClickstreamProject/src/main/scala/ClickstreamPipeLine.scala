import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants
import org.apache.spark.SparkConf

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import service.DataPipeline
import utils.sparksession
import org.apache.spark.internal.Logging

object ClickstreamPipeLine extends Logging {
  def main(args: Array[String]): Unit = {
    try
    {
      // main method where the flow of the code starts
      val JOB_START_TIME: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
      logInfo("Clickstream Data Pipeline Process: Beginning  :  " + JOB_START_TIME)

      // read the configuration file from command line arguments
      val configPath = "C:\\ClickstreamProject\\src\\main\\resources\\clickstreamLocalConfig.conf"
      // val configPath = args(0)
      val applicationConf: Config = ConfigFactory.parseFile(new File(configPath))
      logInfo(s"$applicationConf")

      // initiating the application constants based on configuration files
      val appConstants : ApplicationConstants = new ApplicationConstants()
      logInfo("Hello")

      // creating spark session- begin
      val sparkAppName = applicationConf.getString(appConstants.SPARK_APPNAME)
      val sparkMaster = applicationConf.getString(appConstants.SPARK_MASTER)
      val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
      logInfo(s"This is $sparkConf")

      val sparkSession = sparksession.sparkSession(sparkConf,applicationConf,appConstants)
      val sparkAppID = sparkSession.sparkContext.applicationId
      logInfo("This is the sparkAppID  :  " + sparkAppID)
      // creating spark session- end

      // DataPipeline execution begin
      DataPipeline.dataPipeline(sparkSession,applicationConf,appConstants)

      val JOB_END_TIME: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
      logInfo("Clickstream Data Pipeline Process: Completed  :  " + JOB_END_TIME)
      // Pipeline execution ends
    }
    catch {
      case e: Exception=>
        logInfo("An error occured due to failure of main function ClickstreamPipeline.",e)
    }
  }
}
