package service

import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object FileWriter extends Logging {
  def fileWriter(clickstreamRename:DataFrame,itemsetRename:DataFrame,outputPath:String): DataFrame = {
    try {
      // Join both the dataframes
      val ClickstreamEventItemDataframe: DataFrame = clickstreamRename.join(itemsetRename, Seq("item_id"))

      // Show the joined dataset schema
      ClickstreamEventItemDataframe.printSchema()
      
      // Show the ClickstreamEventItem DataFrame
      println("The joined dataset is: ")
      ClickstreamEventItemDataframe.show()

      // Write the ClickstreamEventItem dataframe to a csv file
      ClickstreamEventItemDataframe.repartition(1).write.mode("overwrite").option("header", "true").csv(outputPath)
      ClickstreamEventItemDataframe
    }
    catch {
      case e: Exception =>
        logInfo(s"No data is there in dataframe to load in MySQL table",e)
        // Returning one of the input DataFrames as an example
        null
    }
  }
}
