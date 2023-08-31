package transform

import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object RemoveDuplicates extends Logging {
  def removeDuplicates(clickstreamRemoveNull:DataFrame,itemsetRemoveNull:DataFrame,duplicatesPathClickstream:String,duplicatesPathItemset:String):(DataFrame,DataFrame)={
    try {
      // remove all duplicate records from primary columns "id" and "item_id"
      val clickstreamDuplicates = clickstreamRemoveNull.dropDuplicates("id")
      val itemsetDuplicates = itemsetRemoveNull.dropDuplicates("item_id")

      // storing all duplicate records into another dataframes
      val duplicatesClickstream = clickstreamRemoveNull.except(clickstreamDuplicates)
      val duplicatesItemset = itemsetRemoveNull.except(itemsetDuplicates)

      // writing the filtered duplicate records to error csv files
      duplicatesClickstream.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathClickstream)
      duplicatesItemset.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathItemset)

      (clickstreamDuplicates, itemsetDuplicates)
    } catch {
      case e: Exception =>
        logInfo("An error occurred during duplicate removal.",e)
        // Returning original DataFrames
        (clickstreamRemoveNull, itemsetRemoveNull)
    }
  }
}
