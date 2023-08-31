package dataQuality

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import constants.ApplicationConstants

object ItemsetEventTimestamp {
  def itemsetEventTimestamp(dfWithTimestamp:DataFrame, appConstants:ApplicationConstants, invalidevents:String): Unit = {
    // Define lower and upper limits for timestamp range
    val lowerLimit = appConstants.DQ_EVENT_TIMESTAMP_LOWER_THRESHOLD
    val upperLimit = appConstants.DQ_EVENT_TIMESTAMP_UPPER_THRESHOLD

    // Filter out timestamps outside the range (invalid data)
    val invalidTimestampData = dfWithTimestamp.filter(col("event_timestamp") < lowerLimit || col("event_timestamp") > upperLimit)

    // Save invalid timestamp data to a separate file (CSV format)
    invalidTimestampData.repartition(1).write.mode("overwrite").option("header", "true").csv(invalidevents)

    // Summary of Timestamp Range for valid data
    val minTimestamp = dfWithTimestamp.agg(min("event_timestamp")).collect()(0)(0)
    val maxTimestamp = dfWithTimestamp.agg(max("event_timestamp")).collect()(0)(0)
    println(s"Minimum timestamp: $minTimestamp")
    println(s"Maximum timestamp: $maxTimestamp")
  }
}
