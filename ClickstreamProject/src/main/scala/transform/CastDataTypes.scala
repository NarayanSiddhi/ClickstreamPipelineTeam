package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object CastDataTypes extends Logging {
  def castDataTypes(clickstreamDataframe:DataFrame,itemSetDataframe:DataFrame):(DataFrame,DataFrame)={
    try
    {
      // casting event_timestamp from string to timestamp datatype
      val clickstreamCast = clickstreamDataframe.withColumn("event_timestamp",
        to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))

      // casting item_price from string to double datatype
      val itemsetCast: DataFrame = itemSetDataframe.withColumn("item_price", col("item_price").cast("Double"))

      (clickstreamCast, itemsetCast)
    }
    catch {
      case e: Exception =>
        logInfo("An error occurred during casting of datatypes.", e)
        // Returning original DataFrames
        (clickstreamDataframe, itemSetDataframe)
    }
  }
}
