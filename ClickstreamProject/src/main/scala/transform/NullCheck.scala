package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.internal.Logging

object NullCheck extends Logging {
  def nullCheck(clickstreamCast:DataFrame,itemsetCast:DataFrame,nullPathClickstream:String,nullPathItemset:String):(DataFrame,DataFrame)={

    try {
      // drop the records where "id" and "item_id" are null
      val clickstreamNotNull = clickstreamCast.na.drop(Seq("id"))
      val itemsetNotNull = itemsetCast.na.drop(Seq("item_id"))

      // defining placeholders for both datasets where null values are present
      val placeholderClickstream = Map(
        "id" -> " ",
        "event_timestamp" -> "%",
        "device_type" -> "%",
        "session_id" -> "%",
        "visitor_id" -> "%",
        "item_id" -> "%",
        "redirection_source" -> "%"
      )
      val placeholderItemset = Map(
        "item_id" -> " ",
        "item_price" -> 0.0,
        "product_type" -> "%",
        "department_name" -> "%"
      )

      // filling all null values in other columns with defined placeholders
      val replacedNullClickstream = clickstreamNotNull.na.fill(placeholderClickstream)
      val replacedNullItemset = itemsetNotNull.na.fill(placeholderItemset)

      // store the null records in dataframes and into error csv files
      val nullRecordsClickstream = clickstreamCast.filter(col("id").isNull)
      val nullRecordsItemset = itemsetCast.filter(col("item_id").isNull)

      // write the filtered records into the error files
      nullRecordsClickstream.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathClickstream)
      nullRecordsItemset.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathItemset)

      (replacedNullClickstream,replacedNullItemset)
    }
    catch {
      case ex: Exception =>
        logInfo("An error occured due to null removel.",ex)
        // return the original dataframes
        (clickstreamCast,itemsetCast)
    }
  }
}
