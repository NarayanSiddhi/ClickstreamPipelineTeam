package transform

import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object RenameColumn extends Logging {
  def renameColumn(clickstreamLowercase:DataFrame,itemsetLowercase:DataFrame):(DataFrame,DataFrame)={
    try
    { // renaming column names to their meaningful names of clickstream dataset
      val clickstreamRename: DataFrame = clickstreamLowercase.withColumnRenamed("id", "Entity_id")
        .withColumnRenamed("device_type", "device_type_t")
        .withColumnRenamed("session_id", "visitor_session_c")
        .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()

      // renaming column names to their meaningful names of item dataset
      val itemsetRename: DataFrame = itemsetLowercase.withColumnRenamed("item_price", "item_unit_price_a")
        .withColumnRenamed("product_type", "product_type_c")
        .withColumnRenamed("department_name", "department_n")
      (clickstreamRename, itemsetRename)
    }
    catch {
      case e: Exception =>
        logInfo("An error occurred during renaming the columns.", e)
        // Returning original DataFrames
        (clickstreamLowercase, itemsetLowercase)
    }
  }
}
