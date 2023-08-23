package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RenameColumn {
  def renameColumn(df1duplicates:DataFrame,df2duplicates:DataFrame):(DataFrame,DataFrame)={
    // renaming column names to their meaningful names of clickstream dataset
    val df1rename: DataFrame = df1duplicates.withColumnRenamed("id", "Entity_id")
      .withColumnRenamed("device_type", "device_type_t")
      .withColumnRenamed("session_id", "visitor_session_c")
      .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()

    // renaming column names to their meaningful names of item dataset
    val df2rename: DataFrame = df2duplicates.withColumnRenamed("item_price", "item_unit_price_a")
      .withColumnRenamed("product_type", "product_type_c")
      .withColumnRenamed("department_name", "department_n")
      (df1rename,df2rename)
  }
}
