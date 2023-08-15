package transform

import org.apache.spark.sql._

object RenameColumns {

  // Rename to meaningful column names
  def renameColumns:(DataFrame,DataFrame)= {

    val (df1cast, df2cast) = CastDatatypes.castDatatypes()
    val df1rename: DataFrame = df1cast.withColumnRenamed("id", "Entity_id")
      .withColumnRenamed("device_type", "device_type_t")
      .withColumnRenamed("session_id", "visitor_session_c")
      .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()
    val df2rename: DataFrame = df2cast.withColumnRenamed("item_price", "item_unit_price_a")
      .withColumnRenamed("product_type", "product_type_c")
      .withColumnRenamed("department_name", "department_n")

    (df1rename,df2rename)

  }
}
