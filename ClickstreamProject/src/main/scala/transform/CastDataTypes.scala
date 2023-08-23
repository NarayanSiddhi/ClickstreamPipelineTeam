package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CastDataTypes {
  def castDataTypes(df1:DataFrame,df2:DataFrame):(DataFrame,DataFrame)={
    // casting event_timestamp from string to timestamp datatype
    val df1cast = df1.withColumn("event_timestamp",
      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))

    // casting item_price from string to double datatype
    val df2cast: DataFrame = df2.withColumn("item_price", col("item_price").cast("Double"))

    (df1cast,df2cast)
  }
}
