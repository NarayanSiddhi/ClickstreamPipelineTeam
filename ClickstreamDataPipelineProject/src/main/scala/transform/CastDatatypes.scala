package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import service.DataPipeline

object CastDatatypes {

  //cast columns to desired datatypes
  def castDatatypes(): (DataFrame,DataFrame) = {

    val (df1,df2)=DataPipeline.dataPipeline()

    val df1cast: DataFrame = df1.withColumn("event_timestamp",
      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
    val df2cast: DataFrame = df2.withColumn("item_price", col("item_price").cast("Double"))

    (df1cast,df2cast)
  }
}
