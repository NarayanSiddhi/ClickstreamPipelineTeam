package transform

import org.apache.spark.sql._

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={
    // initializing the dataframes to null
    var df1notnull: DataFrame = null
    var df2notnull: DataFrame = null

    try {
      // drop the records where "id" and "item_id" are null
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))

      return (df1notnull, df2notnull)
    }
    catch {
      case ex: Exception => println(s"An exception occurred: ${ex.getMessage}")

        // giving null values to both dataframes if exception occured
        df1notnull = null
        df2notnull = null
    }

    // if the dataframes are not null return them otherwise return null
    if (df1notnull != null && df2notnull != null) {
      (df1notnull, df2notnull)
    } else {
      (null, null)
    }
  }
}
