package transform

import org.apache.spark.sql._

object RemoveDuplicates {
  def removeDuplicates(df1removenull:DataFrame,df2removenull:DataFrame):(DataFrame,DataFrame)={
    try {
      // remove all duplicate records from columns "id" and "item_id"
      val df1Duplicates = df1removenull.dropDuplicates("id")
      val df2Duplicates = df2removenull.dropDuplicates("item_id")

      (df1Duplicates, df2Duplicates)
    } catch {
      case e: Exception =>
        println("An error occurred during duplicate removal:")
        e.printStackTrace()
        // we can handle the error here, such as returning default DataFrames or rethrowing the exception
        // Returning original DataFrames as an example
        (df1removenull, df2removenull)
    }
  }
}
