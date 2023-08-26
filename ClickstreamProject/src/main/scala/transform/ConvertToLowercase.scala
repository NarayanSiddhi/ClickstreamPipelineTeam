package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.DataPipeline

object ConvertToLowercase {
  def convertToLowercase(df1duplicate:DataFrame,df2duplicate:DataFrame):(DataFrame,DataFrame)={
    try
    { // converting records of redirection_source to lowercase
      val df1lowercase = df1duplicate.withColumn("redirection_source", lower(col("redirection_source")))

      // converting records of department_name to lowercase
      val df2lowercase = df2duplicate.withColumn("department_name", lower(col("department_name")))

      (df1lowercase, df2lowercase)
    }
    catch {
      case e: Exception =>
        DataPipeline.logger.error("An error occurred during converting to lowercase.", e)
        // Returning original DataFrames as an example
        (df1duplicate, df2duplicate)
    }
  }
}
