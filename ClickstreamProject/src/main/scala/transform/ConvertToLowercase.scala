package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {
  def convertToLowercase(df1rename:DataFrame,df2rename:DataFrame):(DataFrame,DataFrame)={
    // converting records of redirection_source to lowercase
    val df1lowercase = df1rename.withColumn("redirection_source", lower(col("redirection_source")))

    // converting records of department_name to lowercase
    val df2lowercase = df2rename.withColumn("department_name", lower(col("department_name")))

    (df1lowercase,df2lowercase)
  }
}
