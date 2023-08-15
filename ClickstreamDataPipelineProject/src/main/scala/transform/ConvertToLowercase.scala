package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConvertToLowercase {

  //convert everything to lowercase
  def convertToLowercase(df: DataFrame, column: String): DataFrame = {
    df.withColumn(column, lower(col(column)))
  }

  def main():(DataFrame,DataFrame)= {

    val (df1Duplicates,df2Duplicates)=RemoveDuplicate.main()
    val df1lowercase = convertToLowercase(df1Duplicates, "redirection_source_t")
    val df2lowercase = convertToLowercase(df2Duplicates, "department_n")

    (df1lowercase,df2lowercase)
  }
}
