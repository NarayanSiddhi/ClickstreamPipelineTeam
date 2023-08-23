package service

import org.apache.spark.sql._

object FileWriter {
  def fileWriter(df1lowercase:DataFrame, df2lowercase:DataFrame,outputPath:String): DataFrame = {
    try {
      df1lowercase.printSchema()
      df2lowercase.printSchema()

      // Join both the dataframes
      val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))

      // Show the final DataFrame
      finalDF.show()

      // Write the final dataframe to a csv file
      finalDF.repartition(1).write.option("header", "true").csv(outputPath)
      finalDF
    }
    catch {
      case e: Exception =>
        println(s"An error occurred while writing the DataFrame to $outputPath:")
        e.printStackTrace()
        // we can handle the error here, such as returning an empty DataFrame or rethrowing the exception
        // Returning one of the input DataFrames as an example
        df1lowercase
    }
  }
}
