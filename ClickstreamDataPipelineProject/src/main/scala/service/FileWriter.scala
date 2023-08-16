package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import transform.ConvertToLowercase

object FileWriter {

  //write output file to a folder
  def fileWriter(): Unit = {

    val (df1lowercase,df2lowercase)=ConvertToLowercase.convertToLowercase()
    // Print both schemas
    df1lowercase.printSchema()
    df2lowercase.printSchema()

    // Join both the dataframes
    val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))

    // Show the final DataFrame+
    finalDF.show()

    // Write processed data to output path
    val outputPath = ConfigFactory.load("application.conf").getString("output.path")

    // Write the final dataframe to a csv file
    finalDF.repartition(1).write.option("header", "true").csv(outputPath)

  }
}
