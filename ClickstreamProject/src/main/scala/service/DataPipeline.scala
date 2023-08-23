package service

import com.typesafe.config.ConfigFactory
import database.DatabaseWrite
import transform.{CastDataTypes, ConvertToLowercase, NullCheck, RemoveDuplicates, RenameColumn}
import utils.sparksession

// here the main execution of pipeline starts
object DataPipeline {
  def dataPipeline():Unit={
    // calling sparksession object to build SparkSession
    val spark=sparksession.sparkSession()

    // reading the input files from application.conf
    val inputPath_clickstream = ConfigFactory.load("application.conf").getString("input.path1")
    val inputPath_itemset = ConfigFactory.load("application.conf").getString("input.path2")

    // Write processed data to output path
    val outputPath = ConfigFactory.load("application.conf").getString("output.path")

    // calling FileReader to read both input files
    val clickstream_DF=FileReader.readDataFrame(spark,inputPath_clickstream)
    val itemSet_DF=FileReader.readDataFrame(spark,inputPath_itemset)

    // calling CastDataTypes object to cast the datatypes of columns
    val (df1cast,df2cast)=CastDataTypes.castDataTypes(clickstream_DF,itemSet_DF)

    // calling NullCheck object to remove columns where null values are present
    val (df1removenull,df2removenull)=NullCheck.nullCheck(df1cast,df2cast)

    // calling RemoveDuplicates object to remove the columns where duplicate values are present
    val (df1duplicates,df2duplicates)=RemoveDuplicates.removeDuplicates(df1removenull,df2removenull)

    // calling ConvertToLowercase object to convert all records of a particular column to lowercase
    val (df1lowercase,df2lowercase)=ConvertToLowercase.convertToLowercase(df1duplicates,df2duplicates)

    // calling RenameColumn object to rename column names to their meeaningful names
    val (df1rename,df2rename)=RenameColumn.renameColumn(df1lowercase,df2lowercase)

    // calling FileWriter object to join the two dataframes and write the final output to a csv file
    val joinedDF = FileWriter.fileWriter(df1rename,df2rename,outputPath)

    // calling DatabaseWrite object to write the final dataframe to MySql table named "cdp"
    DatabaseWrite.writeToMySQL(joinedDF, "cdp")
    // joinedDF.show()
  }
}
