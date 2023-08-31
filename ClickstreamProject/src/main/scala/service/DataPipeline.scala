package service

import com.typesafe.config.Config
import constants.ApplicationConstants
import database.DatabaseWrite
import org.apache.spark.sql.SparkSession
import dataQuality.{ClickstreamItemPrice,ItemsetEventTimestamp}
import transform.{CastDataTypes, ConvertToLowercase, NullCheck, RemoveDuplicates, RenameColumn}

object DataPipeline{
  // here the main execution of pipeline starts
  def dataPipeline(spark:SparkSession, config:Config, appConstants: ApplicationConstants):Unit={

    // reading the input files from ApplicationConstants class
    val inputPath_clickstream = config.getString(appConstants.CLICKSTREAM_INPUT_PATH)
    val inputPath_itemset = config.getString(appConstants.ITEMSET_INPUT_PATH)

    // Write processed data to output paths
    val outputPath = config.getString(appConstants.JOINED_DATASET)
    val nullPathClickstream = config.getString(appConstants.CLICKSTREAM_NULLS)
    val nullPathItemset = config.getString(appConstants.ITEMSET_NULLS)
    val duplicatesPathClickstream = config.getString(appConstants.CLICKSTREAM_DUPLICATES)
    val duplicatesPathItemset = config.getString(appConstants.ITEMSET_DUPLICATES)
    val invalidItemPrice = config.getString(appConstants.INVALID_ITEM_PRICE)
    val invalidEventTimestamp = config.getString(appConstants.INVALID_EVENT_TIMESTAMP)

    // calling FileReader to read both input files
    val clickstreamDataframe=FileReader.readDataFrame(spark,inputPath_clickstream)
    val itemSetDataframe=FileReader.readDataFrame(spark,inputPath_itemset)

    // Show the original dataset schema
    println("The schema of Clickstream Dataset: ")
    clickstreamDataframe.printSchema()
    println("The schema of Item Dataset:")
    itemSetDataframe.printSchema()

    // Show the original dataset
    println("This is Clickstream Dataset")
    clickstreamDataframe.show()
    println("This is Item Dataset")
    itemSetDataframe.show()

    // calling CastDataTypes object to cast the datatypes of columns
    val (clickstreamCast,itemsetCast)=CastDataTypes.castDataTypes(clickstreamDataframe,itemSetDataframe)

    // calling NullCheck object to remove columns where null values are present
    val (clickstreamRemoveNull,itemsetRemoveNull)=NullCheck.nullCheck(clickstreamCast,itemsetCast,nullPathClickstream,nullPathItemset)

    // calling RemoveDuplicates object to remove the columns where duplicate values are present
    val (clickstreamDuplicates,itemsetDuplicates)=RemoveDuplicates.removeDuplicates(clickstreamRemoveNull,itemsetRemoveNull,duplicatesPathClickstream,duplicatesPathItemset)

    // calling ConvertToLowercase object to convert all records of a particular column to lowercase
    val (clickstreamLowercase,itemsetLowercase)=ConvertToLowercase.convertToLowercase(clickstreamDuplicates,itemsetDuplicates)

    // calling RenameColumn object to rename column names to their meaningful names
    val (clickstreamRename,itemsetRename)=RenameColumn.renameColumn(clickstreamLowercase,itemsetLowercase)

    // calling FileWriter object to join the two dataframes and write the final output to a csv file
    val joinedDataframe = FileWriter.fileWriter(clickstreamRename,itemsetRename,outputPath)

    // data quality check for item_price
    ClickstreamItemPrice.clickstreamItemPrice(joinedDataframe,appConstants,invalidItemPrice)

    // data quality check for event_timestamp
    ItemsetEventTimestamp.itemsetEventTimestamp(joinedDataframe,appConstants,invalidEventTimestamp)

    // calling DatabaseWrite object to write the final dataframe to MySql table named "cdp"
    DatabaseWrite.writeToMySQL(joinedDataframe, "cdp", config, appConstants)
  }
}
