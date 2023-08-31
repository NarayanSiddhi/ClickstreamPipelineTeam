package transform

import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import org.apache.spark.sql.types.{StringType,TimestampType, DoubleType}

class CastDataTypesTest extends AnyFlatSpec {

  "CastDataTypes object" should "do the following"

  it should "cast datatypes of columns to desired datatypes" in{
    // Reading the dataframes
    val (clickstreamDataframe,itemsetDataframe)=sparkReadConfig.readTestDataframe()
    // Calling the CastDataTypes object
    val (clickstreamCast,itemsetCast) = CastDataTypes.castDataTypes(clickstreamDataframe,itemsetDataframe)

    // Defining the datatypes
    val str=clickstreamCast.schema("event_timestamp").dataType
    val double=itemsetCast.schema("item_price").dataType

    // Assert datatype for timestamp column using assertResult
    assertResult(str)(TimestampType)

    // Assert datatype for double column using assertResult
    assertResult(double)(DoubleType)

    // Printing the schemas
    println("The schemas of Clickstream Dataset:")
    clickstreamCast.printSchema()
    println("The schemas of Item Dataset:")
    itemsetCast.printSchema()

    // Printing the datasets
    println("The Clickstream Dataset:")
    clickstreamCast.show()
    println("The Item Dataset:")
    itemsetCast.show()
  }
}
