package transform

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig

class NullCheckTest extends AnyFlatSpec {

  "NullCheck object" should "do the following"

  it should "remove null values" in{
    // Reading the dataframes
    val (clickstreamDataframe, itemsetDataframe) = sparkReadConfig.readTestDataframe()

    // Loading configuration files
    val nullPathClickstream = ConfigFactory.load("test_application.conf").getString("output.sampleNullClickstream")
    val nullPathItemset = ConfigFactory.load("test_application.conf").getString("output.sampleNullItemset")

    // Calling the NullCheck object
    val (clickstreamRemoveNull, itemsetRemoveNull) = NullCheck.nullCheck(clickstreamDataframe, itemsetDataframe, nullPathClickstream, nullPathItemset)

    // Asserting the null records
    assertResult(10)(clickstreamRemoveNull.select("id").count())
    assertResult(11)(itemsetRemoveNull.select("item_id").count())

    // Show the datasets
    clickstreamRemoveNull.show()
    itemsetRemoveNull.show()
  }
}
