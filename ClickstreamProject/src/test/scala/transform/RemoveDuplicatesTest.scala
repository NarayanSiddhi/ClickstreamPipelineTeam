package transform

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig

class RemoveDuplicatesTest extends AnyFlatSpec {

  "RemoveDuplicates object" should "do the following"

  it should "remove duplicate values" in{
    // Read the dataframes
    val (clickstreamDataframe, itemsetDataframe) = sparkReadConfig.readTestDataframe()

    // Load the configuration files
    val duplicatesPathClickstream = ConfigFactory.load("test_application.conf").getString("output.sampleDuplicateClickstream")
    val duplicatesPathItemset = ConfigFactory.load("test_application.conf").getString("output.sampleDuplicateItemset")

    // Calling the RemoveDuplicates object
    val (clickstreamRemoveDuplicates, itemsetRemoveDuplicates) = RemoveDuplicates.removeDuplicates(clickstreamDataframe, itemsetDataframe, duplicatesPathClickstream, duplicatesPathItemset)

    // Asserting the duplicate records
    assertResult(8)(clickstreamRemoveDuplicates.select("id").count())
    assertResult(8)(itemsetRemoveDuplicates.select("item_id").count())

    // Show the datasets
    clickstreamRemoveDuplicates.show()
    itemsetRemoveDuplicates.show()
  }
}
