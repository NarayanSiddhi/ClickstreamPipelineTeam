package transform

import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig

class ConvertToLowercaseTest extends AnyFlatSpec {

  "ConvertToLowercase object" should "do the following"

  it should "convert the specified column records to lowercase" in{
    // Reading the dataframes
    val (clickstreamDataframe, itemsetDataframe) = sparkReadConfig.readTestDataframe()
    // Calling ConvertToLowercase object
    val (clickstreamLowercase, itemsetLowercase) = ConvertToLowercase.convertToLowercase(clickstreamDataframe, itemsetDataframe)

    // Assert lowercase records
    assertResult(12)(clickstreamLowercase.select("redirection_source").count())
    assertResult(12)(itemsetLowercase.select("department_name").count())

    // show the datasets
    clickstreamLowercase.show()
    itemsetLowercase.show()
  }
}
