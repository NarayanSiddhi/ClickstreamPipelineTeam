package transform

import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig

class RenameColumnTest extends AnyFlatSpec {

  "RenameColumn object" should "do the following"

  it should "rename columns" in{
    // Reading the dataframes
    val (clickstreamDataframe, itemsetDataframe) = sparkReadConfig.readTestDataframe()
    // Calling the RenameColumn object
    val (clickstreamRename, itemsetRename) = RenameColumn.renameColumn(clickstreamDataframe, itemsetDataframe)

    // Defining the renamed columns as an array
    val clickstreamExpected=Array("Entity_id","event_timestamp","device_type_t","visitor_session_c","visitor_id","item_id","redirection_source_t")
    val itemset_expected=Array("item_id","item_unit_price_a","product_type_c","department_n")

    // Asserting the renamed columns
    assertResult(clickstreamExpected)(clickstreamRename.columns)
    assertResult(itemset_expected)(itemsetRename.columns)

    // Show the datasets
    clickstreamRename.show()
    itemsetRename.show()
  }
}
