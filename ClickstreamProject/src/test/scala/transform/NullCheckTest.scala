package transform

import org.apache.spark.sql.DataFrame
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import utils.spark_readDF_config_test

class NullCheckTest extends AnyFlatSpec {

  "NullCheck object" should "do the following"

  it should "remove null values" in{
    val (clickstreamDF, itemsetDF) = spark_readDF_config_test.readTestDF()
    val (df1removenull, df2removenull) = NullCheck.nullCheck(clickstreamDF, itemsetDF)

    assertResult(9)(df1removenull.select("id").count())
    assertResult(10)(df2removenull.select("item_id").count())

    df1removenull.show()
    df2removenull.show()
  }
}
