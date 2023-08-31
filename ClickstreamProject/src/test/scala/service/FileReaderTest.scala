package service

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig

class FileReaderTest extends AnyFlatSpec {

  "FileReader object" should "do the following"

  it should "read the files in the path" in {
    // Creating the SparkSession
    val spark = sparkReadConfig.sparkSession()

    // Reading the test configuration file
    val inputPathClickstream = ConfigFactory.load("test_application.conf").getString("input.sample_path1")
    val inputPathItemset = ConfigFactory.load("test_application.conf").getString("input.sample_path2")

    // Calling the FileReader object
    val clickstreamTestDataframe = FileReader.readDataFrame(spark,inputPathClickstream)
    val itemsetTestDataframe = FileReader.readDataFrame(spark,inputPathItemset)

    // Asserting the read dataframes
    assertResult(12)(clickstreamTestDataframe.count())
    assertResult(12)(itemsetTestDataframe.count())

    // Show the datasets
    clickstreamTestDataframe.show()
    itemsetTestDataframe.show()
  }
}
