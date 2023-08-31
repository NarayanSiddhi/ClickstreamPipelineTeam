package constants

class ApplicationConstants extends Serializable {
  val CLICKSTREAM_INPUT_PATH: String ="app.input.clickstreamPath"
  val ITEMSET_INPUT_PATH: String ="app.input.itemsetPath"

  val JOINED_DATASET: String ="app.output.joinedDatasetPath"
  val CLICKSTREAM_NULLS: String ="app.output.nullClickstream"
  val ITEMSET_NULLS: String ="app.output.nullItemset"
  val CLICKSTREAM_DUPLICATES: String ="app.output.duplicateClickstream"
  val ITEMSET_DUPLICATES: String ="app.output.duplicateItemset"
  val INVALID_ITEM_PRICE: String = "app.output.invalidItemPrice"
  val INVALID_EVENT_TIMESTAMP: String = "app.output.invalidEventTimestamp"

  val SPARK_MASTER: String ="app.spark.master"
  val SPARK_APPNAME: String ="app.spark.appName"
  val SPARK_LOGLEVEL: String ="app.spark.logLevel"

  val JDBC_URL: String ="app.jdbc.jdbcUrl"
  val JDBC_USER: String ="app.jdbc.jdbcUser"
  val JDBC_PASSWORD: String ="app.jdbc.jdbcPassword"

  val DQ_ITEM_PRICE_LOWER_THRESHOLD="app.thresholdValue.itemPriceLowerlimit"
  val DQ_ITEM_PRICE_UPPER_THRESHOLD="app.thresholdValue.itemPriceUpperlimit"
  val DQ_EVENT_TIMESTAMP_LOWER_THRESHOLD="app.thresholdValue.eventTimestampLowerlimit"
  val DQ_EVENT_TIMESTAMP_UPPER_THRESHOLD="app.thresholdValue.eventTimestampUpperlimit"
}
