//package dataevents
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
//object transformations{
//
//  def readConfig(): SparkConf = {
//    val config = ConfigFactory.load("application.conf")
//    val sparkAppName = config.getString("spark.appName")
//    val sparkMaster = config.getString("spark.master")
//    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
//  }
//
//  def readDataFrame(spark: SparkSession, inputPath: String): DataFrame = {
//    spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
//  }
//
//  def removeNullRecords(df: DataFrame, columns: Seq[String]): DataFrame = {
//    df.na.drop(columns)
//  }
//
//  def removeDuplicates(df: DataFrame, columns: Seq[String]): DataFrame = {
//    df.dropDuplicates(columns)
//  }
//
//  def convertToLowercase(df: DataFrame, column: String): DataFrame = {
//    df.withColumn(column, lower(col(column)))
//  }
//
//  def main(args: Array[String]): Unit = {
//    val sparkConf = readConfig()
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    val logLevel = ConfigFactory.load("application.conf").getString("spark.logLevel")
//    spark.sparkContext.setLogLevel(logLevel)
//
//    val inputPath1 = ConfigFactory.load("application.conf").getString("input.path1")
//    val inputPath2 = ConfigFactory.load("application.conf").getString("input.path2")
//
//    val df1 = readDataFrame(spark, inputPath1)
//    val df2 = readDataFrame(spark, inputPath2)
//
//    val df1cast = df1.withColumn("event_timestamp",
//      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
//    val df2cast: DataFrame = df2.withColumn("item_price", col("item_price").cast("Double"))
//
//    // Rename to meaningful column names
//    val df1rename: DataFrame = df1cast.withColumnRenamed("id", "Entity_id")
//      .withColumnRenamed("device_type", "device_type_t")
//      .withColumnRenamed("session_id", "visitor_session_c")
//      .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()
//    val df2rename: DataFrame = df2cast.withColumnRenamed("item_price", "item_unit_price_a")
//      .withColumnRenamed("product_type", "product_type_c")
//      .withColumnRenamed("department_name", "department_n")
//
//    val df1notnull = removeNullRecords(df1rename, Seq("Entity_id"))
//    val df2notnull = removeNullRecords(df2rename, Seq("item_id"))
//
//    val df1Duplicates = removeDuplicates(df1notnull, Seq("Entity_id"))
//    val df2Duplicates = removeDuplicates(df2notnull, Seq("item_id"))
//
//    val df1lowercase = convertToLowercase(df1Duplicates, "redirection_source_t")
//    val df2lowercase = convertToLowercase(df2Duplicates, "department_n")
//
//    // Print both schemas
//    df1lowercase.printSchema()
//    df2lowercase.printSchema()
//
//    // Join both the dataframes
//    val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))
//
//    // Show the final DataFrame+
//    finalDF.show()
//
//    // Write processed data to output path
//    val outputPath = ConfigFactory.load("application.conf").getString("output.path")
//
//    // Write the final dataframe to a csv file
//    finalDF.repartition(1).write.option("header", "true").csv(outputPath)
//
//    spark.stop()
//  }
//}
//object transformations {
//
//  def main(args: Array[String]): Unit = {
//
//    // reading configuration file----application.config
//    val config = ConfigFactory.load("application.conf")
//    val sparkAppName = config.getString("spark.appName")
//    val logLevel = config.getString("spark.logLevel")
//    val sparkMaster = config.getString("spark.master")
//    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//    // Read clickstream data from input paths
//    val inputPath1 = config.getString("input.path1")
//    val inputPath2 = config.getString("input.path2")
//
//    val sc = spark.sparkContext
//
//    sc.setLogLevel(logLevel)
//
//    // Read both CSV files into a DataFrame
//    val df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath1)
//    val df2:DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath2)
//
//    // Cast to the desired data types
//    val df1cast=df1.withColumn("event_timestamp",
//      to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
//    val df2cast:DataFrame=df2.withColumn("item_price", col("item_price").cast("Double"))
//
//    // Rename to meaningful column names
//    val df1rename: DataFrame = df1cast.withColumnRenamed("id", "Entity_id")
//      .withColumnRenamed("device_type", "device_type_t")
//      .withColumnRenamed("session_id", "visitor_session_c")
//      .withColumnRenamed("redirection_source", "redirection_source_t")//.printSchema()
//    val df2rename :DataFrame = df2cast.withColumnRenamed("item_price", "item_unit_price_a")
//      .withColumnRenamed("product_type", "product_type_c")
//      .withColumnRenamed("department_name", "department_n")
//
//    // Remove records with null values
//    val df1notnull=df1rename.na.drop(Seq("Entity_id"))
//    val df2notnull=df2rename.na.drop(Seq("item_id"))
//
//    // Remove duplicate values
//    val df1Duplicates = df1notnull.dropDuplicates("Entity_id")
//    val df2Duplicates = df2notnull.dropDuplicates("item_id")
//
//    // Convert everything to lower case
//    val df1lowercase = df1Duplicates.withColumn("redirection_source_t", lower(col("redirection_source_t")))
//    val df2lowercase = df2Duplicates.withColumn("department_n", lower(col("department_n")))
//
//    // Print both schemas
//    df1lowercase.printSchema()
//    df2lowercase.printSchema()
//
//    // Join both the dataframes
//    val finalDF: DataFrame = df1lowercase.join(df2lowercase, Seq("item_id"))
//
//    // Show the final DataFrame+
//    finalDF.show()
//
//    // Write processed data to output path
//    val outputPath = config.getString("output.path")
//
//    // Write the final dataframe to a csv file
//    finalDF.repartition(1).write.option("header","true").csv(outputPath)
//
//    spark.stop()
//  }
//}
//def dfcast(df:DataFrame,columns: Seq[String])={
//  df.withColumn("event_timestamp",
//    to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
//    .withColumn("item_price", col("item_price").cast("Double"))
//}
//
//val df1cast=dfcast(df1)
//val df1cast=dfcast(df2)
