package database

import org.apache.spark.sql._

object DatabaseWrite {
  def writeToMySQL(dataFrame: DataFrame, tableName: String): Unit = {
   dataFrame.write // initiates the process of writing the DataFrame to MySql
      .format("jdbc") // Sets writing format to JDBC
      .mode("overwrite") // Overwrites existing data in target
      .option("driver", "com.mysql.cj.jdbc.Driver") // Specifies MySQL JDBC driver
      .option("url", constant.jdbcUrl) // Sets MySQL database URL
      .option("dbtable", tableName) // Specifies target table name
      .option("user", constant.jdbcUser) // Provides MySQL username
      .option("password", constant.jdbcPassword) // Provides MySQL password
      .save() // Executes DataFrame write to MySQL
  }
}
