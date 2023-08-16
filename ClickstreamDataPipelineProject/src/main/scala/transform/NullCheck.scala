package transform

import org.apache.spark.sql._

object NullCheck {

  //null validation using exception handling
  def removeNullRecords(): (DataFrame,DataFrame) = {
    var df1notnull: DataFrame = null
    var df2notnull: DataFrame = null

    try {
      val (df1rename, df2rename) = RenameColumns.renameColumns
      val df1notnull = df1rename.na.drop(Seq("Entity_id"))
      val df2notnull = df2rename.na.drop(Seq("item_id"))
      return (df1notnull, df2notnull)
    }
    catch {
      case ex: Exception => println(s"An exception occurred: ${ex.getMessage}")
        df1notnull = null
        df2notnull = null
    }

    if (df1notnull != null && df2notnull != null) {
      (df1notnull, df2notnull)
    } else {
      (null, null)
    }
  }
}
