package transform

import org.apache.spark.sql._

object NullCheck {

  //null validation using exception handling
  def removeNullRecords(df: DataFrame, columns: Seq[String]): DataFrame = {
    df.na.drop(columns)
  }

  def main():(DataFrame,DataFrame)= {

    var df1notnull: DataFrame = null
    var df2notnull: DataFrame = null

    try{
      val (df1rename, df2rename) = RenameColumns.renameColumns
      val df1notnull = removeNullRecords(df1rename, Seq("Entity_id"))
      val df2notnull = removeNullRecords(df2rename, Seq("item_id"))
      return (df1notnull,df2notnull)
    }
    catch {
      case ex: Exception => println(s"An exception occurred: ${ex.getMessage}")
        df1notnull=null
        df2notnull=null
    }

    if (df1notnull != null && df2notnull != null) {
      (df1notnull,df2notnull)
    } else{
      (null,null)
    }
  }
}
