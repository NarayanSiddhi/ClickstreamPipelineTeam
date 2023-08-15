package transform

import org.apache.spark.sql._

object RemoveDuplicate {

  //removing all duplicates
  def removeDuplicates(df: DataFrame, columns: Seq[String]): DataFrame = {
    df.dropDuplicates(columns)
  }

  def main():(DataFrame,DataFrame)= {

    val (df1notnull,df2notnull)=NullCheck.main
    val df1Duplicates = removeDuplicates(df1notnull, Seq("Entity_id"))
    val df2Duplicates = removeDuplicates(df2notnull, Seq("item_id"))

    (df1Duplicates,df2Duplicates)
  }
}