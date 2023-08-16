package transform

import org.apache.spark.sql._

object RemoveDuplicate {

  //removing all duplicates
  def removeDuplicates(): (DataFrame,DataFrame) = {
    val (df1notnull,df2notnull)=NullCheck.removeNullRecords()

    val df1Duplicates = df1notnull.dropDuplicates("Entity_id")
    val df2Duplicates = df2notnull.dropDuplicates("item_id")

    (df1Duplicates,df2Duplicates)
  }
}