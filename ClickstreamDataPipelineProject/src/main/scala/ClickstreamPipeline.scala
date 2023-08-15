import service.{DataPipeline, FileWriter}
import transform.{CastDatatypes, ConvertToLowercase, NullCheck, RemoveDuplicate, RenameColumns}

object ClickstreamPipeline {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()
    CastDatatypes.castDatatypes()
    RenameColumns.renameColumns
    NullCheck.main()
    RemoveDuplicate.main()
    ConvertToLowercase.main()
    FileWriter.main()
  }
}