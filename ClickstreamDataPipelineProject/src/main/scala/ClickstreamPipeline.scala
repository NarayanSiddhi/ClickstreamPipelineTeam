import service.{DataPipeline, FileWriter}

object ClickstreamPipeline {
  def main(args: Array[String]): Unit = {
    DataPipeline.dataPipeline()
  }
}

//add tableWriter class----code to load joined dataset into mysql