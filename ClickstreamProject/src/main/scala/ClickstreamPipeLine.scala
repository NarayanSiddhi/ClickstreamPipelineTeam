import service.DataPipeline

object ClickstreamPipeLine {
  def main(args: Array[String]): Unit = {
    // main class where the flow of the code starts
    // first call DataPipeline object
    DataPipeline.dataPipeline()
  }
}