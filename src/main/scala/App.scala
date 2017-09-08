import org.apache.spark.sql.SparkSession
import com.annabee.taganalyser.TagsAnalyser

import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Tags")
      .master("local[*]")
      .getOrCreate

    val maximumTagsPerTag = 3
    val pathToFile = Source.getClass.getResource("/resources/tags.csv").getPath

    new TagsAnalyser(spark).getRelatedTags(pathToFile, maximumTagsPerTag)
  }
}
