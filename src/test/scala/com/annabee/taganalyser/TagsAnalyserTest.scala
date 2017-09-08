package com.annabee.taganalyser

import org.apache.spark.sql.SparkSession
import org.scalatest._
import scala.io.Source

class TagsAnalyserTest extends WordSpec with Matchers  {

  val spark: SparkSession = SparkSession.builder
    .appName("TestApp")
    .master("local[*]")
    .getOrCreate

  val underTest = new TagsAnalyser(spark)
  import spark.implicits._

  "TagsAnalyser" when {

    "given a file with spaces around tags" should {
      "correctly decode rows trimming the spaces" in {
        val resource =  Source.getClass.getResource("/resources/spacesAroundTags.csv").getPath
        val result = underTest.extractDocumentsAndTags(resource)
        result.collect().head.tags should be (Array("tag1", "tag2"))
      }
    }

    "given a file with empty lines" should {
      "correctly decode rows discarding any empty lines" in {
        val resource =  Source.getClass.getResource("/resources/emptyLines.csv").getPath
        val result = underTest.extractDocumentsAndTags(resource)
        result.collect().head.tags should be (Array("tag1", "tag2"))
        result.collect().tail.head.tags should be (Array("tag3"))
      }
    }

    "given a dataset of tagged documents" should {
      "compute how often those tags are referenced as related" in {
        val testInput = Seq(
        DocumentTagged(Array("apple", "fruit", "food")),
        DocumentTagged(Array("house", "building")),
        DocumentTagged(Array("apple", "fruit")),
        DocumentTagged(Array("peach", "fruit", "sweet")),
        DocumentTagged(Array("tower")),
        DocumentTagged(Array("sweet", "fruit", "apple")),
        DocumentTagged(Array("house", "home", "apple")),
        DocumentTagged(Array("home", "building"))).toDS()

        val expectedOutput =
          Array(
            RelatedTagsWithFrequency("fruit", "apple", 3),
            RelatedTagsWithFrequency("apple", "fruit", 3),
            RelatedTagsWithFrequency("fruit", "sweet", 2),
            RelatedTagsWithFrequency("sweet", "fruit", 2),
            RelatedTagsWithFrequency("home", "apple", 1),
            RelatedTagsWithFrequency("apple", "house", 1),
            RelatedTagsWithFrequency("fruit", "peach", 1),
            RelatedTagsWithFrequency("house", "home", 1),
            RelatedTagsWithFrequency("house", "apple", 1),
            RelatedTagsWithFrequency("peach", "sweet", 1),
            RelatedTagsWithFrequency("fruit", "food", 1),
            RelatedTagsWithFrequency("sweet", "peach", 1),
            RelatedTagsWithFrequency("sweet", "apple", 1),
            RelatedTagsWithFrequency("house", "building", 1),
            RelatedTagsWithFrequency("building", "house", 1),
            RelatedTagsWithFrequency("food", "apple", 1),
            RelatedTagsWithFrequency("home", "house", 1),
            RelatedTagsWithFrequency("building", "home", 1),
            RelatedTagsWithFrequency("apple", "food", 1),
            RelatedTagsWithFrequency("apple", "home", 1),
            RelatedTagsWithFrequency("food", "fruit", 1),
            RelatedTagsWithFrequency("peach", "fruit", 1),
            RelatedTagsWithFrequency("home", "building", 1),
            RelatedTagsWithFrequency("apple", "sweet", 1))

        underTest.computeRelatedTags(testInput).collect() should be (expectedOutput)
      }
    }

    "given a dataset of related tags" should {
      "display related tags" in {
        val testInput =
          Seq(RelatedTagsWithFrequency("apple", "fruit", 3),
            RelatedTagsWithFrequency("apple", "food", 2),
            RelatedTagsWithFrequency("fruit", "apple", 1),
            RelatedTagsWithFrequency("house", "building", 1),
            RelatedTagsWithFrequency("building", "house", 1)).toDS()

        val expectedOutput = Array(("apple", "fruit"), ("apple", "food"), ("building", "house"), ("fruit", "apple"), ("house", "building"))
        underTest.getMaximumRelatedTagsPerTag(2, testInput).collect().toSeq should be (expectedOutput)
      }
    }
  }
}
