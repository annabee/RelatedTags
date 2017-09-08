package com.annabee.taganalyser

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

case class DocumentTagged(tags: Array[String])
case class RelatedTags(related: (String, String))
case class RelatedTagsWithFrequency(tag: String, relatedTag: String, frequency: BigInt)

class TagsAnalyser(sparkSession: SparkSession) {

    import sparkSession.implicits._

  /**
    * Given a path to a file containing lines with tags,
    * compute which tags are related across the lines for a given max number of related tags to display
    * save the result as a csv file located in teh resources directory with the current timestamp.
    *
    * @param pathToFile path to the csv file containing tags
    * @param maximumTagsPerTag number of related tags to compute.
    *                          If no number provided, default value 1 is used,
    *                          meaning all possible related tags will be saved
   */
  def getRelatedTags(pathToFile: String, maximumTagsPerTag: Int): Unit = {

    val tagsFromFile = extractDocumentsAndTags(pathToFile)
    val relatedTags = getMaximumRelatedTagsPerTag(maximumTagsPerTag, computeRelatedTags(tagsFromFile))

    relatedTags.write.format("com.databricks.spark.csv")
      .save(s"result_${System.currentTimeMillis()}.csv")
  }

  private [taganalyser]
  def extractDocumentsAndTags(pathToResources: String): Dataset[DocumentTagged] = {
    sparkSession.read.textFile(pathToResources)
      .map(line => line.trim).filter(line => !line.isEmpty)
      .map(row => row.split(",").map(elem => elem.trim))
      .filter(elem => !elem.isEmpty)
      .toDF("tags")
      .as[DocumentTagged]
  }

  private [taganalyser]
  def computeRelatedTags(tagsDataset: Dataset[DocumentTagged]): Dataset[RelatedTagsWithFrequency] = {
    val relatedTags = tagsDataset.flatMap(docs => {
      val arr = docs.tags
      arr flatMap (element => {
        for {e <- arr if !element.equals(e)} yield RelatedTags((element, e))
      })
    })

    relatedTags
      .groupBy(relatedTags.col("related"))
      .agg(count("*") as "frequency")
      .orderBy($"frequency" desc)
      .map(row => row.get(0) match {
        case a: Row => RelatedTagsWithFrequency(a.getString(0), a.getString(1), row.getLong(1))
      })
  }

  private [taganalyser]
  def getMaximumRelatedTagsPerTag(maximumTagsPerTag: Int, relatedTags: Dataset[RelatedTagsWithFrequency]): Dataset[(String, String)] = {

    val tags = relatedTags.select("tag").distinct.collect.flatMap(_.toSeq)
    tags.flatMap(tag => relatedTags.select("tag", "relatedTag").where($"tag" <=> tag)
        .map(row => (row.getString(0), row.getString(1))).take(maximumTagsPerTag))
        .toSeq.toDS().coalesce(1)
  }
}


