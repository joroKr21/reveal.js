package de.tuberlin.dima.aim3

import org.apache.flink.api.scala._

object WikipediaClickStream extends App {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.getConfig.disableSysoutLogging()

  val types = Set("link", "other")

  val entryPoints = Map(
    "other-google" -> "Google",
    "other-yahoo" -> "Yahoo",
    "other-bing" -> "Bing",
    "other-facebook" -> "Facebook",
    "other-twitter" -> "Twitter",
    "other-other" -> "Other"
  ).withDefaultValue("Wikipedia")

  val clickStream = env.readCsvFile[Click](
    "/media/georgy/work/Data/wikipedia-2015-02-clickstream.tsv",
    fieldDelimiter = "\t", ignoreFirstLine = true)

  val links = clickStream
    .filter(click => types.contains(click.tpe.toLowerCase))

  val outDegrees = links
    .map(click => (click.prevTitle, click.count))
    .groupBy("_1").sum("_2")

  val fractions = outDegrees
    .join(links).where(_._1).equalTo(_.prevTitle)
    .apply((out, click) => click.copy(count = click.count / out._2))
    .filter(_.count >= 0.001)

  val initial = fractions
    .filter(click => entryPoints.contains(click.prevTitle.toLowerCase))
    .map(click => click.copy(prevTitle = entryPoints(click.prevTitle)))
    .collect()

  println(clickStream.map(_.currTitle).distinct().count())

  case class Click(prevId: String, currId: String, count: Double,
                   prevTitle: String, currTitle: String, tpe: String)
}
