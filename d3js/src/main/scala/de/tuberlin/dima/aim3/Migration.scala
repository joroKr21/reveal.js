package de.tuberlin.dima.aim3

import scala.io.Source

object Migration extends App {

  val data = Source.fromFile("/home/georgy/work/edu/d3-reveal/data/migration.csv")
    .getLines().toVector.tail.map { line =>
    val record = line.filterNot(_ == '"').split(',')
    val orig = record(1).toInt - 1
    val dest = record(3).toInt - 1
    val migrants = record.slice(8, 12).map(_.toDouble).sum
    (orig, dest, migrants)
  }.distinct.sorted

  val total = data
    .map(_._3).sum

  val fractions = data
    .map { case (orig, dest, migrants) => (orig, dest, migrants / total) }
    .groupBy(_._1).values
    .map(_.map(_._3).mkString("[", ",", "]"))
    .mkString("[", ",", "]")

  println(fractions)
}
