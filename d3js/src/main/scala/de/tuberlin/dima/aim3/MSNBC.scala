package de.tuberlin.dima.aim3

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object MSNBC extends App {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.getConfig.disableSysoutLogging()

  val categories = "frontpage news tech local opinion on-air misc weather msn-news health living" +
    " business msn-sports sports summary bbs travel end" split ' '

  val sequences = env.readTextFile(
    "/media/georgy/work/Data/msnbc/msnbc990928.seq")

  val clickStream = sequences.map { seq =>
    s"${seq.trim} 18".split(' ')
      .take(6).map(_.toInt - 1)
      .map(categories).mkString(" ") -> 1
  }.groupBy("_1").sum("_2").filter(_._2 > 1)

  println(clickStream.count())
  clickStream.writeAsCsv("/media/georgy/work/Data/msnbc/click-stream",
    fieldDelimiter = ",", writeMode = WriteMode.OVERWRITE)

  env.execute("MSNBC")
}
