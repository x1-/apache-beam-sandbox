package com.inkenkun.x1.beam.sandbox

import com.spotify.scio._


/*
SBT
runMain
  com.inkenkun.x1.beam.sandbox.WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://[BUCKET]/[PATH]/[FILE]
  --output=gs://[BUCKET]/[PATH]/[DIR]
*/
object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(args("output"))
    sc.close()
  }
}