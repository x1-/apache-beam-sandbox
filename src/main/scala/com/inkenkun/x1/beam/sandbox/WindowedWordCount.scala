package com.inkenkun.x1.beam.sandbox

import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.util.MimeTypes
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Duration, Instant}

/*
SBT
runMain
  com.inkenkun.x1.beam.sandbox.WindowedWordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://[BUCKET]/[PATH]/[FILE]
  --output=gs://[BUCKET]/[PATH]/[DIR]
*/

object WindowedWordCount {

  private val WINDOW_SIZE = 10L
  private val formatter = ISODateTimeFormat.hourMinute

  def main (cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    FileSystems.setDefaultPipelineOptions(sc.options)

    val input = args("input")
    val windowSize = Duration.standardMinutes(args.long("windowSize", WINDOW_SIZE))
    val minTimestamp = args.long("minTimestampMillis", System.currentTimeMillis())
    val maxTimestamp = args.long(
      "maxTimestampMillis", minTimestamp + Duration.standardHours(1).getMillis)

    val window: SCollection[String] = sc
      .textFile(input)
      .timestampBy {
        _ => new Instant(ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp))
      }
      .withFixedWindows(windowSize) // apply windowing logic


    wordCount(window)
      .map { case (w, vs) =>
        val outputShard = "%s-%s-%s".format(
          args("output"), formatter.print(w.start()), formatter.print(w.end()))
        val resourceId = FileSystems.matchNewResource(outputShard, false)
        val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
        vs.foreach { case (k, v) => out.write(s"$k: $v\n".getBytes) }
        out.close()
      }

    sc.close()
  }

  def wordCount(input: SCollection[String]): SCollection[(IntervalWindow, Iterable[(String, Long)])] =
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .withWindow[IntervalWindow]
      .swap
      .groupByKey

}