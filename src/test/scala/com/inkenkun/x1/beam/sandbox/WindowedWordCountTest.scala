package com.inkenkun.x1.beam.sandbox

import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{Duration, Instant}

class WindowedWordCountTest extends PipelineSpec {

  val inData = Seq(
    ("Lorem ipsum dolor sit amet", new Instant(1)),
    ("consectetur dolor elit", new Instant(1)),
    ("sed dolor ut lorem", new Instant(2))
  )
  val expected = Seq(
    Iterable(
      "Lorem: 1",
      "amet: 1",
      "consectetur: 1",
      "dolor: 3",
      "elit: 1",
      "ipsum: 1",
      "lorem: 1",
      "sed: 1",
      "sit: 1",
      "ut: 1"
    )
  )

  "WindowedWordCount" should "work" in {
    runWithContext { sc =>
      val in = sc.parallelizeTimestamped(inData).withFixedWindows(new Duration(3))

      val r: SCollection[Iterable[String]] = WindowedWordCount.wordCount(in)
        .map { case (w, vs) =>
           vs.toSeq.sortBy(_._1).map {
             case (k, v) => s"${k}: $v"
           }
        }
      r should containInAnyOrder(expected)
    }
  }
}
