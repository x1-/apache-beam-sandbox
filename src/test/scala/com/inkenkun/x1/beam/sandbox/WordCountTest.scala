package com.inkenkun.x1.beam.sandbox

import com.spotify.scio.testing._

class WordCountTest extends PipelineSpec {

  val inData = Seq("Lorem ipsum dolor sit amet", "consectetur dolor elit", "sed dolor ut lorem")
  val expected = Seq(
    "Lorem: 1", "ipsum: 1", "dolor: 3", "sit: 1", "amet: 1", "consectetur: 1", "elit: 1", "sed: 1", "ut: 1", "lorem: 1"
  )

  "WordCount" should "work" in {
    JobTest[com.inkenkun.x1.beam.sandbox.WordCount.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }
}
