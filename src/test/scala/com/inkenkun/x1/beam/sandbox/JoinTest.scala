package com.inkenkun.x1.beam.sandbox

import com.spotify.scio.testing._

class JoinTest extends PipelineSpec {

  val inData1 = Seq(
    """{"unique":"pR9nvOTz68KYevE7dS7ZVZhk","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p1","rad":1,"num1":2,"num2":1}""",
    """{"unique":"XuRl81GldfMBC0qjbBz7lAtt","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p2","rad":1,"num1":3,"num2":1}""",
    """{"unique":"xAaFwjZ9wUHMBkl8F4780twe","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p3","rad":1,"num1":4,"num2":1}""",
    """{"unique":"sdBLA0reIybE1SwVLD3xHNLd","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p4","rad":1,"num1":5,"num2":1}""",
    """{"unique":"1sSntFWXEPFGd2i01J2Jy78a","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p5","rad":1,"num1":6,"num2":1}""",
    """{"unique":"mFe6qxgAFVdJvbVvFmzE7cuF","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p1","rad":2,"num1":7,"num2":1}""",
    """{"unique":"ePIUG7IbKSzltkxYF4kSgrIJ","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p1","rad":2,"num1":1000,"num2":1}""",
    """{"unique":"BkvnNfUkAiaatckwG7nKkGU6","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p2","rad":2,"num1":8,"num2":1}""",
    """{"unique":"emycq6Hpcnk06yAwlM3bemZ9","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p3","rad":2,"num1":9,"num2":1}""",
    """{"unique":"pE5QWjWOgxMgi2LGXKC6nxBb","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p4","rad":2,"num1":10,"num2":1}"""
  )
  val inData2 = Seq(
    """{"unique":"pR9nvOTz68KYevE7dS7ZVZhz","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p1","rad":1,"num1":100,"num2":1}""",
    """{"unique":"XuRl81GldfMBC0qjbBz7lAts","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p2","rad":1,"num1":100,"num2":1}""",
    """{"unique":"xAaFwjZ9wUHMBkl8F4780tww","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p3","rad":1,"num1":100,"num2":1}""",
    """{"unique":"sdBLA0reIybE1SwVLD3xHNLe","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p4","rad":1,"num1":100,"num2":1}""",
    """{"unique":"1sSntFWXEPFGd2i01J2Jy78r","time":"2017-09-20 01:02:00 UTC","sz":"s1","ps":"p8","rad":9,"num1":2000,"num2":20}""",
  )
  val expected = Seq(
    "1, p1, 2, 1, 100, 1",
    "1, p2, 3, 1, 100, 1",
    "1, p3, 4, 1, 100, 1",
    "1, p4, 5, 1, 100, 1",
    "1, p5, 6, 1, 0, 0",
    "2, p1, 1007, 2, 0, 0",
    "2, p2, 8, 1, 0, 0",
    "2, p3, 9, 1, 0, 0",
    "2, p4, 10, 1, 0, 0"
  )

  "Join" should "work" in {
    JobTest[com.inkenkun.x1.beam.sandbox.Join.type]
      .args("--input1=in1.txt", "--input2=in2.txt", "--output=out.txt")
      .input(TextIO("in1.txt"), inData1)
      .input(TextIO("in2.txt"), inData2)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
