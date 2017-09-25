package com.inkenkun.x1.beam.sandbox

import io.circe
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import com.spotify.scio._
import com.spotify.scio.values.SCollection


/*
SBT
runMain
  com.inkenkun.x1.beam.sandbox.Join
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input1=gs://[BUCKET]/[PATH]/[FILE]
  --input2=gs://[BUCKET]/[PATH]/[FILE]
  --output=gs://[BUCKET]/[PATH]/[DIR]
*/
object Join {
  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val in1: SCollection[((Long, String), Iterable[Record])] =
      sc.textFile(args("input1"))
        .map(convertRecord)
        .filter(_.isRight)
        .map(_.right.get)
        .keyBy(r => (r.rad, r.ps))
        .groupByKey

    val in2 =
      sc.textFile(args("input2"))
        .map(convertRecord)
        .filter(_.isRight)
        .map(_.right.get)
        .keyBy(r => (r.rad, r.ps))
        .groupByKey
        .asMapSideInput


    in1.withSideInputs(in2)
      .map { (m1, side) =>
        val ((rad: Long, ps: String), json: Iterable[Record]) = m1
        val m2: Map[(Long, String), Iterable[Record]] = side(in2)

        val m1sum = json.foldLeft((0L, 0L)){(acc, j) =>
          (acc._1 + j.num1, acc._2 + j.num2)
        }
        val m2sum = m2.getOrElse((rad, ps), Iterable.empty[Record]).foldLeft((0L, 0L)){(acc, j) =>
          (acc._1 + j.num1, acc._2 + j.num2)
        }
        s"${rad}, ${ps}, ${m1sum._1}, ${m1sum._2}, ${m2sum._1}, ${m2sum._2}"
      }
    .toSCollection
    .saveAsTextFile(args("output"))
    sc.close()
  }

  def convertRecord(s: String): Either[circe.Error, Record] = decode[Record](s)

  case class Record (
    unique: String,
    time  : String,
    sz    : String,
    ps    : String,
    rad   : Long,
    num1  : Long,
    num2  : Long
  )
}
