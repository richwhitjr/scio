package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.testing._

object BigQueryTornadoesTest {

  val input = Seq(
    (1, true),
    (1, false),
    (2, false),
    (3, true),
    (4, true),
    (4, true)
  ).map(t => TableRow("month" -> t._1, "tornado" -> t._2))

  val expected = Seq((1, 1), (3, 1), (4, 2)).map(t => TableRow("month" -> t._1, "tornado_count" -> t._2))

}

class BigQueryTornadoesTest extends PipelineSpec {

  import BigQueryTornadoesTest._

  "BigQueryTornadoes" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.BigQueryTornadoes")
      .args("--input=publicdata:samples.gsod", "--output=dataset.table")
      .input(BigQueryIO("publicdata:samples.gsod"), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}