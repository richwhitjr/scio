package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.QTree
import org.scalatest.{FlatSpec, Matchers}

class BinnerTest extends FlatSpec with Matchers {
  "Binner" should "bin" in {
    val binner = Binner[Double](5)

    val values = List(1.0, 10.0, 3.0, 25.0, 1000.0, 1.0, 1.0, 1.0, 4.0)

    val aggregator = binner.aggregator

    val qTree = values
      .map(aggregator.prepare)
      .reduceLeft[QTree[Double]]{case(l, r) => aggregator.semigroup.plus(l, r)}

    val aggr = aggregator.present(qTree)

    val bins = values.map(v => binner.toSparse(v, aggr))

    val expected = List(
      SparseVector(5)((2,1.0)),
      SparseVector(5)((4,1.0)),
      SparseVector(5)((4,1.0)),
      SparseVector(5)((0,1.0)),
      SparseVector(5)((0,1.0)),
      SparseVector(5)((0,1.0)),
      SparseVector(5)((3,1.0))
    )

    bins should contain allElementsOf expected
  }
}
