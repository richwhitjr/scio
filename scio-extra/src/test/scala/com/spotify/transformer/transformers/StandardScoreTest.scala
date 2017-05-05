package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.Moments
import org.scalatest.{FlatSpec, Matchers}

class StandardScoreTest extends FlatSpec with Matchers {
  "StandardScore" should "standard" in {
    val values = List(1.0, 10.0, 3.0, 25.0, 1000.0, 1.0, 1.0, 1.0, 4.0)

    val aggregator = StandardScore.aggregator

    val aggr = aggregator.present{
      values
        .map(aggregator.prepare)
        .reduceLeft[Moments]{case(l, r) => aggregator.semigroup.plus(l, r)}
    }

    val standards = values.map(v => StandardScore.toSparse(v, aggr))

    val expected = List(
      SparseVector(1)((0, -0.3686525391512648)),
      SparseVector(1)((0, -0.3398571142030946)),
      SparseVector(1)((0, -0.3622535558294492)),
      SparseVector(1)((0, -0.2918647392894777)),
      SparseVector(1)((0, 2.827639630095622)),
      SparseVector(1)((0, -0.3686525391512648)),
      SparseVector(1)((0, -0.3686525391512648)),
      SparseVector(1)((0, -0.3686525391512648)),
      SparseVector(1)((0, -0.35905406416854135))
    )

    standards should contain allElementsOf expected
  }
}
