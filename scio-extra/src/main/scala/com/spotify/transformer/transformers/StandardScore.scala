package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.{Aggregator, Moments, MomentsAggregator}

case object StandardScore extends Transformer[Double, Moments, Moments] {
  override def toSparse(value: Double, aggregation: Moments): SparseVector[Double] =
    SparseVector(1)((0, (value - aggregation.mean)/aggregation.stddev))

  def aggregator: Aggregator[Double, Moments, Moments] = MomentsAggregator
}
