package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.Aggregator

import scala.reflect.runtime.universe.TypeTag

case class PassValue[S]()(implicit numeric: Numeric[S]) extends Transformer[S, Unit, Unit] {
  protected def typeTag: TypeTag[Unit] = implicitly[TypeTag[Unit]]

  override def toSparseOpt(value: Option[S], aggregation: Option[Unit])
    : SparseVector[Double] =
    SparseVector.apply(value.map(numeric.toDouble).getOrElse(0.0))

  def aggregator: Aggregator[S, Unit, Unit] = new UnitAggregator[S]()
}
