package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.Aggregator

import scala.reflect.runtime.universe.TypeTag

case class PassBooleanValue[T]() extends Transformer[Boolean, Unit, Unit] {
  protected def typeTag: TypeTag[Unit] = implicitly[TypeTag[Unit]]

  override def toSparseOpt(value: Option[Boolean], aggregation: Option[Unit])
    : SparseVector[Double] =
    SparseVector.apply(if(value.exists(identity)) 1.0 else 0.0)

  def aggregator: Aggregator[Boolean, Unit, Unit] =
    new UnitAggregator[Boolean]()
}
