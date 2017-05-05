package com.spotify.transformer.transformers

import breeze.linalg.{SparseVector, VectorBuilder}
import com.twitter.algebird.Aggregator

import scala.reflect.runtime.universe.TypeTag

case class Flatten(length: Int) extends Transformer[List[Double], Unit, Unit]{
  protected def typeTag: TypeTag[Unit] = implicitly[TypeTag[Unit]]

  override def toColumns(name: String, aggr: Option[Unit]): List[String] =
    0.until(length).map(e => name + "_" + e).toList

  override def toSparseOpt(value: Option[List[Double]], aggregation: Option[Unit])
    : SparseVector[Double] =
    value.map{v =>
      VectorBuilder[Double](length)(v.zipWithIndex.map{case(vl, i) => (i, vl)}:_*)
        .toSparseVector
    }.getOrElse(SparseVector.zeros[Double](length))

  def aggregator: Aggregator[List[Double], Unit, Unit] = new UnitAggregator[List[Double]]()
}
