package com.spotify.transformer.transformers

import breeze.linalg.SparseVector
import com.twitter.algebird.Aggregator

import scala.reflect.runtime.universe.TypeTag

abstract class Transformer[S, V, U](implicit vt: TypeTag[V], uT: TypeTag[U])
  extends java.io.Serializable {
  def size: Int = 1

  def toColumnsAny(name: String, aggr: Option[Any]): List[String] =
    toColumns(name, aggr.map(_.asInstanceOf[U]))

  def toColumns(name: String, aggr: Option[U]): List[String] =
    List(name)

  def toSparseVector(value: Option[S], aggr: Option[Any]): SparseVector[Double] =
    toSparseOpt(value, aggr.map(_.asInstanceOf[U]))

  def toSparseOpt(value: Option[S], aggregation: Option[U]): SparseVector[Double] =
    (for{
      v <- value
      aggr <- aggregation
    } yield toSparse(v, aggr)).getOrElse(SparseVector.zeros[Double](size))

  def toSparse(value: S, aggregation: U): SparseVector[Double] =
    SparseVector.zeros[Double](size)

  def aggregator: Aggregator[S, V, U]

  def sum(l: Any, r: Any): Any =
    aggregator.semigroup.combine(r.asInstanceOf[V], l.asInstanceOf[V])

  def present(item: Option[Any]): Option[Any] =
    item.map(i => aggregator.present(i.asInstanceOf[V]))
}
