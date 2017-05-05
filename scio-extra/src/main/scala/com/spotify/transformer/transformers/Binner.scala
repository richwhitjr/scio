package com.spotify.transformer.transformers

import breeze.linalg.{SparseVector, VectorBuilder}
import com.twitter.algebird.{Aggregator, QTree, QTreeSemigroup, Semigroup}

import scala.reflect.runtime.universe.TypeTag

case class Binner[S](bins: Int)(implicit num: Numeric[S])
  extends Transformer[S, QTree[Double], List[Double]] {

  override def size: Int = bins

  protected def typeTag: TypeTag[QTree[Double]] = implicitly[TypeTag[QTree[Double]]]

  override def toColumns(name: String, aggr: Option[List[Double]]): List[String] =
    0.until(bins).map(e => name + "_" + e).toList

  override def toSparse(value: S, aggregation: List[Double]): SparseVector[Double] =
    VectorBuilder[Double](bins)((aggregation.indexWhere(_ > num.toDouble(value))-1, 1.0))
      .toSparseVector

  def aggregator: Aggregator[S, QTree[Double], List[Double]] = {
    new Aggregator[S, QTree[Double], List[Double]] {
      def prepare(input: S): QTree[Double] = QTree.apply(num.toDouble(input))

      def semigroup: Semigroup[QTree[Double]] =
        new QTreeSemigroup[Double](8)

      def present(reduction: QTree[Double]): List[Double] =
        0.until(bins-1).scanLeft(0.0){case(lower, r) =>
          val (l, u) = reduction.quantileBounds(r/(bins-1).toDouble)
          l + ((u - l)/2.0)
        }.toList :+ reduction.upperBound
    }
  }
}
