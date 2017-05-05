package com.spotify.transformer.transformers

import breeze.linalg.{SparseVector, VectorBuilder}
import com.twitter.algebird.{Aggregator, Semigroup}

case class NHotEncoder() extends Transformer[List[String], Set[String], List[String]] {
  override def toSparseOpt(value: Option[List[String]], aggregation: Option[List[String]])
    : SparseVector[Double] = {
    (for{
      v <- value
      aggr <- aggregation
    } yield {
      val enums = v.toSet
      val dims = aggr.zipWithIndex.collect{case(f, idx) if enums.contains(f) =>
        (idx, 1.0)
      }
      VectorBuilder[Double](aggr.size)(dims:_*).toSparseVector
    }).getOrElse(SparseVector.zeros[Double](aggregation.map(_.size).getOrElse(0)))
  }

  override def toColumns(name: String, aggr: Option[List[String]]): List[String] =
    aggr.map(_.map(e => name.replaceAll(" ","_") + "_" + e)).getOrElse(Nil)

  def aggregator: Aggregator[List[String], Set[String], List[String]] = {
    new Aggregator[List[String], Set[String], List[String]] {
      def prepare(input: List[String]): Set[String] =
        input.toSet

      def semigroup: Semigroup[Set[String]] =
        Semigroup.from[Set[String]]{case(l, r) => l.union(r)}

      def present(reduction: Set[String]): List[String] =
        reduction.toList.sorted
    }
  }
}
