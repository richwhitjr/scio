package com.spotify.transformer

import breeze.linalg.SparseVector
import com.spotify.transformer.transformers.Transformer

case class IndexedColumn(index: Int, name: String) {
  override def toString: String = s"$index,$name"
}

case class GetterTransformer[T, S, U, V](name: String,
                                         getter: T => Option[S],
                                         transformer: Transformer[S, U, V])
  extends java.io.Serializable {
  private val aggr = transformer.aggregator

  def prepare(t: T): Option[U] = getter(t).map(aggr.prepare)

  def toSparse(t: T, aggregation: Option[Any]): SparseVector[Double] =
    transformer.toSparseVector(getter(t), aggregation)

  def toColumns(aggregation: Option[Any]): List[String] =
    transformer.toColumnsAny(name, aggregation)
}

case class Transforms[T](transforms: List[GetterTransformer[T, _, _, _]])
  extends java.io.Serializable {
  def toSparse(fet: T, aggregatedFeatures: AggregatedFeatures): SparseVector[Double] = {
    val vecs = aggregatedFeatures.aggr.zip(transforms).map{case(aggr, transformer) =>
      transformer.toSparse(fet, aggr)
    }
    SparseVector.vertcat(vecs:_*)
  }
}

object Transforms {
  def collect[T](transforms: Transforms[T]*): Transforms[T] =
    Transforms(transforms.flatMap(_.transforms).toList)

  def toColumnIndexed[T](transforms: => List[GetterTransformer[T, _, _, _]],
                      aggregatedFeatures: AggregatedFeatures): List[IndexedColumn] =
    aggregatedFeatures.aggr.zip(transforms).flatMap{case(aggr, transformer) =>
        transformer.toColumns(aggr)
      }
      .zipWithIndex
      .map{case(name, idx) => IndexedColumn(idx, name)}
}
