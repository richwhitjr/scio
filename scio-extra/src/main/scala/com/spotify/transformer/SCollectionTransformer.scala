package com.spotify.transformer

import breeze.linalg.SparseVector
import com.spotify.scio.values.SCollection

import scala.language.implicitConversions

class SCollectionTransformer[T](collection: SCollection[T])
  extends java.io.Serializable {

  def featureAggregation(transformer: => Transforms[T]): SCollection[AggregatedFeatures] = {
    lazy val sg = FeatureMonoid(transformer)
    lazy val summer = transformer.transforms.map(_.transformer)

    collection
      .map{fet => transformer.transforms.map(_.prepare(fet))}
      .sum(sg)
      .map{fet =>
        AggregatedFeatures{fet.zip(summer).map{case(red, transform) => transform.present(red)}}
      }
  }

  def featureTransform(transformer: => Transforms[T]): SCollection[SparseVector[Double]] =
    collection
      .cross(featureAggregation(transformer))
      .map{case(fet, aggr) => transformer.toSparse(fet, aggr)}

  def featureTransformNamed(transformer: => Transforms[T])
    : (SCollection[SparseVector[Double]], SCollection[List[IndexedColumn]]) = {

    val aggregated = featureAggregation(transformer)

    val feats = collection
      .cross(aggregated)
      .map { case (fet, aggr) => transformer.toSparse(fet, aggr) }

    val labels = aggregated
      .map(a => Transforms.toColumnIndexed(transformer.transforms, a))

    (feats, labels)
  }

  def featureTransformKeyed(transformer: => Transforms[T])
  : (SCollection[(T, SparseVector[Double])], SCollection[List[IndexedColumn]]) = {

    val aggregated = featureAggregation(transformer)

    val feats = collection
      .cross(aggregated)
      .map{case (fet, aggr) => (fet, transformer.toSparse(fet, aggr))}

    val labels = aggregated
      .map(a => Transforms.toColumnIndexed(transformer.transforms, a))

    (feats, labels)
  }

  def featureTransformAggregation(transformer: => Transforms[T],
                                  aCol: SCollection[AggregatedFeatures])
    : SCollection[SparseVector[Double]] =
    collection
      .cross(aCol)
      .map{case(fet, aggr) => transformer.toSparse(fet, aggr)}

  def featureNames(transformer: => Transforms[T]): SCollection[List[IndexedColumn]] =
    featureAggregation(transformer)
      .map(a => Transforms.toColumnIndexed(transformer.transforms, a))
}

object SCollectionTransformer {
  implicit def convert[T](@transient col: SCollection[T]): SCollectionTransformer[T] =
    new SCollectionTransformer[T](col)
}
