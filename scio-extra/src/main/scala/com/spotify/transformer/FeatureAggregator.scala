package com.spotify.transformer

import com.spotify.transformer.transformers.Transformer
import com.twitter.algebird.Semigroup

case class AggregatedFeatures(aggr: List[Option[Any]])

object FeatureMonoid {
  def summer[T](transformer: Transforms[T]): List[Transformer[_, _, _]] =
    transformer.transforms.map(_.transformer)

  def apply[T](@transient transformer: => Transforms[T]): Semigroup[List[Option[Any]]] = {
    @transient lazy val sum = summer(transformer)
    Semigroup.from[List[Option[Any]]]{case(ll, rl) =>
      ll.zip(rl).zip(sum).map{
        case((Some(l), Some(r)), transform) => Some(transform.sum(l, r))
        case((None, Some(r)), _) => Some(r)
        case((Some(l), None), _) => Some(l)
        case _ => None
      }
    }
  }
}
