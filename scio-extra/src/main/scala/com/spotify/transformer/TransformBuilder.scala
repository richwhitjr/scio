package com.spotify.transformer

import com.spotify.transformer.transformers._
import com.twitter.algebird.{Moments, QTree}

class TransformBuilder[T](transforms: Transforms[T]) {
  private val DEFAULT_BINS = 5

  def addNumericFeature[S](name: String, fn: T => Option[S])
                          (implicit num: Numeric[S]): TransformBuilder[T] = {
    val transformer = GetterTransformer[T, S, Unit, Unit](name, fn, PassValue())
    new TransformBuilder(Transforms(transforms.transforms :+ transformer))
  }

  def addBooleanFeature(name: String, fn: T => Option[Boolean]): TransformBuilder[T] = {
    val transformer = GetterTransformer[T, Boolean, Unit, Unit](name, fn, PassBooleanValue())
    new TransformBuilder(Transforms(transforms.transforms :+ transformer))
  }

  def addVectorFeature(name: String, fn: T => Option[List[Double]], length: Int)
    : TransformBuilder[T] = {
    val getter = GetterTransformer[T, List[Double], Unit, Unit](name, fn, Flatten(length))
    new TransformBuilder(Transforms(transforms.transforms :+ getter))
  }

  def addBinnedFeature[S](name: String, fn: T => Option[S], bins: Int = DEFAULT_BINS)
                         (implicit num: Numeric[S]): TransformBuilder[T] = {
    val getter = GetterTransformer[T, S, QTree[Double], List[Double]](name, fn, Binner[S](bins))
    new TransformBuilder(Transforms(transforms.transforms :+ getter))
  }

  def addNHotEncodedFeature(name: String, fn: T => Option[List[String]]): TransformBuilder[T] = {
    val getter =
      GetterTransformer[T, List[String], Set[String], List[String]](name, fn, NHotEncoder())
    new TransformBuilder(Transforms(transforms.transforms :+ getter))
  }

  def addStandardFeature(name: String, fn: T => Option[Double]): TransformBuilder[T] = {
    val getter = GetterTransformer[T, Double, Moments, Moments](name, fn, StandardScore)
    new TransformBuilder(Transforms(transforms.transforms :+ getter))
  }

  def build: Transforms[T] = transforms
}

object Transformer {
  def builder[T]: TransformBuilder[T] =
    new TransformBuilder[T](Transforms(Nil))
}
