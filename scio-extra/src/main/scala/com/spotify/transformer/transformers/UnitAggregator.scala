package com.spotify.transformer.transformers

import com.twitter.algebird.{Aggregator, Semigroup}

class UnitAggregator[T] extends Aggregator[T, Unit, Unit] {
  def prepare(input: T): Unit = ()
  def semigroup: Semigroup[Unit] = Semigroup.from[Unit]{case(l, r) => ()}
  def present(reduction: Unit): Unit = ()
}
