package com.spotify.transformer

import breeze.linalg.SparseVector
import com.spotify.scio.testing.PipelineSpec

case class Feature(
                    enum: String = "",
                    bool: Boolean = false,
                    num: Int = 0,
                    vec: List[Double] = Nil)

object TestFunctions {
  lazy val nHotTransformer = Transformer
    .builder[Feature]
    .addNHotEncodedFeature("fet", v => Some(List(v.enum)))
    .build

  lazy val fetTransformer = Transformer
    .builder[Feature]
    .addBooleanFeature("fet", v => Some(v.bool))
    .build

  lazy val fetNumTransformer = Transformer
    .builder[Feature]
    .addNumericFeature("fet", v => Some(v.num))
    .build

  lazy val vecTransformer = Transformer
    .builder[Feature]
    .addVectorFeature("fet", v => Some(v.vec), 1)
    .build

  lazy val binTransformer = Transformer
    .builder[Feature]
    .addBinnedFeature("fet", v => Some(v.num.toDouble))
    .build
}

class TransformsTest extends PipelineSpec {
  import com.spotify.transformer.SCollectionTransformer._

  "NHotEncoderBack" should "work" in {
    runWithContext { sc =>
      val res = sc
        .parallelize(List(Feature("one"), Feature("two")))
        .featureTransform(TestFunctions.nHotTransformer)

      val expected = List(
        SparseVector[Double](2)((0, 1.0)),
        SparseVector[Double](2)((1, 1.0))
      )

      res should containInAnyOrder (expected)
    }
  }

  "addFeatureBool" should "work" in {
    runWithContext { sc =>
      val res = sc
        .parallelize(List(Feature(bool = true), Feature(bool = false)))
        .featureTransform(TestFunctions.fetTransformer)

      val expected = List(
        SparseVector[Double](1)((0, 1.0)),
        SparseVector.zeros[Double](1)
      )

      res should containInAnyOrder (expected)
    }
  }

  "addFeatureInt" should "work" in {
    runWithContext { sc =>
      val res = sc
        .parallelize(List(Feature(num = 1), Feature(num = 2)))
        .featureTransform(TestFunctions.fetNumTransformer)

      val expected = List(
        SparseVector[Double](1)((0, 1.0)),
        SparseVector[Double](1)((0, 2.0))
      )

      res should containInAnyOrder (expected)
    }
  }

  "flatten" should "work" in {
    runWithContext { sc =>
      val res = sc
        .parallelize(List(Feature(vec = List(1.0)), Feature(vec = List(2.0))))
        .featureTransform(TestFunctions.vecTransformer)

      val expected = List(
        SparseVector[Double](1)((0, 1.0)),
        SparseVector[Double](1)((0, 2.0))
      )

      res should containInAnyOrder (expected)
    }
  }

  "binner" should "work" in {
    runWithContext { sc =>
      val res = sc
        .parallelize(List(Feature(num = 1), Feature(num = 10)))
        .featureTransform(TestFunctions.binTransformer)

      val expected = List(
        SparseVector[Double](5)((0, 1.0)),
        SparseVector[Double](5)((2, 1.0))
      )

      res should containInAnyOrder(expected)
    }
  }
}

