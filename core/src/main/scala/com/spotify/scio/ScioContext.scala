package com.spotify.scio

import java.beans.Introspector
import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.datastore.DatastoreV1.{Entity, Query}
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.io.{AvroIO => GAvroIO, BigQueryIO => GBigQueryIO, DatastoreIO => GDatastoreIO, PubsubIO => GPubsubIO, TextIO => GTextIO}
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled
import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob
import com.google.cloud.dataflow.sdk.testing.TestPipeline
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.{Create, PTransform}
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput, TimestampedValue}
import com.spotify.scio.bigquery._
import com.spotify.scio.testing._
import com.spotify.scio.util.CallSites
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Set => MSet}
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

/** Convenience object for creating [[ScioContext]] and [[Args]]. */
object ContextAndArgs {
  /** Create [[ScioContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (ScioContext, Args) = {
    val (_opts, _args) = ScioContext.parseArguments[DataflowPipelineOptions](args)
    (new ScioContext(_opts, _args.optional("testId")), _args)
  }
}

/** Companion object for [[ScioContext]]. */
object ScioContext {

  /** Create a new [[ScioContext]] instance. */
  def apply(): ScioContext = ScioContext(defaultOptions)

  /** Create a new [[ScioContext]] instance. */
  def apply(options: DataflowPipelineOptions): ScioContext = new ScioContext(options, None)

  /** Create a new [[ScioContext]] instance for testing. */
  def forTest(testId: String): ScioContext = new ScioContext(defaultOptions, Some(testId))

  /** Parse PipelineOptions and application arguments from command line arguments. */
  def parseArguments[T <: PipelineOptions : ClassTag](cmdlineArgs: Array[String]): (T, Args) = {
    val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val dfPatterns = cls.getMethods.flatMap { m =>
      val n = m.getName
      if ((!n.startsWith("get") && !n.startsWith("is")) ||
        m.getParameterTypes.nonEmpty || m.getReturnType == classOf[Unit]) {
        None
      } else {
        Some(Introspector.decapitalize(n.substring(if (n.startsWith("is")) 2 else 3)))
      }
    }.map(s => s"--$s($$|=)".r)
    val (dfArgs, appArgs) = cmdlineArgs.partition(arg => dfPatterns.exists(_.findFirstIn(arg).isDefined))

    (PipelineOptionsFactory.fromArgs(dfArgs).as(cls), Args(appArgs))
  }

  private val defaultOptions: DataflowPipelineOptions =
    PipelineOptionsFactory.fromArgs(Array.empty).as(classOf[DataflowPipelineOptions])

}

/**
 * Main entry point for Dataflow functionality. A ScioContext represents a Dataflow pipeline,
 * and can be used to create SCollections and distributed caches on that cluster.
 *
 * @groupname accumulator Accumulators
 * @groupname dist_cache Distributed Cache
 * @groupname in_memory In-memory Collections
 * @groupname input Input Sources
 * @groupname Ungrouped Other Members
 */
class ScioContext private[scio] (val options: DataflowPipelineOptions, testId: Option[String]) {

  import Implicits._

  /** Dataflow pipeline. */
  val pipeline: Pipeline = this.newPipeline()

  /* Mutable members */
  private var _isClosed: Boolean = false
  private val _promises: ListBuffer[(Promise[AnyRef], AnyRef)] = ListBuffer.empty
  private val _accumulators: MSet[String] = MSet.empty

  /** Wrap a [[com.google.cloud.dataflow.sdk.values.PCollection PCollection]]. */
  def wrap[T: ClassTag](p: PCollection[T]): SCollection[T] =
    new SCollectionImpl[T](p, this)

  // =======================================================================
  // Miscellaneous
  // =======================================================================

  private lazy val bigQueryClient: BigQueryClient =
    BigQueryClient(options.getProject, options.getGcpCredential)

  private def newPipeline(): Pipeline = {
    // override app name and job name
    options.setAppName(CallSites.getAppName)
    options.setJobName(new DataflowPipelineOptions.JobNameFactory().create(options))

    val p = if (testId.isEmpty) {
      Pipeline.create(options)
    } else {
      TestPipeline.create()
    }
    p.getOptions.setStableUniqueNames(CheckEnabled.WARNING)
    p.getCoderRegistry.registerScalaCoders()
    p
  }

  // =======================================================================
  // States
  // =======================================================================

  /** Close the context. No operation can be performed once the context is closed. */
  def close(): ScioResult = {
    _isClosed = true
    val result = this.pipeline.run()

    val finalState = result match {
      // non-blocking runner, handle callbacks asynchronously
      case job: DataflowPipelineJob =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val f = Future {
          val state = job.waitToFinish(-1, TimeUnit.SECONDS, null)
          updateFutures(state)
          state
        }
        f.onFailure {
          case e: Throwable => _promises.foreach(_._1.failure(e))
        }
        f
      // blocking runner, handle callbacks directly
      case _ =>
        updateFutures(result.getState)
        Future.successful(result.getState)
    }

    new ScioResult(result, finalState, pipeline)
  }

  /** Whether the context is closed. */
  def isClosed: Boolean = _isClosed

  /* Ensure an operation is called before the pipeline is closed. */
  private[scio] def pipelineOp[T](body: => T): T = {
    require(!this.isClosed, "ScioContext already closed")
    body
  }

  // =======================================================================
  // Futures
  // =======================================================================

  /* To be updated once the pipeline completes. */
  private[scio] def makeFuture[T](value: AnyRef): Future[T] = {
    val p = Promise[AnyRef]()
    _promises.append((p, value))
    p.future.asInstanceOf[Future[T]]
  }

  private def updateFutures(state: State): Unit = _promises.foreach { kv =>
    if (state == State.DONE || state == State.UPDATED) {
      kv._1.success(kv._2)
    } else {
      kv._1.failure(new RuntimeException("Dataflow pipeline failed to complete: " + state))
    }
  }

  // =======================================================================
  // Test wiring
  // =======================================================================

  private implicit def context: ScioContext = this

  private[scio] def isTest: Boolean = testId.isDefined

  private[scio] def testIn: TestInput = TestDataManager.getInput(testId.get)
  private[scio] def testOut: TestOutput = TestDataManager.getOutput(testId.get)
  private[scio] def testDistCache: TestDistCache = TestDataManager.getDistCache(testId.get)

  private[scio] def getTestInput[T: ClassTag](key: TestIO[T]): SCollection[T] =
    this.parallelize(testIn(key).asInstanceOf[Seq[T]])

  // =======================================================================
  // Read operations
  // =======================================================================

  private[scio] def applyInternal[Output <: POutput](root: PTransform[_ >: PBegin, Output]): Output =
    pipeline.apply(CallSites.getCurrent, root)

  /**
   * Get an SCollection of specific record type for an Avro file.
   * @group input
   */
  def avroFile[T: ClassTag](path: String): SCollection[T] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val o = this.applyInternal(GAvroIO.Read.from(path).withSchema(cls))
      wrap(o).setName(path)
    }
  }

  /**
   * Get an SCollection of generic record type for an Avro file.
   * @group input
   */
  def avroFile(path: String, schema: Schema): SCollection[GenericRecord] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      wrap(this.applyInternal(GAvroIO.Read.from(path).withSchema(schema))).setName(path)
    }
  }

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * @group input
   */
  def bigQuerySelect(sqlQuery: String): SCollection[TableRow] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else {
      val table = this.bigQueryClient.queryIntoTable(sqlQuery)
      bigQueryTable(table).setName(sqlQuery)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] = pipelineOp {
    val tableSpec: String = GBigQueryIO.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      wrap(this.applyInternal(GBigQueryIO.Read.from(table))).setName(tableSpec)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(GBigQueryIO.parseTableSpec(tableSpec))

  /**
   * Get an SCollection for a Datastore query.
   * @group input
   */
  def datastore(datasetId: String, query: Query): SCollection[Entity] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(DatastoreIO(datasetId, query))
    } else {
      wrap(this.applyInternal(GDatastoreIO.readFrom(datasetId, query)))
    }
  }

  /**
   * Get an SCollection for a Pub/Sub subscription.
   * @group input
   */
  def pubsubSubscription(sub: String, idLabel: String = null, timestampLabel: String = null): SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(PubsubIO(sub))
    } else {
      var transform = GPubsubIO.Read.subscription(sub)
      if (idLabel != null) transform = transform.idLabel(idLabel)
      if (timestampLabel != null) transform = transform.timestampLabel(timestampLabel)
      wrap(this.applyInternal(transform)).setName(sub)
    }
  }

  /**
   * Get an SCollection for a Pub/Sub topic.
   * @group input
   */
  def pubsubTopic(topic: String, idLabel: String = null, timestampLabel: String = null): SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(PubsubIO(topic))
    } else {
      var transform = GPubsubIO.Read.topic(topic)
      if (idLabel != null) transform = transform.idLabel(idLabel)
      if (timestampLabel != null) transform = transform.timestampLabel(timestampLabel)
      wrap(this.applyInternal(transform)).setName(topic)
    }
  }

  /**
   * Get an SCollection of TableRow for a JSON file.
   * @group input
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(TableRowJsonIO(path))
    } else {
      wrap(this.applyInternal(GTextIO.Read.from(path).withCoder(TableRowJsonCoder.of()))).setName(path)
    }
  }

  /**
   * Get an SCollection for a text file.
   * @group input
   */
  def textFile(path: String): SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      wrap(this.applyInternal(GTextIO.Read.from(path))).setName(path)
    }
  }

  // =======================================================================
  // Accumulators
  // =======================================================================

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the maximum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def maxAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator $n already exists")
    _accumulators.add(n)
    new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.maxFn()
    }
  }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the minimum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def minAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator $n already exists")
    _accumulators.add(n)
    new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.minFn()
    }
  }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the sum of values. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def sumAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator $n already exists")
    _accumulators.add(n)
    new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.sumFn()
    }
  }

  // =======================================================================
  // In-memory collections
  // =======================================================================

  /**
   * Distribute a local Scala Iterable to form an SCollection.
   * @group in_memory
   */
  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder))).setName(elems.toString())
  }

  /**
   * Distribute a local Scala Map to form an SCollection.
   * @group in_memory
   */
  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder))).map(kv => (kv.getKey, kv.getValue))
      .setName(elems.toString())
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)]): SCollection[T] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder))).setName(elems.toString())
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T],
                                          timestamps: Iterable[Instant]): SCollection[T] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder))).setName(elems.toString())
  }

  // =======================================================================
  // Distributed cache
  // =======================================================================

  /**
   * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance.
   * @param uri Google Cloud Storage URI of the file to be distributed to all workers
   * @param initFn function to initialized the distributed file
   *
   * {{{
   * // Prepare distributed cache as Map[Int, String]
   * val dc = sc.distCache("gs://dataflow-samples/samples/misc/months.txt") { f =>
   *   scala.io.Source.fromFile(f).getLines().map { s =>
   *     val t = s.split(" ")
   *     (t(0).toInt, t(1))
   *   }.toMap
   * }
   *
   * val p: SCollection[Int] = // ...
   * // Extract distributed cache inside a transform
   * p.map(x => dc().getOrElse(x, "unknown"))
   * }}}
   * @group dist_cache
   */
  def distCache[F](uri: String)(initFn: File => F): DistCache[F] = pipelineOp {
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uri)))
    } else {
      new DistCacheSingle(new URI(uri), initFn, options)
    }
  }

  /**
   * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance.
   * @param uris Google Cloud Storage URIs of the files to be distributed to all workers
   * @param initFn function to initialized the distributed files
   * @group dist_cache
   */
  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] = pipelineOp {
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uris.mkString("\t"))))
    } else {
      new DistCacheMulti(uris.map(new URI(_)), initFn, options)
    }
  }

}