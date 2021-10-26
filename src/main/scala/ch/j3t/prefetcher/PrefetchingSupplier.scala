package ch.j3t.prefetcher

import zio.duration.{ Duration, _ }
import zio.logging._
import zio._
import zio.metrics.dropwizard._
import zio.metrics.dropwizard.helpers._
import java.time.Instant

import zio.stream.ZStream

/**
 * This class is akin to a Supplier[T] that will always have a T immediately available,
 * while it is updated by a background job on a regular basis.
 *
 * @tparam T the type of the pre-fetched value
 */
trait PrefetchingSupplier[T] {

  /**
   * @return an IO wrapping the value currently held by this pre-fetcher.
   */
  def get: IO[Nothing, T]

  /**
   * @return the time of the last successful update
   */
  def lastSuccessfulUpdate: IO[Nothing, Instant]

  /**
   * @return the interval at which this prefetcher is updated
   */
  def updateInterval: Duration

  /**
   * @return the stream of updated prefetcher values
   */
  def updatesStream: ZStream[Any, Nothing, T]

}

/**
 * Concrete implementation for the PrefetchingSupplier trait. Should be used most of the time.
 *
 * @param prefetchedValueRef the Ref pointing to the currently held pre-fetched value
 * @param updateFiber the fiber running the regular update job
 * @tparam T the type of the pre-fetched value
 */
class LivePrefetchingSupplier[T] private[prefetcher] (
  prefetchedValueRef: Ref[T],
  lastOkUpdate: Ref[Instant],
  hub: Hub[T],
  val updateInterval: Duration,
  val updateFiber: Fiber[Throwable, Any]
) extends PrefetchingSupplier[T] {

  val currentValueRef = prefetchedValueRef.readOnly

  val get = prefetchedValueRef.get

  /**
   * @return the elapsed duration since the last successful update
   */
  def lastSuccessfulUpdate: IO[Nothing, Instant] = lastOkUpdate.get

  def updatesStream: ZStream[Any, Nothing, T] = PrefetchingSupplier.setupUpdatesStream[T](hub, get)
}

/**
 * Static implementation of the prefetching supplier trait.
 *
 * Mostly useful for tests.
 *
 * @param get the forever fixed value to be returned
 * @tparam T the type of the pre-fetched value
 */
class StaticPrefetchingSupplier[T] private[prefetcher] (
  hub: Hub[T],
  val get: UIO[T],
  val updateInterval: Duration
) extends PrefetchingSupplier[T] {

  override def lastSuccessfulUpdate: IO[Nothing, Instant] = IO.succeed(Instant.now())

  def updatesStream: ZStream[Any, Nothing, T] =
    PrefetchingSupplier.setupUpdatesStream[T](hub, get)
}

object PrefetchingSupplier {
  /*
    Capacity of the hub that holds updates values in a message queue.
    We set it as 1 (combined with a sliding hub) because we are only interested in keeping the last update
   */
  val hubCapacity = 1

  /*
    Creates an updates stream of prefetcher values
    It prepends the stream with current value to make sure
    at the moment of getting the stream we don't miss initial value of the prefetcher
   */
  def setupUpdatesStream[T](hub: Hub[T], currentVal: UIO[T]): ZStream[Any, Nothing, T] =
    ZStream.fromEffect(currentVal) ++ ZStream.fromHub(hub)

  /**
   * Build a static prefetcher (eg, a trivial supplier) from the passed value
   */
  def static[T](v: T): PrefetchingSupplier[T] =
    zio.Runtime.default.unsafeRun(for {
      hub <- Hub.sliding[T](hubCapacity)
    } yield new StaticPrefetchingSupplier(hub, IO.succeed(v), Duration.Infinity))

  /**
   * Build a static prefetcher (eg, a trivial supplier) from the passed UIO
   */
  def staticM[T](v: UIO[T]): PrefetchingSupplier[T] =
    zio.Runtime.default.unsafeRun(for {
      hub <- Hub.sliding[T](hubCapacity)
    } yield new StaticPrefetchingSupplier(hub, v, Duration.Infinity))

  /**
   * Builds a prefetcher that refreshes its stored value using the passed supplier at every updateInterval.
   * The passed 'initialValue' is available immediately, while the supplier will begin execution immediately.
   *
   * The supplier will have the currently held value at its disposal should it be required to determine how to compute
   * the next value to be pre-fetched.
   *
   * Note that the passed supplier may be a Task[T] if it ignores the previously held value to compute the next one.
   *
   * @param initialValue   the value that will be available immediately
   * @param supplier       the effect that computes the new, updated, value. Invoked multiple times.
   * @param updateInterval the periods at which the supplier is invoked
   * @param initialWait    time to wait before launching the regular updated job
   * @tparam T the type that will be held by this prefetching supplier
   */
  def withInitialValue[T: Tag](
    initialValue: T,
    supplier: ZIO[Has[T], Throwable, T],
    updateInterval: Duration,
    initialWait: Duration = 0.seconds,
    prefetcherName: String = "default_name"
  ) =
    for {
      hub                   <- Hub.sliding[T](hubCapacity)
      refWithInitialContent <- Ref.make(initialValue)
      lastOkUpdate          <- Ref.make(Instant.now())
      updateFiber <- scheduleUpdate(
                       refWithInitialContent,
                       lastOkUpdate,
                       supplier,
                       updateInterval,
                       initialWait,
                       hub,
                       prefetcherName
                     ).forkDaemon
    } yield new LivePrefetchingSupplier(refWithInitialContent, lastOkUpdate, hub, updateInterval, updateFiber)

  /**
   * Variant of #withInitialValue() that will compute the first value to be held by the prefetcher by invoking the supplier.
   *
   * 'zero' is passed to the supplier the first time it is run, but is never held by the returned prefetcher,
   *
   * Once the prefetcher is instantiated, it will contain a pre-fetched value.
   */
  def withInitialFetch[T: Tag](
    zero: T,
    supplier: ZIO[ZEnv with Has[T], Throwable, T],
    updateInterval: Duration,
    prefetcherName: String = "default_name"
  ) =
    for {
      hub                   <- Hub.sliding[T](hubCapacity)
      initialValue          <- supplier.provideCustomLayer(ZLayer.succeed(zero))
      refWithInitialContent <- Ref.make[T](initialValue)
      lastOkUpdate          <- Ref.make(Instant.now())
      updateFiber <- scheduleUpdateWithInitialDelay[T](
                       refWithInitialContent,
                       lastOkUpdate,
                       supplier,
                       updateInterval,
                       hub,
                       prefetcherName
                     ).forkDaemon
    } yield new LivePrefetchingSupplier(refWithInitialContent, lastOkUpdate, hub, updateInterval, updateFiber)

  private[prefetcher] def updatePrefetchedValueRef[T: Tag](
    valueRef: Ref[T],
    successTimeRef: Ref[Instant],
    valueSupplier: ZIO[ZEnv with Has[T], Throwable, T],
    hub: Hub[T],
    prefetcherName: String
  ) =
    for {
      _ <- log.info(s"Running supplier to update pre-fetched value for $prefetcherName...")
      // TODO we probably want to keep track of how much time goes by here
      previousVal <- valueRef.get
      newVal <- valueSupplier
                  .provideCustomLayer(ZLayer.succeed(previousVal))
                  .onError(err =>
                    // Error output pretty ugly.
                    log.error(
                      "Evaluation of the supplier failed, prefetched value not updated: " +
                        err.failureOption.map(_.getMessage).getOrElse("")
                    )
                  )
      _ <- valueRef.set(newVal)
      _ <- successTimeRef.set(Instant.now())
      _ <- hub.publish(newVal)
      _ <- log.debug("Successfully updated pre-fetched value.")
    } yield ()

  private def scheduleUpdate[T: Tag](
    valueRef: Ref[T],
    successTimeRef: Ref[Instant],
    supplier: ZIO[ZEnv with Has[T], Throwable, T],
    updateInterval: Duration,
    initialWait: Duration,
    hub: Hub[T],
    prefetcherName: String
  ) =
    // Sleep for the initial wait duration
    ZIO.sleep(initialWait) *>
      // Then attempt a refresh
      updatePrefetchedValueRef(valueRef, successTimeRef, supplier, hub, prefetcherName)
        // Retry at each interval until we succeed
        .retry(Schedule.spaced(updateInterval))
        // When we succeed, repeat at every interval
        .repeat(Schedule.spaced(updateInterval))

  private def scheduleUpdateWithInitialDelay[T: Tag](
    valueRef: Ref[T],
    successTimeRef: Ref[Instant],
    supplier: ZIO[ZEnv with Has[T], Throwable, T],
    updateInterval: Duration,
    hub: Hub[T],
    prefetcherName: String
  ) =
    ZIO.sleep(updateInterval) *>
      scheduleUpdate(
        valueRef,
        successTimeRef,
        supplier,
        updateInterval,
        Duration.Zero,
        hub,
        prefetcherName
      )

  /**
   * Similar in all points to #withInitialValue(), but exposes some refresh statistics
   * via dropwizard metrics, using the registry passed in the environment.
   *
   * @param prefetcherName the name used to identify this prefetcher in the exposed metrics
   */
  def monitoredWithInitialValue[T: Tag](
    initialValue: T,
    supplier: ZIO[Has[T], Throwable, T],
    updateInterval: Duration,
    prefetcherName: String,
    initialWait: Duration = 0.seconds
  ) =
    for {
      // Registry metrics and get the registry from the environment
      (t, fm, registry) <- registerMetrics(prefetcherName)
      // Rely on the traditional "non-registry aware" #withInitialValue()
      pfs <- withInitialValue(
               initialValue,
               // Wrap the passed supplier in another one that updates the metrics,
               // pass the registry so we can rely in #withInitialValue without making it
               // Registry aware
               timedSupplier(supplier, t, fm).provideSomeLayer[Has[T]](registry),
               updateInterval,
               initialWait
             )
      _ <- registerGauge(pfs, prefetcherName)
    } yield pfs

  def monitoredWithInitialFetch[T: Tag](
    zero: T,
    supplier: ZIO[ZEnv with Has[T], Throwable, T],
    updateInterval: Duration,
    prefetcherName: String
  ) = for {
    // Registry metrics and get the registry from the environment
    (t, fm, registry) <- registerMetrics(prefetcherName)
    pfs <- withInitialFetch(
             zero,
             // Wrap the passed supplier in another one that updates the metrics,
             // pass the registry so we can rely in #withInitialValue
             // without making it Registry aware.
             timedSupplier(supplier, t, fm).provideSomeLayer[Has[T] with ZEnv](registry),
             updateInterval
           )
    _ <- registerGauge(pfs, prefetcherName)
  } yield pfs

  private def registerMetrics(prefetcherName: String) =
    for {
      // Register a timer
      t <- timer.register(prefetcherName, Array("refresh_timer"))
      // ... and a failures meter
      fm <- meter.register(prefetcherName, Array("failures"))
      // Read the registry from the environment for later direct usage.
      registryFromEnv <- ZIO.access[Registry](_.get).map(ZLayer.succeed(_))
    } yield (t, fm, registryFromEnv)

  /**
   * @return a supplier that additionally tracks how long the passed supplier takes via the specified timer.
   */
  private def timedSupplier[R, T](supplier: ZIO[R, Throwable, T], t: Timer, failures: Meter) =
    for {
      ctx <- t.start()
      suppliedVal <- supplier
                       // If the supplier fails -> mark a failure
                       .onError(_ => failures.mark().ignore)
                       // In any case, stop the timer
                       .ensuring(t.stop(ctx).ignore)
    } yield suppliedVal

  private def registerGauge[T](pfs: PrefetchingSupplier[T], prefetcherName: String) = {
    // Absolutely ugly way of having the gauge be able to read directly from _something_ that is not a ZIO effect:
    // This volatile var will be written to from a separate fiber on a regular basis.
    @volatile
    var elapsedTime: Long = 0
    for {
      _ <- gauge.register(prefetcherName, Array("last_success_ms"), () => elapsedTime)
      // Update the elapsed time since last success every second, so it may be read by the gauge.
      _ <- millisSinceLastSuccessfulUpdate(pfs)
             .map(ms => elapsedTime = ms)
             .repeat(Schedule.spaced(1.second))
             .forkDaemon
    } yield ()
  }

  private def millisSinceLastSuccessfulUpdate[T](pfs: PrefetchingSupplier[T]) =
    pfs.lastSuccessfulUpdate.map(t => System.currentTimeMillis() - t.toEpochMilli)

}
