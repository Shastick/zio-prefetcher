package ch.j3t.prefetcher

import zio.duration.{ Duration, _ }
import zio.logging._
import zio._

/**
 * This class is akin to a Supplier[T] that will always have a T immediately available,
 * while it is updated by a background job on a regular basis.
 *
 * @param prefetchedValueRef the Ref pointing to the currently held pre-fetched value
 * @param updateFiber the fiber running the regular update job
 * @tparam T the type of the pre-fetched value
 */
private[prefetcher] class PrefetchingSupplier[T](
  prefetchedValueRef: Ref[T],
  val updateFiber: Fiber[Throwable, Any]
) {

  val currentValueRef = prefetchedValueRef.readOnly

}

object PrefetchingSupplier {

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
  def withInitialValue[T](
    initialValue: T,
    supplier: ZIO[T, Throwable, T],
    updateInterval: Duration,
    initialWait: Duration = 0.seconds
  ) =
    for {
      refWithInitialContent <- Ref.make(initialValue)
      updateFiber           <- scheduleUpdate(refWithInitialContent, supplier, updateInterval, initialWait).fork
    } yield new PrefetchingSupplier(refWithInitialContent, updateFiber)

  /**
   * Variant of #withInitialValue() that will compute the first value to be held by the prefetcher by invoking the supplier.
   *
   * 'zero' is passed to the supplier the first time it is run, but is never held by the returned prefetcher,
   *
   * Once the prefetcher is instantiated, it will contain a pre-fetched value.
   */
  def withInitialFetch[T](zero: T, supplier: ZIO[T, Throwable, T], updateInterval: Duration) =
    for {
      initialValue          <- supplier.provide(zero)
      refWithInitialContent <- Ref.make(initialValue)
      updateFiber           <- scheduleUpdateWithInitialDelay(refWithInitialContent, supplier, updateInterval).fork
    } yield new PrefetchingSupplier(refWithInitialContent, updateFiber)

  private def updatePrefetchedValueRef[T](
    valueRef: Ref[T],
    valueSupplier: ZIO[T, Throwable, T]
  ) =
    for {
      _ <- log.info("Running supplier to updated pre-fetched value...")
      // TODO we probably want to keep track of how much time goes by here
      previousVal <- valueRef.get
      newVal <- valueSupplier
                  .provide(previousVal)
                  .onError(err =>
                    // Error output pretty ugly.
                    log.error(
                      "Evaluation of the supplier failed, prefetched value not updated: " +
                        err.failureOption.map(_.getMessage).getOrElse("")
                    )
                  )
      _ <- valueRef.set(newVal)
      _ <- log.debug("Successfully update pre-fetched value.")
    } yield ()

  private def scheduleUpdate[T](
    valueRef: Ref[T],
    supplier: ZIO[T, Throwable, T],
    updateInterval: Duration,
    initialWait: Duration
  ) =
    ZIO.sleep(initialWait) *> updatePrefetchedValueRef(valueRef, supplier)
      .retry(Schedule.spaced(updateInterval))
      .repeat(Schedule.spaced(updateInterval))

  private def scheduleUpdateWithInitialDelay[T](
    valueRef: Ref[T],
    supplier: ZIO[T, Throwable, T],
    updateInterval: Duration
  ) =
    ZIO.sleep(updateInterval) *> scheduleUpdate(valueRef, supplier, updateInterval, Duration.Zero)

}
