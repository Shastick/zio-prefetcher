package ch.j3t.prefetcher

import zio.Clock.instant
import zio.stream.{ UStream, ZStream }
import zio.{ durationInt, Clock, Duration, Fiber, Hub, IO, Ref, Tag, ZEnv, ZIO }

import java.time.Instant

object StreamingPrefetchingSupplier {

  /**
   * A PrefetchingSupplier trait that is fed from a stream of updates, not a batch job.
   *
   * @param prefetchedValueRef the Ref pointing to the currently held pre-fetched value
   * @param lastOkUpdate holds the last time the value was updated successfully
   * @param streamConsumptionFiber the fiber that consumes from the stream
   * @tparam T the type of the pre-fetched value
   */
  class StreamingPrefetchingSupplier[T] private[prefetcher] (
    prefetchedValueRef: Ref[T],
    lastOkUpdate: Ref[Instant],
    hub: Hub[T],
    val streamConsumptionFiber: Fiber[Throwable, Any]
  ) extends PrefetchingSupplier[T] {

    val get = prefetchedValueRef.get

    // Consider using the groupedWithinDuration here?
    def updateInterval: Duration = 0.seconds

    /**
     * @return the elapsed duration since the last successful update
     */
    def lastSuccessfulUpdate: IO[Nothing, Instant] = lastOkUpdate.get

    def updatesStream: ZStream[Any, Nothing, T] =
      PrefetchingSupplier.setupUpdatesStream[T](hub, get)

  }

  /**
   * Builds a pre-fetcher that is fed from a stream of updates.
   *
   * Outside of updates provided by the 'supplyingStream', no mutation of the pre-fetched value occurs.
   *
   * @param initialValue the value that will be available immediately.
   *                     Note that it will be overridden immediately if the passed stream already contains a value.
   *
   * @param supplyingStream the stream of updates that will replace the value held in this prefetcher.
   *                        This stream is expected to be infinite and blocking (a separate process feeds updates into it).
   *                        It will continuously and asynchronously be drained. No recovery will be attempted if it fails.
   * @tparam I Type of the incoming stream
   */
  def fromStream[T: Tag](
    initialValue: T,
    supplyingStream: UStream[T],
    prefetcherName: String = "default_name"
  ): ZIO[Clock with ZEnv, Nothing, StreamingPrefetchingSupplier[T]] =
    for {
      hub          <- Hub.sliding[T](PrefetchingSupplier.hubCapacity)
      contentRef   <- Ref.make(initialValue)
      lastOkUpdate <- instant.flatMap(i => Ref.make(i))
      // Caller is responsible for handling any errors:
      // if the stream fails or stops the prefetcher will stop
      updateFiber <-
        supplyingStream
          .mapZIO(u =>
            PrefetchingSupplier.updatePrefetchedValueRef(contentRef, lastOkUpdate, ZIO.succeed(u), hub, prefetcherName)
          )
          .runDrain
          .forkDaemon
    } yield new StreamingPrefetchingSupplier(
      contentRef,
      lastOkUpdate,
      hub,
      updateFiber
    )

}
