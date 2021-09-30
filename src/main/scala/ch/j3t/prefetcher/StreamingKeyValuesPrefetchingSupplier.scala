package ch.j3t.prefetcher

import zio.clock._
import zio.{ Chunk, Fiber, Hub, IO, Ref, ZDequeue, ZIO, ZManaged }
import zio.duration.{ durationInt, Duration }
import zio.stream.{ UStream, ZStream }
import java.time.Instant

object StreamingKeyValuesPrefetchingSupplier {

  sealed trait Update[K, +V]
  case class Drop[K, V](k: K) extends Update[K, V]
  case class Put[K, V](k: K, v: V) extends Update[K, V] {
    def toTuple = (k, v)
  }

  /**
   * A PrefetchingSupplier trait that is fed from a stream of updates, not a batch job.
   *
   * @param prefetchedValueRef the Ref pointing to the currently held pre-fetched value
   * @param lastOkUpdate holds the last time the value was updated successfully
   * @param updateFiber the fiber running the regular update job
   * @tparam T the type of the pre-fetched value
   */
  class StreamingPrefetchingSupplier[T] private[prefetcher] (
    prefetchedValueRef: Ref[T],
    lastOkUpdate: Ref[Instant],
    val updateFiber: Fiber[Throwable, Any],
    val subscribeToUpdates: ZManaged[Any, Nothing, ZDequeue[Any, Throwable, T]]
  ) extends PrefetchingSupplier[T] {

    val currentValueRef = prefetchedValueRef.readOnly

    val get = prefetchedValueRef.get

    // Consider using the groupedWithinDuration here?
    def updateInterval: Duration = 0.seconds

    /**
     * @return the elapsed duration since the last successful update
     */
    def lastSuccessfulUpdate: IO[Nothing, Instant] = lastOkUpdate.get

  }

  /**
   * Builds a prefetcher wrapping a map, of which the values will be updated
   * by a stream of updates.
   *
   * Outside of updates provided by the 'supplyingStream', no mutation of the pre-fetched value occurs:
   * unless they are deleted explicitly, entries remain in the map forever
   *
   * @param initialValue the value that will be available immediately
   * @param supplyingStream the stream of updates that will be used to update the map.
   *                        This stream is expected to be infinite and blocking (a separate process feeds updates into it).
   *                        It will continuously and asynchronously be drained. No recovery will be attempted if it fails.
   * @param groupedWithinSize the maximum group size of updates to group together
   * @param groupedWithinDuration how long to wait for grouping streamed values together
   * @tparam K Type of the keys in the underlying map
   * @tparam V Type of the values in the underlying map
   */
  def withInitialValue[K, V](
    initialValue: Map[K, V],
    supplyingStream: UStream[Update[K, V]],
    groupedWithinSize: Int = 128,
    groupedWithinDuration: Duration = 1.second
  ): ZIO[Clock, Nothing, StreamingPrefetchingSupplier[Map[K, V]]] =
    for {
      hub          <- Hub.sliding[Map[K, V]](1)
      contentRef   <- Ref.make(initialValue)
      lastOkUpdate <- instant.flatMap(i => Ref.make(i))
      // Caller is responsible for handling any errors:
      // if the stream fails or stops the prefetcher will stop
      updateFiber <- supplyingStream
                       .groupedWithin(groupedWithinSize, groupedWithinDuration)
                       .mapM(u => updateMap(contentRef, u, lastOkUpdate).runDrain)
                       .runDrain
                       .fork
    } yield new StreamingPrefetchingSupplier(
      contentRef,
      lastOkUpdate,
      updateFiber,
      hub.subscribe
    )

  private def updateMap[K, V](
    mapRef: Ref[Map[K, V]],
    updates: Chunk[Update[K, V]],
    lastOkUpdate: Ref[Instant]
  ): ZStream[Clock, Nothing, Unit] =
    // Deletions are expected to be rare, but we still need to respect the order of the updates:
    // we ensure that a deletion happens _after_ anything addition that came before it in the chunk
    updates.size match {
      // Nothing to do anymore
      case 0 => UStream.empty
      // 1 or more updates. Jump through the streaming hoops so we don't blow the stack
      // (chaining ZIO's with recursive calls to updateMap would explode quickly)
      // TODO rely on the Clock from the environment as it's available anyway
      case _ =>
        updates.head match {
          case _: Drop[K, V] =>
            val (drops, remaining) = updates.splitWhere(!_.isInstanceOf[Drop[K, V]])
            UStream.fromEffect(
              dropAllFromMap(mapRef, drops.asInstanceOf[Seq[Drop[K, V]]]) *> instant.map(i => lastOkUpdate.set(i)).unit
            ) ++ updateMap(mapRef, remaining, lastOkUpdate)
          case _: Put[K, V] =>
            val (puts, remaining) = updates.splitWhere(!_.isInstanceOf[Put[K, V]])
            UStream.fromEffect(
              putAllIntoMap(mapRef, puts.asInstanceOf[Seq[Put[K, V]]]) *> instant.map(i => lastOkUpdate.set(i)).unit
            ) ++ updateMap(mapRef, remaining, lastOkUpdate)
        }
    }

  private def dropAllFromMap[K, V](mapRef: Ref[Map[K, V]], deletions: Seq[Drop[K, V]]): ZIO[Any, Nothing, Unit] =
    mapRef.update(_.--(deletions.map(_.k)))

  private def putAllIntoMap[K, V](mapRef: Ref[Map[K, V]], additions: Seq[Put[K, V]]): ZIO[Any, Nothing, Unit] =
    mapRef.update(_.++(additions.map(_.toTuple)))

}
