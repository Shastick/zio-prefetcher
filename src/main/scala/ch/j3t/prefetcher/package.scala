package ch.j3t

import zio.{ IO, Task, ZIO }

package object prefetcher {

  /**
   * Implicit class for looking up elements in a pre-fetched map.
   *
   * Allows returning a failed ZIO if the element cannot be found.
   *
   * @param m the map on which to do the lookup
   * @tparam K key type
   * @tparam V value type
   */
  implicit class MapLookupHelper[K, V](m: Map[K, V]) {

    def getOrFail(k: K): IO[NotFound, V] =
      // At some point we should have more finely typed errors (ie, to map a "not found" to a 4XX http response)
      ZIO.fromOption(m.get(k)).mapError(_ => NotFound("Not found: " + k))

    def getAll(keys: Set[K]): Task[Map[K, V]] =
      ZIO.succeed(
        keys.toSeq
          .flatMap(k => m.get(k).map(v => (k, v)))
          .toMap
      )

    def getAllOrFail(keys: Set[K]): IO[NotFound, Map[K, V]] =
      ZIO
        .foreach(keys)(k =>
          getOrFail(k)
            .map(v => (k, v))
        )
        .map(_.toMap)
        .mapError(t => NotFound("Failed to lookup set of keys: " + keys, Some(t)))

  }

  /**
   * Implicit class for easing lookups in prefetchers that contain nested maps.
   *
   * @param a the instance to do the lookup on
   * @tparam K outer key type
   * @tparam N nested key type
   * @tparam V value type
   */
  implicit class PrefetcherLookupHelper[K, N, V](a: PrefetchingSupplier[Map[K, Map[N, V]]]) {

    /**
     * Lookup a map within a pre-fetched map.
     * @param k the (outer) key for the (inner) map to be returned
     * @return a map contained in the pre-fetched map.
     */
    def getMap(k: K): IO[NotFound, Map[N, V]] =
      a.get.flatMap(prefetchedVal => prefetchedVal.getOrFail(k))

    /**
     * Lookup a key in a (nested) map within a pre-fetched map
     * @param nestedKey the key within the nested map for which to fetch the mapped value
     * @param outerKey the key to access the nested map
     * @return the nested value after an outerKey -> nestedKey lookup.
     */
    def nestedLookup(nestedKey: N)(implicit outerKey: K): IO[NotFound, V] =
      getMap(outerKey).flatMap(map => map.getOrFail(nestedKey))

    /**
     * Lookup a set of keyy in a (nested) map within a pre-fetched map
     * @param nestedKeys the keys within the nested map for which to fetch the mapped values
     * @param outerKey the key to access the nested map
     * @return the nested values after an outerKey -> nestedKeys lookups.
     * @note will return a failure if any of the passed keys does not exist in the nested map
     */
    def nestedLookups(nestedKeys: Set[N])(implicit outerKey: K): IO[NotFound, Map[N, V]] =
      getMap(outerKey).flatMap(map => map.getAllOrFail(nestedKeys))

  }

}
