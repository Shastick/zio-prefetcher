package ch.j3t.prefetcher

/**
 * Conveys the fact that a lookup failed.
 *
 * @param message details about the failed lookup
 * @param cause when failing to look up a set of keys, contains the error for the first key that was not found.
 */
final case class NotFound(message: String, cause: Option[NotFound] = None) extends Exception(message)
