package some.test.pack

import ch.j3t.prefetcher.PrefetchingSupplier

/**
 * A silly object to check the type can be used ;)
 */
case class TestTypeUsage(
  prefetcher: PrefetchingSupplier[Any]
)
