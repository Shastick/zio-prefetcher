package ch.j3t.prefetcher

import com.codahale.metrics.MetricRegistry
import zio.{ Chunk, ZIO, ZLayer }
import zio.test.Assertion.{ endsWithString, equalTo }
import zio.test.{ assert, liveEnvironment, TestClock, ZIOSpecDefault }
import zio.metrics.dropwizard._
import zio._
import zio.ZIO.attemptBlocking
import zio.logging.LogFormat

object PrefetchingSupplierSpec extends ZIOSpecDefault {

  val logger = LogFormat.default.toLogger

  def withNewRegistryLayer =
    ZIO.succeed(new MetricRegistry()).map { mr =>
      (mr, (ZLayer.succeed[Option[MetricRegistry]](Some(mr)) >>> Registry.explicit))
    }

  def spec = suite("PrefetchingSupplierSpec")(
    test("The dirty counter-incrementing effect works as expected") {
      val incr = new Incr().supplier
      for {
        v1 <- incr
        v2 <- incr
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    },
    test("The clean counter-incrementing effect works as expected")(
      for {
        v1 <- incrementer.provide(ZLayer.succeed(-1))
        v2 <- incrementer.provide(ZLayer.succeed(v1))
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    ),
    test("Correctly update the pre-fetched ref")(
      for {
        prefetcher          <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    test("Correctly update the pre-fetched ref even if the building fiber was forked and joined")(
      for {
        prefetcherF         <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).fork
        prefetcher          <- prefetcherF.join
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    test("Correctly stream the updatesStream")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        stream      = prefetcher.updatesStream
        fiber      <- stream.take(4).runCollect.fork
        _          <- TestClock.adjust(3.second)
        collected  <- fiber.join
      } yield assert(collected)(equalTo(Chunk(0, 1, 2, 3)))
    ),
    test("Correctly allow to subscribe more than one stream")(
      for {
        prefetcher      <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        firstStream      = prefetcher.updatesStream
        secondStream     = prefetcher.updatesStream
        firstFiber      <- firstStream.take(3).runCollect.fork
        secondFiber     <- secondStream.take(3).runCollect.fork
        _               <- TestClock.adjust(2.second)
        firstCollected  <- firstFiber.join
        secondCollected <- secondFiber.join
      } yield assert(firstCollected)(equalTo(Chunk(0, 1, 2))) &&
        assert(firstCollected)(equalTo(secondCollected))
    ),
    test("Correctly get the later value if we subscribe later")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        _          <- TestClock.adjust(110.millis)
        stream      = prefetcher.updatesStream
        fiber      <- stream.take(1).runCollect.fork
        _          <- TestClock.adjust(1.second)
        collected  <- fiber.join
      } yield assert(collected)(equalTo(Chunk(1)))
    ),
    test("Correctly stream values for two streams subscribed at different time")(
      for {
        prefetcher      <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        _               <- TestClock.adjust(1.millis)
        firstStream      = prefetcher.updatesStream
        secondStream     = prefetcher.updatesStream
        _               <- TestClock.adjust(100.millis)
        firstFiber      <- firstStream.take(3).runCollect.fork
        _               <- TestClock.adjust(1.second)
        secondFiber     <- secondStream.take(3).runCollect.fork
        _               <- TestClock.adjust(2.seconds)
        firstCollected  <- firstFiber.join
        secondCollected <- secondFiber.join
      } yield assert(firstCollected)(equalTo(Chunk(1, 2, 3))) &&
        assert(secondCollected)(equalTo(Chunk(2, 3, 4)))
    ),
    test("Correctly update the pre-fetched ref and update metrics")(
      for {
        tup          <- withNewRegistryLayer
        (mr, mrLayer) = tup
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialValue(
                          0,
                          incrementer,
                          1.second,
                          "test_prefetcher",
                          100.millis
                        )
                        .provideSomeLayer[ZEnv](mrLayer)
        // Implicitly checks that we indeed have instantiated metrics...
        gaugeName               = mr.getGauges.entrySet().iterator().next().getKey
        timer                   = mr.getTimers.entrySet().iterator().next().getValue
        failures                = mr.getMeters.entrySet().iterator().next().getValue
        beforeFirstRefresh      = timer.getCount
        immediatelyHeld        <- prefetcher.get
        _                      <- TestClock.adjust(100.millis)
        afterFirstRefreshCount  = timer.getCount
        initialSupplierCall    <- prefetcher.get
        _                      <- TestClock.adjust(1.second)
        afterSecondRefreshCount = timer.getCount
        secondSupplierCall     <- prefetcher.get
        totalFailureCount       = failures.getCount
      } yield assert(gaugeName)(endsWithString("last_success_ms")) &&
        assert(beforeFirstRefresh)(equalTo(0L)) &&
        assert(immediatelyHeld)(equalTo(0)) &&
        assert(afterFirstRefreshCount)(equalTo(1L)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(afterSecondRefreshCount)(equalTo(2L)) &&
        assert(secondSupplierCall)(equalTo(2)) &&
        assert(totalFailureCount)(equalTo(0L))
    ),
    test("Correctly deal with supplier errors")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(-42, new FailingIncr().failEvery2, 1.second)
        // First call to the effect is done within these 100 ms
        _ <- TestClock.adjust(100.millis)
        // The call has failed, thus we should still have the initial value here
        initialSupplierCall <- prefetcher.get
        // Wait for a second to pass...
        _ <- TestClock.adjust(1.second)
        // Now we should have the state of the counter, which is 1
        secondSupplierCall <- prefetcher.get
      } yield assert(initialSupplierCall)(equalTo(-42)) &&
        assert(secondSupplierCall)(equalTo(1))
    ),
    test("Correctly deal with supplier errors and update metrics accordingly")(
      for {
        tup          <- withNewRegistryLayer
        (mr, mrLayer) = tup
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialValue(-42, new FailingIncr().failEvery2, 1.second, "test_prefetcher")
                        .provideSomeLayer[ZEnv](mrLayer)
        failures = mr.getMeters.entrySet().iterator().next().getValue
        // First call to the effect is done within these 100 ms
        _ <- TestClock.adjust(100.millis)
        // The call has failed, thus we should still have the initial value here
        initialSupplierCall <- prefetcher.get
        failureCount         = failures.getCount
        // Wait for a second to pass...
        _ <- TestClock.adjust(1.second)
        // Now we should have the state of the counter, which is 1
        secondSupplierCall <- prefetcher.get
      } yield assert(initialSupplierCall)(equalTo(-42)) &&
        assert(secondSupplierCall)(equalTo(1)) &&
        assert(failureCount)(equalTo(1L))
    ),
    test("Correctly work with a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialValue(-42, incr, 1.second, 100.millis)
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(-42)) &&
        assert(initialSupplierCall)(equalTo(0)) &&
        assert(secondSupplierCall)(equalTo(1))
    },
    test("Correctly do an initial fetch from a supplier")(
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incrementer, 1.second)
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    ),
    test("Correctly do an initial fetch from a supplier and expose metrics properly")(
      for {
        tup          <- withNewRegistryLayer
        (mr, mrLayer) = tup
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialFetch(-42, incrementer, 1.second, "test_prefetcher")
                        .provideSomeLayer[ZEnv](mrLayer)
        // Implicitly checks that we indeed have instantiated metrics...
        _                       = mr.getGauges.entrySet().iterator().next().getKey
        timer                   = mr.getTimers.entrySet().iterator().next().getValue
        _                       = mr.getMeters.entrySet().iterator().next().getValue
        afterInitialFetchCount  = timer.getCount
        immediatelyHeld        <- prefetcher.get
        _                      <- TestClock.adjust(1.second)
        afterFirstRefreshCount  = timer.getCount
        initialSupplierCall    <- prefetcher.get
        _                      <- TestClock.adjust(1.second)
        afterSecondRefreshCount = timer.getCount
        secondSupplierCall     <- prefetcher.get
      } yield assert(afterInitialFetchCount)(equalTo(1L)) &&
        assert(afterFirstRefreshCount)(equalTo(2L)) &&
        assert(afterSecondRefreshCount)(equalTo(3L)) &&
        assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    ),
    test("Correctly do an initial fetch from a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incr, 1.second)
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    },
    test("Correctly work from a supplier that relies on the ZEnv") {
      val incr = new BlockingIncr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incr, 1.second)
        immediatelyHeld     <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
      } yield assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    }
  ).provide(liveEnvironment)

  private val incrementer = ZIO.environmentWith[Int](i => i.get + 1)

  class Incr() {
    private var counter: Int = -1

    val supplier = ZIO.attempt {
      counter += 1
      counter
    }
  }

  class BlockingIncr() {

    val supplier: ZIO[Any with Int, Throwable, Int] =
      for {
        prev <- ZIO.environment[Int]
        next <- attemptBlocking(prev.get + 1)
      } yield next
  }

  class FailingIncr() {
    private var counter: Int = -1

    val failEvery2 = ZIO.attempt {
      counter += 1
      if (counter % 2 == 0) {
        ZIO.fail(new Exception("Darn"))
      } else {
        ZIO.succeed(counter)
      }
    }.flatten
  }

}
