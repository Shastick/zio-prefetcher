package ch.j3t.prefetcher

import com.codahale.metrics.MetricRegistry
import zio.{ Chunk, Has, ZIO, ZLayer }
import zio.logging.Logging
import zio.test.Assertion.{ endsWithString, equalTo }
import zio.test.environment.TestClock
import zio.test.{ assert, DefaultRunnableSpec }
import zio.duration._
import zio.blocking._
import zio.metrics.dropwizard._

object PrefetchingSupplierSpec extends DefaultRunnableSpec {

  val logLayer = Logging.console(
    format = (_, logEntry) => logEntry
  ) >>> Logging.withRootLoggerName("test-logger")

  def withNewRegistryLayer =
    ZIO.effectTotal(new MetricRegistry()).map { mr =>
      (mr, (ZLayer.succeed[Option[MetricRegistry]](Some(mr)) >>> Registry.explicit) ++ logLayer)
    }

  def spec = suite("PrefetchingSupplierSpec")(
    testM("The dirty counter-incrementing effect works as expected") {
      val incr = new Incr().supplier
      for {
        v1 <- incr
        v2 <- incr
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    },
    testM("The clean counter-incrementing effect works as expected")(
      for {
        v1 <- incrementer.provide(Has(-1))
        v2 <- incrementer.provide(Has(v1))
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    ),
    testM("Correctly update the pre-fetched ref")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    testM("Correctly update the pre-fetched ref even if the building fiber was forked and joined")(
      for {
        prefetcherF <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer).fork
        prefetcher          <- prefetcherF.join
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    testM("Correctly stream the updatesStream")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer)
        stream     = prefetcher.updatesStream
        fiber     <- stream.take(4).runCollect.fork
        _         <- TestClock.adjust(3.second)
        collected <- fiber.join
      } yield assert(collected)(equalTo(Chunk(0, 1, 2, 3)))
    ),
    testM("Correctly allow to subscribe more than one stream")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer)
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
    testM("Correctly get the later value if we subscribe later")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer)
        _         <- TestClock.adjust(110.millis)
        stream     = prefetcher.updatesStream
        fiber     <- stream.take(1).runCollect.fork
        _         <- TestClock.adjust(1.second)
        collected <- fiber.join
      } yield assert(collected)(equalTo(Chunk(1)))
    ),
    testM("Correctly stream values for two streams subscribed at different time")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logLayer)
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
    testM("Correctly update the pre-fetched ref and update metrics")(
      for {
        (mr, mrLayer) <- withNewRegistryLayer
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialValue(
                          0,
                          incrementer,
                          1.second,
                          "test_prefetcher",
                          100.millis
                        )
                        .provideCustomLayer(mrLayer)
        // Implicitly checks that we indeed have instantiated metrics...
        gaugeName               = mr.getGauges.entrySet().iterator().next().getKey
        timer                   = mr.getTimers.entrySet().iterator().next().getValue
        failures                = mr.getMeters.entrySet().iterator().next().getValue
        beforeFirstRefresh      = timer.getCount
        immediatelyHeld        <- prefetcher.currentValueRef.get
        _                      <- TestClock.adjust(100.millis)
        afterFirstRefreshCount  = timer.getCount
        initialSupplierCall    <- prefetcher.currentValueRef.get
        _                      <- TestClock.adjust(1.second)
        afterSecondRefreshCount = timer.getCount
        secondSupplierCall     <- prefetcher.currentValueRef.get
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
    testM("Correctly deal with supplier errors")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(-42, new FailingIncr().failEvery2, 1.second).provideCustomLayer(logLayer)
        // First call to the effect is done within these 100 ms
        _ <- TestClock.adjust(100.millis)
        // The call has failed, thus we should still have the initial value here
        initialSupplierCall <- prefetcher.currentValueRef.get
        // Wait for a second to pass...
        _ <- TestClock.adjust(1.second)
        // Now we should have the state of the counter, which is 1
        secondSupplierCall <- prefetcher.currentValueRef.get
      } yield assert(initialSupplierCall)(equalTo(-42)) &&
        assert(secondSupplierCall)(equalTo(1))
    ),
    testM("Correctly deal with supplier errors and update metrics accordingly")(
      for {
        (mr, mrLayer) <- withNewRegistryLayer
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialValue(-42, new FailingIncr().failEvery2, 1.second, "test_prefetcher")
                        .provideCustomLayer(mrLayer)
        failures = mr.getMeters.entrySet().iterator().next().getValue
        // First call to the effect is done within these 100 ms
        _ <- TestClock.adjust(100.millis)
        // The call has failed, thus we should still have the initial value here
        initialSupplierCall <- prefetcher.currentValueRef.get
        failureCount         = failures.getCount
        // Wait for a second to pass...
        _ <- TestClock.adjust(1.second)
        // Now we should have the state of the counter, which is 1
        secondSupplierCall <- prefetcher.currentValueRef.get
      } yield assert(initialSupplierCall)(equalTo(-42)) &&
        assert(secondSupplierCall)(equalTo(1)) &&
        assert(failureCount)(equalTo(1L))
    ),
    testM("Correctly work with a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialValue(-42, incr, 1.second, 100.millis).provideCustomLayer(logLayer)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(-42)) &&
        assert(initialSupplierCall)(equalTo(0)) &&
        assert(secondSupplierCall)(equalTo(1))
    },
    testM("Correctly do an initial fetch from a supplier")(
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incrementer, 1.second).provideCustomLayer(logLayer)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    ),
    testM("Correctly do an initial fetch from a supplier and expose metrics properly")(
      for {
        (mr, mrLayer) <- withNewRegistryLayer
        prefetcher <- PrefetchingSupplier
                        .monitoredWithInitialFetch(-42, incrementer, 1.second, "test_prefetcher")
                        .provideCustomLayer(mrLayer)
        // Implicitly checks that we indeed have instantiated metrics...
        _                       = mr.getGauges.entrySet().iterator().next().getKey
        timer                   = mr.getTimers.entrySet().iterator().next().getValue
        _                       = mr.getMeters.entrySet().iterator().next().getValue
        afterInitialFetchCount  = timer.getCount
        immediatelyHeld        <- prefetcher.currentValueRef.get
        _                      <- TestClock.adjust(1.second)
        afterFirstRefreshCount  = timer.getCount
        initialSupplierCall    <- prefetcher.currentValueRef.get
        _                      <- TestClock.adjust(1.second)
        afterSecondRefreshCount = timer.getCount
        secondSupplierCall     <- prefetcher.currentValueRef.get
      } yield assert(afterInitialFetchCount)(equalTo(1L)) &&
        assert(afterFirstRefreshCount)(equalTo(2L)) &&
        assert(afterSecondRefreshCount)(equalTo(3L)) &&
        assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    ),
    testM("Correctly do an initial fetch from a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incr, 1.second).provideCustomLayer(logLayer)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    },
    testM("Correctly work from a supplier that relies on the ZEnv") {
      val incr = new BlockingIncr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incr, 1.second).provideCustomLayer(logLayer)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    }
  )

  private val incrementer = ZIO.fromFunction[Has[Int], Int](i => i.get + 1)

  class Incr() {
    private var counter: Int = -1

    val supplier = ZIO.effect {
      counter += 1
      counter
    }
  }

  class BlockingIncr() {

    val supplier: ZIO[Blocking with Has[Int], Throwable, Int] =
      for {
        prev <- ZIO.access[Has[Int]](_.get)
        next <- effectBlocking(prev + 1)
      } yield next
  }

  class FailingIncr() {
    private var counter: Int = -1

    val failEvery2 = ZIO.effect {
      counter += 1
      if (counter % 2 == 0) {
        ZIO.fail(new Exception("Darn"))
      } else {
        ZIO.succeed(counter)
      }
    }.flatten
  }

}
