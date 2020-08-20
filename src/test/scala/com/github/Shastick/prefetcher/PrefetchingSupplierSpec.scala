package com.github.Shastick.prefetcher

import zio.ZIO
import zio.logging.Logging
import zio.test.Assertion.equalTo
import zio.test.environment.TestClock
import zio.test.{ assert, suite, testM, DefaultRunnableSpec }
import zio.duration._

object PrefetchingSupplierSpec extends DefaultRunnableSpec {

  val logEnv = Logging.console(
    format = (_, logEntry) => logEntry,
    rootLoggerName = Some("test-logger")
  )

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
        v1 <- incrementer.provide(-1)
        v2 <- incrementer.provide(v1)
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    ),
    testM("Correctly update the pre-fetched ref")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logEnv)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    testM("Correctly deal with supplier errors")(
      for {
        prefetcher <-
          PrefetchingSupplier.withInitialValue(-42, new FailingIncr().failEvery2, 1.second).provideCustomLayer(logEnv)
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
    testM("Correctly work with a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialValue(-42, incr, 1.second, 100.millis).provideCustomLayer(logEnv)
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
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incrementer, 1.second).provideCustomLayer(logEnv)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(-41)) &&
        assert(initialSupplierCall)(equalTo(-40)) &&
        assert(secondSupplierCall)(equalTo(-39))
    ),
    testM("Correctly do an initial fetch from a supplier that ignores the previous value") {
      val incr = new Incr().supplier
      for {
        prefetcher          <- PrefetchingSupplier.withInitialFetch(-42, incr, 1.second).provideCustomLayer(logEnv)
        immediatelyHeld     <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    }
  )

  private val incrementer = ZIO.fromFunction[Int, Int](i => i + 1)

  class Incr() {
    private var counter: Int = -1

    val supplier = ZIO.effect {
      counter += 1
      counter
    }
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
