package com.github.Shastick

import zio._
import zio.test._
import zio.test.Assertion._
import zio.duration._
import zio.test.environment.TestClock
import zio.logging._


object PrefetchingSupplierSpec extends DefaultRunnableSpec {

  val logEnv = Logging.console(
    format = (_, logEntry) => logEntry,
    rootLoggerName = Some("test-logger")
  )

  def spec = suite("PrefetchingSupplierSpec")(
    testM("The dirty counter-incrementing effect works as expected")(
      for {
        v1 <- supplierFromCounter
        v2 <- supplierFromCounter
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    ),
    testM("The clean counter-incrementing effect works as expected")(
      for {
        v1 <- incrementer.provide(-1)
        v2 <- incrementer.provide(v1)
      } yield assert(v1)(equalTo(0)) && assert(v2)(equalTo(1))
    ),
    testM("Correctly update the pre-fetched ref")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).provideCustomLayer(logEnv)
        immediatelyHeld <- prefetcher.currentValueRef.get
        _ <- TestClock.adjust(100.millis)
        initialSupplierCall <- prefetcher.currentValueRef.get
        _ <- TestClock.adjust(1.second)
        secondSupplierCall <- prefetcher.currentValueRef.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(secondSupplierCall)(equalTo(2))
    ),
    testM("Correctly deal with supplier errors")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(-42, new FailingIncr().failEvery2, 1.second).provideCustomLayer(logEnv)
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
    )
  )

  private var counter: Int = -1

  private val supplierFromCounter = ZIO.effect {
    counter += 1
    counter
  }

  private val incrementer = ZIO.fromFunction[Int, Int](i => i + 1)

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
