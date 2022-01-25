package ch.j3t.prefetcher

import com.codahale.metrics.MetricRegistry
import zio.Clock
import zio.metrics.dropwizard._
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{ assert, liveEnvironment, TestClock, ZIOSpecDefault }
import zio.{ Chunk, Tag, ZEnv, ZIO, ZLayer }
import zio._
import zio.ZIO.attemptBlocking

object StreamingPrefetchingSupplierSpec extends ZIOSpecDefault {

  def withNewRegistryLayer =
    ZIO.succeed(new MetricRegistry()).map { mr =>
      (mr, (ZLayer.succeed[Option[MetricRegistry]](Some(mr)) >>> Registry.explicit))
    }

  def spec = suite("StreamingPrefetchingSupplierSpec")(
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
    test("Correctly update the derived value")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        derived    <- withDerived[Int, Int](prefetcher, i => i + 100, 666)
        // Adjust the clock slightly so streams will be consumed
        _                   <- TestClock.adjust(1.millis)
        immediatelyHeld     <- prefetcher.get
        immediatelyDerived  <- derived.get
        _                   <- TestClock.adjust(99.millis)
        initialSupplierCall <- prefetcher.get
        derivedInitialCall  <- derived.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
        derivedSecondCall   <- derived.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(immediatelyDerived)(equalTo(100)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(derivedInitialCall)(equalTo(101)) &&
        assert(secondSupplierCall)(equalTo(2)) &&
        assert(derivedSecondCall)(equalTo(102))
    ),
    test("Correctly update the derived value even if the parent fiber terminates")(
      for {
        prefetcherF <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis).fork
        prefetcher  <- prefetcherF.join
        derived     <- withDerived[Int, Int](prefetcher, i => i + 100, 666)
        // Adjust the clock slightly so streams will be consumed
        _                   <- TestClock.adjust(1.millis)
        immediatelyHeld     <- prefetcher.get
        immediatelyDerived  <- derived.get
        _                   <- TestClock.adjust(99.millis)
        initialSupplierCall <- prefetcher.get
        derivedInitialCall  <- derived.get
        _                   <- TestClock.adjust(1.second)
        secondSupplierCall  <- prefetcher.get
        derivedSecondCall   <- derived.get
      } yield assert(immediatelyHeld)(equalTo(0)) &&
        assert(immediatelyDerived)(equalTo(100)) &&
        assert(initialSupplierCall)(equalTo(1)) &&
        assert(derivedInitialCall)(equalTo(101)) &&
        assert(secondSupplierCall)(equalTo(2)) &&
        assert(derivedSecondCall)(equalTo(102))
    ),
    test("Correctly propagate the derived value to the updatesStream")(
      for {
        prefetcher           <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        derived              <- withDerived[Int, Int](prefetcher, i => i + 100, 666)
        _                    <- TestClock.adjust(1.millis)
        derivedStream         = derived.updatesStream
        fiber                <- derivedStream.take(4).runCollect.fork
        _                    <- TestClock.adjust(3.second)
        collectedFromDerived <- fiber.join
      } yield assert(collectedFromDerived)(equalTo(Chunk(100, 101, 102, 103)))
    ),
    test("Correctly get the later derived if we subscribe later")(
      for {
        prefetcher <- PrefetchingSupplier.withInitialValue(0, incrementer, 1.second, 100.millis)
        derived    <- withDerived[Int, Int](prefetcher, i => i + 100, 666)
        _          <- TestClock.adjust(100.millis)
        stream      = derived.updatesStream
        fiber      <- stream.take(1).runCollect.fork
        _          <- TestClock.adjust(1.second)
        collected  <- fiber.join
      } yield assert(collected)(equalTo(Chunk(101)))
    ),
    test("Return the passed initial value")(
      for {
        prefetcher <- StreamingPrefetchingSupplier
                        .fromStream(42, ZStream.empty)

        initialGet       <- prefetcher.get
        _                <- TestClock.adjust(100.millis)
        getAfterSomeTime <- prefetcher.get

      } yield assert(initialGet)(equalTo(42)) &&
        assert(getAfterSomeTime)(equalTo(42))
    )
  ).provide(liveEnvironment)

  private def withDerived[T, M: Tag](
    ps: PrefetchingSupplier[T],
    transformation: T => M,
    canary: M // A value we should not see in the derived pre-fetcher.
  ): ZIO[
    Clock with ZEnv,
    Nothing,
    StreamingPrefetchingSupplier.StreamingPrefetchingSupplier[M]
  ] =
    StreamingPrefetchingSupplier
      .fromStream(canary, ps.updatesStream.map(transformation))

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
