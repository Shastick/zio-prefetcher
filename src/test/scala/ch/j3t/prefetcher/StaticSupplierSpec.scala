package ch.j3t.prefetcher

import zio.test.Assertion.equalTo
import zio.test.{ assert, TestClock, ZIOSpecDefault }
import zio.{ Chunk }
import zio._

object StaticSupplierSpec extends ZIOSpecDefault {

  def spec = suite("StaticSupplierSpec")(
    test("Wrapping a simple value should work") {
      val sup = PrefetchingSupplier.static(42)
      for {
        v1 <- sup.get
        v2 <- sup.get
      } yield assert(v1)(equalTo(42)) && assert(v2)(equalTo(42))
    },
    test("Wrapping a UIO should work") {
      val sup = PrefetchingSupplier.staticM(ZIO.succeed(42))
      for {
        v1 <- sup.get
        v2 <- sup.get
      } yield assert(v1)(equalTo(42)) && assert(v2)(equalTo(42))
    },
    test("static prefetcher updates stream should provide the value") {
      val sup    = PrefetchingSupplier.static(42)
      val stream = sup.updatesStream
      for {
        fiber     <- stream.take(1).runCollect.fork
        _         <- TestClock.adjust(1.second)
        collected <- fiber.join
      } yield assert(collected)(equalTo(Chunk(42)))
    },
    test("staticM prefetcher updates stream should provide the value") {
      val sup    = PrefetchingSupplier.staticM(ZIO.succeed(42))
      val stream = sup.updatesStream
      for {
        fiber     <- stream.take(1).runCollect.fork
        _         <- TestClock.adjust(1.second)
        collected <- fiber.join
      } yield assert(collected)(equalTo(Chunk(42)))
    }
  )

}
