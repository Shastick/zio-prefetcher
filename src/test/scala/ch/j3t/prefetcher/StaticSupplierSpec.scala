package ch.j3t.prefetcher

import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }
import zio.{ UIO }

object StaticSupplierSpec extends DefaultRunnableSpec {

  def spec = suite("StaticSupplierSpec")(
    testM("Wrapping a simple value should work") {
      val sup = PrefetchingSupplier.static(42)
      for {
        v1 <- sup.get
        v2 <- sup.get
      } yield assert(v1)(equalTo(42)) && assert(v2)(equalTo(42))
    },
    testM("Wrapping a UIO should work") {
      val sup = PrefetchingSupplier.staticM(UIO(42))
      for {
        v1 <- sup.get
        v2 <- sup.get
      } yield assert(v1)(equalTo(42)) && assert(v2)(equalTo(42))
    }
  )

}
