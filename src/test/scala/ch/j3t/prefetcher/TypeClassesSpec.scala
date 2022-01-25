package ch.j3t.prefetcher

import zio.test.Assertion._
import zio.test._
import zio.test.ZIOSpecDefault

object TypeClassesSpec extends ZIOSpecDefault {

  def spec = suite("TypeClassesSpec")(
    test("MapLookupHelper should work as expected") {
      val sup = PrefetchingSupplier.static(Map("A" -> 1L, "B" -> 2L, "C" -> 3L))
      for {
        m        <- sup.get
        v1       <- m.getAll(Set("A", "C"))
        v2       <- m.getOrFail("A")
        fld      <- m.getOrFail("X").exit
        fldMulti <- m.getAllOrFail(Set("A", "Y")).exit
      } yield assert(v1)(equalTo(Map("A" -> 1L, "C" -> 3L))) &&
        assert(v2)(equalTo(1L)) &&
        assert(fld)(fails(equalTo(NotFound("Not found: X", None)))) &&
        assert(fldMulti)(
          fails(equalTo(NotFound("Failed to lookup set of keys: Set(A, Y)", Some(NotFound("Not found: Y")))))
        )
    },
    test("PrefetcherLookupHelper should work as expected") {
      val sup =
        PrefetchingSupplier.static(
          Map("A" -> Map(1 -> "one", 2 -> "two", 5 -> "five"), "B" -> Map(3 -> "three", 4 -> "four"))
        )
      for {
        v1              <- sup.nestedLookup(1)("A")
        v2              <- sup.nestedLookups(Set(1, 5))("A")
        failNested      <- sup.nestedLookup(6)("A").exit
        failOuter       <- sup.nestedLookup(1)("C").exit
        multiFailNested <- sup.nestedLookups(Set(1, 3, 5))("A").exit
        multiFailOuter  <- sup.nestedLookups(Set(1, 5))("C").exit
      } yield assert(v1)(equalTo("one")) &&
        assert(v2)(equalTo(Map(1 -> "one", 5 -> "five"))) &&
        assert(failNested)(fails(equalTo(NotFound("Not found: 6")))) &&
        assert(failOuter)(fails(equalTo(NotFound("Not found: C")))) &&
        assert(multiFailNested)(
          fails(equalTo(NotFound("Failed to lookup set of keys: Set(1, 3, 5)", Some(NotFound("Not found: 3")))))
        ) &&
        assert(multiFailOuter)(fails(equalTo(NotFound("Not found: C"))))
    }
  )

}
