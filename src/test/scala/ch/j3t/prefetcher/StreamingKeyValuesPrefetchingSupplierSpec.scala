package ch.j3t.prefetcher

import ch.j3t.prefetcher.StreamingKeyValuesPrefetchingSupplier.{ Drop, Put, Update }
import zio.duration.durationInt
import zio.stream.UStream
import zio.test.Assertion.equalTo
import zio.test.environment.TestClock
import zio.test.{ assert, DefaultRunnableSpec }
import zio.{ Chunk, Queue }
import java.time.Instant

object StreamingKeyValuesPrefetchingSupplierSpec extends DefaultRunnableSpec {

  def spec = suite("StreamingKeyValuesPrefetchingSupplierSpec")(
    testM("The initial value is served as expected") {
      val initial = Map("initial" -> "value")
      for {
        q        <- Queue.unbounded[Update[String, String]]
        pf       <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(initial, UStream.fromQueue(q))
        initialQ <- pf.get
      } yield assert(initialQ)(equalTo(initial))
    },
    testM("Adding new entries and overriding them in separate batches works as expected") {
      for {
        q   <- Queue.unbounded[Update[String, String]]
        pf  <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        _   <- q.offer(Put("new", "value"))
        _   <- TestClock.adjust(2.seconds)
        qr1 <- pf.get
        _   <- q.offer(Put("another", "kind"))
        _   <- TestClock.adjust(2.seconds)
        qr2 <- pf.get
        _   <- q.offer(Put("new", "stuff"))
        _   <- TestClock.adjust(2.seconds)
        qr3 <- pf.get
      } yield assert(qr1)(equalTo(Map("new" -> "value"))) &&
        assert(qr2)(equalTo(Map("new" -> "value", "another" -> "kind"))) &&
        assert(qr3)(equalTo(Map("new" -> "stuff", "another" -> "kind")))
    },
    // Let's hope the ZIO peeps have their stuff together: the test below isn't entirely rigorous when
    // it comes to guarantee that the ordering is always as we think it is ;)
    testM("Adding new entries and deleting them in separate batches works as expected") {
      for {
        q   <- Queue.unbounded[Update[String, String]]
        pf  <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        _   <- q.offer(Put("new", "value"))
        _   <- TestClock.adjust(2.seconds)
        qr1 <- pf.get
        _   <- q.offer(Put("another", "kind"))
        _   <- TestClock.adjust(2.seconds)
        qr2 <- pf.get
        _   <- q.offer(Drop("new"))
        _   <- TestClock.adjust(2.seconds)
        qr3 <- pf.get
      } yield assert(qr1)(equalTo(Map("new" -> "value"))) &&
        assert(qr2)(equalTo(Map("new" -> "value", "another" -> "kind"))) &&
        assert(qr3)(equalTo(Map("another" -> "kind")))
    },
    testM("Dropping a non-existing value works fine") {
      for {
        q   <- Queue.unbounded[Update[String, String]]
        pf  <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        _   <- q.offer(Put("new", "value"))
        _   <- TestClock.adjust(2.seconds)
        qr1 <- pf.get
        _   <- q.offer(Drop("another"))
        _   <- TestClock.adjust(2.seconds)
        qr2 <- pf.get
      } yield assert(qr1)(equalTo(Map("new" -> "value"))) &&
        assert(qr2)(equalTo(Map("new" -> "value")))
    },
    testM("Adding and updating values in the same batch works fine") {
      for {
        q   <- Queue.unbounded[Update[String, String]]
        pf  <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 2, 1.second)
        _   <- q.offer(Put("new", "value"))
        qr1 <- pf.get                      // Clock has not advanced and group size not reached: the prefetcher should not be updated
        _   <- q.offer(Put("new", "stuff"))
        _   <- TestClock.adjust(2.seconds) // After this a batch update should have occurred
        qr2 <- pf.get
      } yield assert(qr1)(equalTo(Map[String, String]())) &&
        assert(qr2)(equalTo(Map("new" -> "stuff")))
    },
    testM("Adding, deleting and re-adding values in the same batch works fine") {
      for {
        q   <- Queue.unbounded[Update[String, String]]
        pf  <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 3, 1.second)
        _   <- q.offer(Put("new", "value"))
        qr1 <- pf.get                      // Clock has not advanced and group size not reached: the prefetcher should not be updated
        _   <- q.offer(Drop("new"))
        qr2 <- pf.get                      // Clock has not advanced and group size not reached: the prefetcher should still not be updated
        _   <- q.offer(Put("new", "stuff"))
        _   <- TestClock.adjust(2.seconds) // After this a batch update should have occurred
        qr3 <- pf.get
      } yield assert(qr1)(equalTo(Map[String, String]())) &&
        assert(qr2)(equalTo(Map[String, String]())) &&
        assert(qr3)(equalTo(Map("new" -> "stuff")))
    },
    testM("A relatively long batch of updates does not cause a stack overflow and is properly grouped") {
      for {
        q    <- Queue.unbounded[Update[String, String]]
        pf   <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1024, 1.second)
        _    <- UStream.range(0, 1023).mapM(i => q.offer(Put("new", "value: " + i))).runDrain
        qr1  <- pf.get                             // Clock has not advanced and group size not reached: the prefetcher should not be updated
        _    <- q.offer(Put("new", "value: 1023")) // This should trigger the group size to be reached
        _    <- TestClock.adjust(300.millis)       // "Wait" a little bit but don't trigger the grouping timer limit
        qr2  <- pf.get                             // Clock has not advanced but group size was reached
        tEnd <- pf.lastSuccessfulUpdate
      } yield assert(qr1)(equalTo(Map[String, String]())) &&
        assert(qr2)(equalTo(Map("new" -> "value: 1023"))) &&
        assert(tEnd)(equalTo(Instant.ofEpochMilli(0)))
    },
    testM("Updates stream returns the init value") {
      for {
        q         <- Queue.unbounded[Update[String, String]]
        pf        <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        stream     = pf.updatesStream
        sFiber    <- stream.take(1).runCollect.fork
        _         <- TestClock.adjust(1.second)
        collected <- sFiber.join
      } yield assert(collected)(equalTo(Chunk(Map.empty[String, String])))
    },
    testM("Updates stream returns later value if we susbcribe later") {
      for {
        q         <- Queue.unbounded[Update[String, String]]
        pf        <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        stream     = pf.updatesStream
        _         <- TestClock.adjust(1.second)
        _         <- q.offer(Put("key", "value"))
        sFiber    <- stream.take(1).runCollect.fork
        collected <- sFiber.join
      } yield assert(collected)(equalTo(Chunk(Map("key" -> "value"))))
    },
    testM("Updates stream reflects the updates") {
      for {
        q      <- Queue.unbounded[Update[String, String]]
        pf     <- StreamingKeyValuesPrefetchingSupplier.withInitialValue(Map(), UStream.fromQueue(q), 1, 1.second)
        stream  = pf.updatesStream
        sFiber <- stream.take(5).runCollect.fork
        _      <- TestClock.adjust(1.second)
        _      <- q.offer(Put("new", "value"))
        _      <- q.offer(Put("one", "value"))
        _      <- TestClock.adjust(1.second)
        _      <- q.offer(Put("one", "value"))
        _      <- TestClock.adjust(1.second)
        _      <- q.offer(Drop("one"))

        collected <- sFiber.join
      } yield assert(collected)(
        equalTo(
          Chunk(
            Map.empty[String, String],
            Map("new" -> "value"),
            Map("new" -> "value", "one" -> "value"),
            // Currently we emit the update again even if it's the same as previous
            Map("new" -> "value", "one" -> "value"),
            Map("new" -> "value")
          )
        )
      )
    }
  )

}
