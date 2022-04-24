# `zio-prefetcher`

RAM is cheap. Caches can behave in unpredictable ways. Thus:

> Either things are in RAM or they don't exist.

## What's a Prefetcher?
 
A prefetcher is something that will do some work _in advance_ of when it is needed, while keeping the result around until it is eventually useful.

Hence, the time it takes to access pre-fetched things is entirely predictable. Think of a prefetcher as a `Supplier[T]` 
that returns a `T` immediately while regularly refreshing itself in the background  

Note that it is _slightly_ different from a cache. Please refer to [this blog post](https://j3t.ch/tech/prefetching-pattern/) for some further context.


## How To Use

See [mvnrepository.com for the latest version](https://mvnrepository.com/artifact/ch.j3t/zio-prefetcher)

```sbt
// https://mvnrepository.com/artifact/ch.j3t/zio-prefetcher
libraryDependencies += "ch.j3t" %% "zio-prefetcher" % "0.7.0"
```

All dependencies need to be provided (`zio`, `zio-streams`, `zio-logging`). Additionally, if you intend to rely on the pre-fetchers that expose metrics, you'll also need to provide `zio-metrics-dropwizard`.

### Simple Case

The example below shows how to load the output of a supplier into memory and periodically refresh it: 

```scala

// Some Map you'd like to have around for quick lookups
type PrefetchedVal = Map[UserId, UserSettings]

// Something that computes the map too slowly for your taste
val supplier: ZIO[PrefetchedVal, Throwable, PrefetchedVal] = ???

for {
  prefetcher <- PrefetchingSupplier.withInitialValue(initialValue = Map(), supplier = supplier(), updateInterval = 1.second)
  ...
  instantAccess <- prefetcher.get
  settings = instantAccess(someUserId)
  ...
} yield ...


```

### From A Stream

This example is similar to the previous one. Here the value held by the pre-fetcher is the last 
one that was emitted by the passed stream.

```scala

// Some Map you'd like to have around for quick lookups
type PrefetchedVal = Map[UserId, UserSettings]

// A stream that occasionally emits the current version of the Map, which we want to keep around in memory:
val supplyingStream: UStream[PrefetchedVal] = ???

for {
  prefetcher <- StreamingPrefetchingSupplier.fromStream(initialValue, supplyingStream)
  ...
  instantAccess <- prefetcher.get
  settings = instantAccess(someUserId)
  ...
} yield ...


```

### Combining Pre-Fetchers

The true foot-gun potential of this library gets entirely obvious once you start combining pre-fetchers together,
thanks to `ZStream`'s `zipWithLatest` and the prefetcher's own `updatesStream` that lets you subscribe to prefetcher updates:

```scala

// Some Map you'd like to have around for quick lookups,
// but which requires looking up two different and slow data sources
type CombinedType = Map[UserId, UserSettings]

// Prefetchers for the two data-sources.
// These can have very different refresh intervals
val pfA: PrefetchingSupplier[SomeType] = ???
val pfB: PrefetchingSupplier[AnotherType] = ???

// Defines how to combine the values from each data source
def combinePrefetchedValues(some: SomeType, another: AnotherType): CombinedType = ???

// Build a stream of the combined type
val combinedUpdateStream: UStream[CombinedType] = 
  pfA.updatesStream.zipWithLatest(pfB.updatesStream)
    .map{case (some, another) => combinePrefetchedValues(some, another)}

for {
  // Pipe the stream into a pre-fetcher
  prefetcher <- StreamingPrefetchingSupplier.fromStream(initialValue, combinedUpdateStream)
  ...
  instantAccess <- prefetcher.get
  settings = instantAccess(someUserId)
  ...
} yield ...
```

### About Initial Values

The examples show how to provide an initial value, to be used by the pre-fetcher while the first _real_ value is being computed.

An alternative is available to synchronously wait for the first value to have been computed, eg: `PrefetchingSupplier.withInitialFetch`

## Sample Use Cases

Assume a service with some users and their associated settings saved somewhere in a storage engine. 
Occasionally, these settings may change, but it's OK if updates don't propagate immediately.

In your code, you have some implementation of `UserId => Future[UserSettings]`. This is nice, but...

Your constraints may be such that looking up some settings in the backend takes too much time to your taste (_very_ picky users, crappy database, ...)

Assuming you have some spare RAM, you can set up a prefetcher such that you'll always have a `Map[UserId, UserSettings]`
sitting around, ready to be used. Now you can access all existing settings with a direct map lookup at any time.

# Considerations

## When to use a pre-fetcher

A pre-fetcher can be useful in situations where you have some data (configuration or otherwise) that is either:
 - entirely static
 - rarely updated 
 - robust to staleness (ie, for which not immediately having the latest version is OK).
  
Combined with access costs that range from _annoying_ to _prohibitive_, eg:
 - latencies to reach some other service
 - complex queries that take a (long) while but yield small results
 
And assuming that everything you generally need fits in RAM...

For cases that match all the above, it can be interesting to rely on pre-fetched data instead of looking things up
through an external service, be it with or without some caching involved.

## What does this solve that a cache does not?

Caches are great, no questions here.

The issue with a cache, in general, is that on a cache miss, the caller still needs to wait.  

Having a cache that pre-loads all possible queries or arguments to a function or service, while pro-actively refreshing 
the cached values if required, could solve that problem.

In such situations, a pre-fetcher might still be easier to reason about: the values it contains can be guaranteed to be internally consistent.
This might be harder to achieve with a traditional cache.

## How it came to be

I've been using this _prefetching pattern_ in some projects, and am currently learning more about ZIO. 
Making a pure ZIO prefetcher seemed like a nice little challenge. 

## Disclaimer

This is ~~some toy-code that will see some production use~~ actually being used in production but is, 
nevertheless, written by a relative ZIO-Newbie: please use it at your own risks.

## Contributors

Many thanks to [Natalia Juszka](https://github.com/meluru) for adding the reactive/streaming pre-fetcher.