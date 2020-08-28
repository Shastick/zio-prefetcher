# `zio-prefetcher`

RAM is cheap. Caches can behave in unpredictable ways. Thus:

> Either things are in RAM or they don't exist.

## What's a Prefetcher?
 
A prefetcher is something that will do some work _in advance_ of when it is needed, while keeping the result around until it is eventually useful.

Hence, the time it takes to access pre-fetched things is entirely predictable. Think of a prefetcher as a `Supplier[T]` 
that returns a `T` immediately while regulary refreshing itself in the background  

Note that it is _slightly_ different from a cache. Please refer to [this blog post](https://j3t.ch/tech/prefetching-pattern/) for some further context.


## How To Use

```scala

type PrefetchedVal = Map[UserId, UserSettings]

val supplier(): ZIO[PrefetchedVal, Throwable, PrefetchedVal] = ...

for {
  prefetcher <- PrefetchingSupplier.withInitialValue(initialValue = Map(), supplier = supplier(), updateInterval = 1.second)
  ...
  instantAccess <- prefetcher.currentValueRef.get
  settings = instantAccess(someUserId)
  ...
} yield ...


```

This library is built against `zio` version `1.0.1` and `zio-logging` version `0.4.0` but expects you to provide `zio` and `zio-logging` at runtime. At the moment only Scala 2.13 is supported.

See [here](https://search.maven.org/search?q=g:ch.j3t%20AND%20a:zio-prefetcher_2.13) for the latest version.

## Example use cases

Assume a service with some users and their associated settings saved somewhere in a storage engine. 
Occasionally, these settings may change, but it's OK if updates don't propagate immediately.

In your code, you have some implementation of `UserId => Future[UserSettings]`. This is nice, but...

Your constraints may be such that looking up some settings in the backend takes too much time to your taste (_very_ picky users, crappy database, ...)

Assuming you have some spare RAM, you can set up a prefetcher such that you'll always have a `Map[UserId, UserSettings]`
sitting around, ready to be used. Now you can access all existing settings with a direct map lookup at any time.

# Consideration

## When to use a pre-fetcher

A pre-fetcher can be useful in situations where you have some data (configuration or otherwise) that is either:
 - entirely static
 - rarely updated 
 - robust to staleness (ie, for which not immediately having the latest version is OK).
  
Combined with access costs that range from _annoying_ to _prohibitive_, eg:
 - latencies to reach some other service
 - complex queries that take a (long) while but yield small resulty
 
And assuming that everything you generally need fits in RAM...

For cases that match all of the above, it can be interesting to rely on pre-fetched data instead of looking things up
through an external service, be it with or without some caching involved.

## What does this solve that a cache cannot?

Caches are great, no questions here.

The issue with a cache, in general, is that on a cache miss, the caller still needs to wait.  

Having a cache that pre-loads all possible queries or arguments to a function or service, while pro-actively refreshing 
the cached values if required, would solve for that problem.

However, if you do so, you essentially end up with a _prefetcher_, not using the other cache features like eviction policies: 
This pre-fetcher is intended for use in exactly such cases.  

## How it came to be

I've been using this _prefetching pattern_ in some projects, and am currently learning more about ZIO. 
Making a pure ZIO prefetcher seemed like a nice little challenge. 

## Disclaimer

This is some toy-code that _will_ see some production use but that is, nevertheless, written by a ZIO-Newbie: so use it at your own risks.