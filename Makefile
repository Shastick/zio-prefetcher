VERSION=HEAD-SNAPSHOT

.PHONY: build test release release-local

build:
	sbt +clean +compile

test:
	sbt +test

release: test
	sbt 'set version := "$(VERSION)"' +publishSigned

# Do a +publishLocal if you need it in your local .ivy2 repo
release-local:
	sbt 'set version := "$(VERSION)"' +publishM2
