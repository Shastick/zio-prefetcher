// *****************************************************************************
// Projects
// *****************************************************************************

lazy val root =
  project
    .in(file("."))
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.zio % Provided,
        library.zioStreams % Provided,
        library.zioLogging % Provided,
        library.zioMetrics % Provided,
        // library.metricsCore % Provided,
        library.zioTest    % Test,
        library.zioTestSbt % Test
      ),
      publishArtifact := true,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {
      val zio = "1.0.7"
    }

    val zio        = "dev.zio" %% "zio"          % Version.zio
    val zioStreams = "dev.zio" %% "zio-streams"  % Version.zio
    val zioLogging = "dev.zio" %% "zio-logging"  % "0.5.8"
    val zioMetrics = "dev.zio" %% "zio-metrics-dropwizard" % "1.0.8"
    val metricsCore = "io.dropwizard.metrics" % "metrics-core" % "4.1.21"
    val zioTest    = "dev.zio" %% "zio-test"     % Version.zio
    val zioTestSbt = "dev.zio" %% "zio-test-sbt" % Version.zio
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
    scalafmtSettings ++
    commandAliases

lazy val commonSettings =
  Seq(
    name := "zio-prefetcher",
    scalaVersion := "2.13.5",
    crossScalaVersions := Seq("2.12.14", "2.13.5"),
    organization := "ch.j3t",
    organizationName := "j3t",
    homepage := Some(url("https://github.com/Shastick/zio-prefetcher/")),
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/Shastick/zio-prefetcher.git"),
        "scm:git:git@github.com:Shastick/zio-prefetcher.git"
      )
    ),
    developers := List(
      Developer("shastick", "Shastick", "", url("https://github.com/Shastick"))
    )
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )

lazy val commandAliases =
  addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt") ++
    addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

// Fix that annoying "scalac: 'nullary-override' is not a valid choice for '-Xlint'" error
scalacOptions ~= { opts => opts.filterNot(Set("-Xlint:nullary-override")) }