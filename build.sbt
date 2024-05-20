import Dependencies.*

name := "sharding-strategy"

organization := "com.evolutiongaming"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/sharding-strategy"))
startYear := Some(2018)

crossScalaVersions := Seq("2.13.14")
scalaVersion := crossScalaVersions.value.head
scalacOptions := Seq(
  "-release:17",
  "-Xsource:3",
)
releaseCrossBuild := true
publishTo := Some(Resolver.evolutionReleases) // sbt-release
versionPolicyIntention := Compatibility.BinaryCompatible // sbt-version-policy

// TODO remove after 3.0.2 is released
versionPolicyIgnored := Seq(
  "com.evolutiongaming" %% "nel",
  "com.evolutiongaming" %% "safe-actor",
)

libraryDependencies ++= Seq(
  `ddata-tools`,
  `cats-helper`,
  Akka.actor,
  Akka.`distributed-data`,
  Akka.cluster,
  Akka.sharding,
  Akka.testkit % Test,
  Cats.core,
  Cats.effect,
  scalatest % Test)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

//addCommandAlias("fmt", "scalafixAll; all scalafmtAll scalafmtSbt")
//addCommandAlias("check", "scalafixEnable; scalafixAll --check; all versionPolicyCheck scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("check", "versionPolicyCheck")
addCommandAlias("build", "all compile test")
