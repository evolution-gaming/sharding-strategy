import Dependencies._

name := "sharding-strategy"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/sharding-strategy"))

startYear := Some(2018)

organizationName := "Evolution"

organizationHomepage := Some(url("http://evolution.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.5", "2.12.15")

scalacOptions -= "-Ywarn-unused:params"

publishTo := Some(Resolver.evolutionReleases)

libraryDependencies ++= Seq(
  `ddata-tools`,
  `safe-actor`,
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

releaseCrossBuild := true