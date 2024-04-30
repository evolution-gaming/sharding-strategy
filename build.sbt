import Dependencies._

name := "sharding-strategy"

organization := "com.evolutiongaming"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/sharding-strategy"))
startYear := Some(2018)

crossScalaVersions := Seq("2.13.13")
scalaVersion := crossScalaVersions.value.head
scalacOptions := Seq(
  "-release:17",
  "-Xsource:3-cross",
)
releaseCrossBuild := true
publishTo := Some(Resolver.evolutionReleases)

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

addCommandAlias("build", "all compile test")
