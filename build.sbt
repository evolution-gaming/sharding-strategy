import Dependencies._

name := "sharding-strategy"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/sharding-strategy"))

startYear := Some(2018)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

bintrayOrganization := Some("evolutiongaming")

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.1", "2.12.10")

scalacOptions -= "-Ywarn-unused:params"

resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

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