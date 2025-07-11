import Dependencies.*

name := "sharding-strategy"

organization := "com.evolutiongaming"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/sharding-strategy"))
startYear := Some(2018)

crossScalaVersions := Seq("2.13.16", "3.3.6")
scalaVersion := crossScalaVersions.value.head
scalacOptions := Seq(
  "-release:17",
  "-deprecation",
)
scalacOptions ++= crossSettings(
  scalaVersion = scalaVersion.value,
  // Good compiler options for Scala 2.13 are coming from com.evolution:sbt-scalac-opts-plugin:0.0.9,
  // but its support for Scala 3 is limited, especially what concerns linting options.
  if2 = Seq(
    "-Xsource:3",
  ),
  // If Scala 3 is made the primary target, good linting scalac options for it should be added first.
  if3 = Seq(
    "-Ykind-projector:underscores",

    // disable new brace-less syntax:
    // https://alexn.org/blog/2022/10/24/scala-3-optional-braces/
    "-no-indent",

    // improve error messages:
    "-explain",
    "-explain-types",
  ),
)
publishTo := Some(Resolver.evolutionReleases) // sbt-release
versionPolicyIntention := Compatibility.BinaryCompatible // sbt-version-policy

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

//addCommandAlias("fmt", "all scalafmtAll scalafmtSbt")
//addCommandAlias("check", "all versionPolicyCheck scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("check", "+versionPolicyCheck")
addCommandAlias("build", "+all compile test")

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}
