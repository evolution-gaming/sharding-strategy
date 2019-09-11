import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"      % "3.0.8"
  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.2"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"    % "0.0.30"
  val `ddata-tools`    = "com.evolutiongaming" %% "ddata-tools"    % "2.0.6"
  val `safe-actor`     = "com.evolutiongaming" %% "safe-actor"     % "2.0.7"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }

  object Akka {
    private val version = "2.5.25"
    val actor              = "com.typesafe.akka" %% "akka-actor"            % version
    val cluster            = "com.typesafe.akka" %% "akka-cluster"          % version
    val sharding           = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val `distributed-data` = "com.typesafe.akka" %% "akka-distributed-data" % version
    val testkit            = "com.typesafe.akka" %% "akka-testkit"          % version
  }
}
