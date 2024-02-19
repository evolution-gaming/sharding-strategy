import sbt._

object Dependencies {

  val scalatest        = "org.scalatest"       %% "scalatest"      % "3.2.9"
  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.2"
  val `cats-helper`    = "com.evolutiongaming" %% "cats-helper"    % "3.10.0"
  val `ddata-tools`    = "com.evolutiongaming" %% "ddata-tools"    % "4.0.0"
  val `safe-actor`     = "com.evolutiongaming" %% "safe-actor"     % "3.0.0"

  object Cats {
    private val version = "2.9.0"
    private val effectVersion = "3.4.8"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % effectVersion
  }

  object Akka {
    private val version = "2.6.20"
    val actor              = "com.typesafe.akka" %% "akka-actor"            % version
    val cluster            = "com.typesafe.akka" %% "akka-cluster"          % version
    val sharding           = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val `distributed-data` = "com.typesafe.akka" %% "akka-distributed-data" % version
    val testkit            = "com.typesafe.akka" %% "akka-testkit"          % version
  }
}
