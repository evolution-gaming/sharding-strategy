import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"   % "3.2.18"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "3.10.3"
  val `ddata-tools` = "com.evolutiongaming" %% "ddata-tools" % "4.0.0"

  object Cats {
    private val version = "2.10.0"
    private val effectVersion = "3.5.4"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % effectVersion
  }

  object Akka {
    private val version = "2.6.21"
    val actor              = "com.typesafe.akka" %% "akka-actor"            % version
    val cluster            = "com.typesafe.akka" %% "akka-cluster"          % version
    val sharding           = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val `distributed-data` = "com.typesafe.akka" %% "akka-distributed-data" % version
    val testkit            = "com.typesafe.akka" %% "akka-testkit"          % version
  }
}
